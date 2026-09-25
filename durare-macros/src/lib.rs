//! Procedural macros for `durare`.
//!
//! - [`macro@workflow`] leaves your async fn untouched and, alongside it, emits
//!   a compile-time registration (so the engine auto-discovers the workflow —
//!   no manual `engine.register(...)`) plus a typed `UpperCamelCase` marker
//!   implementing `durare::WorkflowDef`, so the workflow can be started by a
//!   type-checked reference rather than a string.
//! - [`macro@step`] wraps an async fn's body in a durable
//!   `ctx.step(...)` checkpoint, so a step reads like an ordinary `async fn`
//!   call — no closure, no `Box::pin`, no `Ok::<_, Error>` annotation. The fn it
//!   emits returns a `durare::PendingStep`, so the call claims its position
//!   where it is written, like every durable call written by hand.
//! - [`macro@transaction`] does the same for `ctx.transaction(...)`: the body's
//!   SQL writes and the checkpoint commit together, without the
//!   `|tx| Box::pin(async move { ... })` wrapper.

use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::visit_mut::{self, VisitMut};
use syn::{
    parse_macro_input, parse_quote, FnArg, GenericParam, Ident, ItemFn, Lifetime, LifetimeParam,
    LitStr, ReturnType, Signature, Token, Type, TypeParamBound, TypeReference,
};

/// Parsed `#[workflow(...)]` arguments. Supports a bare name literal
/// (`#[workflow("orders.process")]`) and/or keyed args
/// (`#[workflow(name = "...", schedule = "* * * * * *")]`).
struct WorkflowArgs {
    name: Option<String>,
    schedule: Option<String>,
}

impl Parse for WorkflowArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut schedule = None;
        while !input.is_empty() {
            if input.peek(LitStr) {
                // Bare string literal: the registered name.
                name = Some(input.parse::<LitStr>()?.value());
            } else {
                let key: Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                let val: LitStr = input.parse()?;
                match key.to_string().as_str() {
                    "name" => name = Some(val.value()),
                    "schedule" => schedule = Some(val.value()),
                    other => {
                        return Err(syn::Error::new(
                            key.span(),
                            format!("unknown `#[workflow]` argument `{other}` (expected `name` or `schedule`)"),
                        ))
                    }
                }
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }
        Ok(WorkflowArgs { name, schedule })
    }
}

/// Register an `async fn(DurableContext, Input) -> Result<Output>` as a durable
/// workflow.
///
/// ```ignore
/// #[durare::workflow]
/// async fn process_order(ctx: &DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Override the registered name:
/// #[durare::workflow("orders.process")]
/// async fn process_order(ctx: &DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Run on a cron schedule (6-field cron, second precision). The workflow
/// // receives the scheduled tick time (RFC 3339) as its input:
/// #[durare::workflow(schedule = "0 0 * * * *")] // top of every hour
/// async fn hourly(ctx: &DurableContext, scheduled_at: String) -> Result<()> { ... }
/// ```
///
/// The function is left as-is. The macro additionally emits:
/// - an `inventory` registration — `DurableEngine::new`/`builder` collect every
///   one in the binary, so annotated workflows need no manual `register` call;
///   scheduled ones start firing once [`DurableEngine::launch`] is called;
/// - a typed marker — an `UpperCamelCase` zero-sized struct named after the
///   function (`process_order` → `ProcessOrder`) implementing
///   `durare::WorkflowDef`, so `engine.start_with(ProcessOrder, order, opts)`
///   is checked on input and output without a turbofish.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as WorkflowArgs);

    // Name defaults to the function's identifier.
    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());
    let schedule = match args.schedule {
        Some(s) => quote! { Some(#s) },
        None => quote! { None },
    };

    let ident = &func.sig.ident;
    let vis = &func.vis;

    // Input type: the second parameter, after `DurableContext`.
    let input_ty = match func.sig.inputs.iter().nth(1) {
        Some(FnArg::Typed(pt)) => (*pt.ty).clone(),
        _ => {
            return syn::Error::new_spanned(
                &func.sig,
                "a `#[workflow]` fn must take `(DurableContext, Input)`",
            )
            .to_compile_error()
            .into()
        }
    };
    let return_ty = match result_ty(&func.sig, "workflow") {
        Ok(ty) => ty,
        Err(e) => return e.to_compile_error().into(),
    };
    // Marker type name: `UpperCamelCase` of the function identifier.
    let marker = Ident::new(&ident.to_string().to_upper_camel_case(), ident.span());

    let expanded = quote! {
        #func

        /// Typed reference to this workflow, emitted by `#[durare::workflow]`.
        /// Pass it to `DurableEngine::start_with`.
        #[derive(Clone, Copy, Debug)]
        #vis struct #marker;

        impl durare::WorkflowDef for #marker {
            type Input = #input_ty;
            type Output = <#return_ty as durare::WorkflowResult>::Ok;
            const NAME: &'static str = #name;
        }

        durare::inventory::submit! {
            durare::WorkflowRegistration {
                name: #name,
                // A non-capturing closure coerces to `fn() -> WorkflowFn`.
                // `erase` infers the Input/Output types from the fn signature.
                builder: || durare::erase(#ident),
                schedule: #schedule,
            }
        }
    };

    expanded.into()
}

/// Rewrite a durable fn's signature so the call claims its position where it is
/// written: a plain `fn` returning a `durare::PendingStep`, not an `async fn`
/// whose body — and so whose position — waits for the first poll.
///
/// The `Ok` type is projected at the type level rather than parsed out of the
/// return type's tokens, so any `Result` alias works, as in [`macro@workflow`].
fn pending_signature(mut sig: Signature, macro_name: &str) -> syn::Result<Signature> {
    let ret = result_ty(&sig, macro_name)?.clone();
    let borrow = Lifetime::new("'__durare", Span::call_site());

    // The returned `PendingStep` borrows for as long as the call lives, so the
    // signature needs a lifetime to name. Relying on elision would only work
    // for a fn whose context is its one reference, and would reject an ordinary
    // generic step outright, so the macro introduces its own: every elided
    // borrow in the arguments becomes `'__durare`, every lifetime already named
    // must outlive it, and every type parameter must be valid for it, since the
    // future the call returns holds all of them.
    let mut elided = ElidedBorrows(borrow.clone());
    for arg in sig.inputs.iter_mut() {
        elided.visit_fn_arg_mut(arg);
    }
    // Each bound goes where that parameter's bounds already are: adding a
    // `where` predicate for a parameter written as `<T: Serialize>` would leave
    // it bounded in two places, which `clippy::multiple_bound_locations` reports
    // in the caller's code, about a bound the caller never wrote.
    let mut unbounded_lifetimes = Vec::new();
    let mut unbounded_types = Vec::new();
    for param in sig.generics.params.iter_mut() {
        match param {
            GenericParam::Lifetime(def) if def.bounds.is_empty() => {
                unbounded_lifetimes.push(def.lifetime.clone())
            }
            GenericParam::Lifetime(def) => def.bounds.push(borrow.clone()),
            GenericParam::Type(ty) if ty.bounds.is_empty() => {
                unbounded_types.push(ty.ident.clone())
            }
            GenericParam::Type(ty) => ty.bounds.push(TypeParamBound::Lifetime(borrow.clone())),
            GenericParam::Const(_) => {}
        }
    }
    if !unbounded_lifetimes.is_empty() || !unbounded_types.is_empty() {
        let where_clause = sig.generics.make_where_clause();
        for lifetime in unbounded_lifetimes {
            where_clause
                .predicates
                .push(parse_quote!(#lifetime: #borrow));
        }
        for ty in unbounded_types {
            where_clause.predicates.push(parse_quote!(#ty: #borrow));
        }
    }
    sig.generics.params.insert(
        0,
        GenericParam::Lifetime(LifetimeParam::new(borrow.clone())),
    );

    sig.asyncness = None;
    sig.output = parse_quote! {
        -> durare::PendingStep<#borrow, <#ret as durare::WorkflowResult>::Ok>
    };
    Ok(sig)
}

/// Rewrites the elided borrows in a durable fn's arguments — `&T` and `&'_ T` —
/// to the lifetime the macro owns. An already-named lifetime is left alone and
/// picks up an outlives bound instead.
struct ElidedBorrows(Lifetime);

impl VisitMut for ElidedBorrows {
    fn visit_type_reference_mut(&mut self, node: &mut TypeReference) {
        if node.lifetime.as_ref().is_none_or(|lt| lt.ident == "_") {
            node.lifetime = Some(self.0.clone());
        }
        visit_mut::visit_type_reference_mut(self, node);
    }

    fn visit_lifetime_mut(&mut self, node: &mut Lifetime) {
        if node.ident == "_" {
            *node = self.0.clone();
        }
    }
}

/// A durable fn's declared return type, which every one of these macros
/// requires to be a `Result`. None of them parses the `Ok` type out of it —
/// they project `<ReturnType as WorkflowResult>::Ok` and let the compiler
/// extract it, so any `Result` alias works.
fn result_ty<'s>(sig: &'s Signature, macro_name: &str) -> syn::Result<&'s Type> {
    match &sig.output {
        ReturnType::Type(_, ty) => Ok(ty),
        ReturnType::Default => Err(syn::Error::new_spanned(
            sig,
            format!("a `#[{macro_name}]` fn must return `Result<..>`"),
        )),
    }
}

/// Parsed `#[step(...)]` arguments: an optional name override — a bare literal
/// (`#[step("charge")]`) or `name = "..."` — defaulting to the function name.
struct StepArgs {
    name: Option<String>,
}

impl Parse for StepArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(StepArgs { name: None });
        }
        if input.peek(LitStr) {
            return Ok(StepArgs {
                name: Some(input.parse::<LitStr>()?.value()),
            });
        }
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let val: LitStr = input.parse()?;
        if key != "name" {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown `#[step]` argument `{key}` (expected `name`)"),
            ));
        }
        Ok(StepArgs {
            name: Some(val.value()),
        })
    }
}

/// Turn an `async fn(&DurableContext, args..) -> Result<T>` into a durable
/// [`step`](durare::DurableContext::step): the body is checkpointed on first run
/// and served from the checkpoint on replay — exactly like calling
/// `ctx.step("name", |_| async move { ... })` by hand, but without the closure,
/// the `Box::pin`, or the `Ok::<_, Error>` annotation.
///
/// ```ignore
/// #[durare::step]
/// async fn charge(ctx: &DurableContext, cents: i64) -> Result<Receipt> {
///     // ordinary async work — runs at most once per logical step
///     Ok(gateway::charge(cents).await?)
/// }
///
/// // inside a workflow, call it like any async fn:
/// let receipt = charge(&ctx, 1299).await?;
/// ```
///
/// The step name defaults to the function name; override with `#[step("name")]`
/// or `#[step(name = "...")]`. The first parameter is the context the step
/// checkpoints into (usually `ctx: &DurableContext`); the rest are the step's
/// arguments.
///
/// What it emits is a plain `fn` returning a
/// `durare::PendingStep` rather than an `async fn`, so the call
/// claims its step position where it is written and not where it is first
/// polled — the rule every durable call on a `DurableContext` follows. Calls
/// still read as `charge(&ctx, 1299).await?`; what changes is that building one
/// and never awaiting it spends the position anyway, which `#[must_use]` warns
/// about. The fn may take further references and be generic: the macro gives
/// the signature a lifetime of its own and bounds the parameters by it, rather
/// than leaning on elision.
#[proc_macro_attribute]
pub fn step(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as StepArgs);

    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());

    // The first parameter is the context the step checkpoints into.
    let ctx_ident = match func.sig.inputs.first() {
        Some(FnArg::Typed(pt)) => match &*pt.pat {
            syn::Pat::Ident(pi) => &pi.ident,
            _ => {
                return syn::Error::new_spanned(
                    &pt.pat,
                    "the first parameter of a `#[step]` fn must be a plain `ctx` binding",
                )
                .to_compile_error()
                .into()
            }
        },
        _ => {
            return syn::Error::new_spanned(
                &func.sig,
                "a `#[step]` fn must take `(&DurableContext, ..)`",
            )
            .to_compile_error()
            .into()
        }
    };

    let attrs = &func.attrs;
    let vis = &func.vis;
    let block = &func.block;
    let sig = match pending_signature(func.sig.clone(), "step") {
        Ok(sig) => sig,
        Err(e) => return e.to_compile_error().into(),
    };

    let expanded = quote! {
        #(#attrs)*
        #vis #sig {
            #ctx_ident.step(#name, move |_step| async move #block)
        }
    };

    expanded.into()
}

/// Parsed `#[transaction(...)]` arguments: an optional name override, like
/// [`StepArgs`].
struct TransactionArgs {
    name: Option<String>,
}

impl Parse for TransactionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(TransactionArgs { name: None });
        }
        if input.peek(LitStr) {
            return Ok(TransactionArgs {
                name: Some(input.parse::<LitStr>()?.value()),
            });
        }
        let key: Ident = input.parse()?;
        input.parse::<Token![=]>()?;
        let val: LitStr = input.parse()?;
        if key != "name" {
            return Err(syn::Error::new(
                key.span(),
                format!("unknown `#[transaction]` argument `{key}` (expected `name`)"),
            ));
        }
        Ok(TransactionArgs {
            name: Some(val.value()),
        })
    }
}

/// The simple identifier a parameter binds, if it is a plain `name: Ty` pattern.
fn param_ident(arg: &FnArg) -> Option<&Ident> {
    match arg {
        FnArg::Typed(pt) => match &*pt.pat {
            syn::Pat::Ident(pi) => Some(&pi.ident),
            _ => None,
        },
        _ => None,
    }
}

/// Turn an `async fn(&DurableContext, &mut Tx, args..) -> Result<T>` into a
/// durable [`transaction`](durare::DurableContext::transaction): the body's SQL
/// writes and the step checkpoint commit in one database transaction, without
/// the `|tx| Box::pin(async move { ... })` wrapper.
///
/// ```ignore
/// #[durare::transaction]
/// async fn debit(ctx: &DurableContext, tx: &mut Tx<'_>, cents: i64, id: i64) -> Result<i64> {
///     tx.execute("UPDATE acct SET bal = bal - ? WHERE id = ?", &params![cents, id]).await?;
///     let row = tx.query_one("SELECT bal FROM acct WHERE id = ?", &params![id]).await?;
///     Ok(row.get::<i64>("bal"))
/// }
///
/// // the injected `tx` is not a caller argument:
/// let bal = debit(&ctx, 10, 1).await?;
/// ```
///
/// The first parameter is the context, the **second** is the transaction handle
/// (dropped from the public signature — it is supplied by the runtime), and the
/// rest are the caller's arguments. Because a transaction may retry on a
/// serialization conflict, its body runs more than once, so each argument is
/// re-`clone`d per attempt — arguments must be [`Clone`]. The step name defaults
/// to the fn name; override with `#[transaction("name")]`.
///
/// Like [`macro@step`], what it emits is a plain `fn` returning a
/// `durare::PendingStep`, so the transaction claims its position
/// where it is written.
#[proc_macro_attribute]
pub fn transaction(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    let args = parse_macro_input!(attr as TransactionArgs);

    let name = args.name.unwrap_or_else(|| func.sig.ident.to_string());

    let inputs: Vec<&FnArg> = func.sig.inputs.iter().collect();
    if inputs.len() < 2 {
        return syn::Error::new_spanned(
            &func.sig,
            "a `#[transaction]` fn must take `(&DurableContext, &mut Tx, ..)`",
        )
        .to_compile_error()
        .into();
    }
    let Some(ctx_ident) = param_ident(inputs[0]) else {
        return syn::Error::new_spanned(
            inputs[0],
            "the first parameter of a `#[transaction]` fn must be a plain `ctx` binding",
        )
        .to_compile_error()
        .into();
    };
    // `tx`: its ident names the generated closure parameter, and its written
    // type is referenced below so a `use ..::Tx` import stays live even though
    // the parameter is dropped from the public signature.
    let (FnArg::Typed(tx_pt), Some(tx_ident)) = (inputs[1], param_ident(inputs[1])) else {
        return syn::Error::new_spanned(
            inputs[1],
            "the second parameter of a `#[transaction]` fn must be a plain `tx: &mut Tx` binding",
        )
        .to_compile_error()
        .into();
    };
    let tx_ty = &*tx_pt.ty;
    // The caller's arguments are everything after `ctx` and `tx`. Each must be a
    // plain `name: Ty` binding so it can be re-cloned per attempt; a destructured
    // arg couldn't be, and would silently make the body `FnOnce` — a confusing
    // "expected `Fn`" error far from the cause — so reject it cleanly here.
    let mut arg_idents: Vec<&Ident> = Vec::new();
    for &arg in &inputs[2..] {
        let Some(id) = param_ident(arg) else {
            return syn::Error::new_spanned(
                arg,
                "arguments of a `#[transaction]` fn must be plain `name: Ty` bindings",
            )
            .to_compile_error()
            .into();
        };
        arg_idents.push(id);
    }

    // The public signature drops `tx` (index 1) — the runtime supplies it.
    let mut sig = func.sig.clone();
    sig.inputs = func
        .sig
        .inputs
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != 1)
        .map(|(_, a)| a.clone())
        .collect();

    let attrs = &func.attrs;
    let vis = &func.vis;
    let block = &func.block;
    let sig = match pending_signature(sig, "transaction") {
        Ok(sig) => sig,
        Err(e) => return e.to_compile_error().into(),
    };

    let expanded = quote! {
        #(#attrs)*
        #[allow(clippy::clone_on_copy)]
        #vis #sig {
            #ctx_ident.transaction(#name, move |#tx_ident| {
                #( let #arg_idents = #arg_idents.clone(); )*
                ::std::boxed::Box::pin(async move #block)
            })
        }

        // The `tx` parameter is dropped from the signature above (the runtime
        // supplies it), so reference its written type here to keep a
        // `use ..::Tx` import from being reported as unused.
        const _: () = {
            #[allow(dead_code)]
            fn _tx_type_referenced(_: #tx_ty) {}
        };
    };

    expanded.into()
}
