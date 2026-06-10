//! Procedural macros for `durust`.
//!
//! The only macro is [`macro@workflow`], the Rust analog of Python's
//! `@DBOS.workflow()` decorator: it leaves your async fn untouched and emits a
//! compile-time registration so the engine discovers it automatically — no
//! manual `engine.register(...)` call, and the workflow name defaults to the
//! function name.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitStr};

/// Register an `async fn(DurableContext, Input) -> Result<Output>` as a durable
/// workflow.
///
/// ```ignore
/// #[durust::workflow]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
///
/// // Override the registered name:
/// #[durust::workflow("orders.process")]
/// async fn process_order(ctx: DurableContext, order: Order) -> Result<Receipt> { ... }
/// ```
///
/// The function is left as-is; the macro additionally submits an
/// `inventory` registration. `DurableEngine::new` collects every such
/// registration in the binary, so annotated workflows need no manual
/// `register` call.
#[proc_macro_attribute]
pub fn workflow(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    // Name defaults to the function's identifier; `#[workflow("name")]` overrides it.
    let name = if attr.is_empty() {
        func.sig.ident.to_string()
    } else {
        parse_macro_input!(attr as LitStr).value()
    };

    let ident = &func.sig.ident;

    let expanded = quote! {
        #func

        durust::inventory::submit! {
            durust::WorkflowRegistration {
                name: #name,
                // A non-capturing closure coerces to `fn() -> WorkflowFn`.
                // `erase` infers the Input/Output types from the fn signature.
                builder: || durust::erase(#ident),
            }
        }
    };

    expanded.into()
}
