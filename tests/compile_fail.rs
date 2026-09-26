//! The workflow context is lent, not owned: moving durable work into a task the
//! engine does not drive must fail to compile, while in-scope concurrency must
//! not. Cases live in `tests/compile_fail/` and `tests/compile_pass/`.

#[test]
fn borrowed_context_cannot_escape_the_workflow() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/compile_fail/*.rs");
    t.pass("tests/compile_pass/*.rs");
}
