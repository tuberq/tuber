//! ee/'s boundary, checked by the compiler.
//!
//! In tuber's own build, ee/ is a module of the crate, so `tuber::` paths
//! (through `extern crate self as tuber`) reach `pub(crate)` items too and
//! nothing stops ee/ leaning on internals. An integration test, though, is
//! a crate of its own depending on tuber. Compiling ee/ here makes `tuber::`
//! mean the library as any other crate sees it: only `pub` items, and
//! `crate::` names this file rather than tuber. A use of anything else
//! fails this target's build, and with it `cargo test`.
//!
//! No tests: building it is the check.

#[path = "../mod.rs"]
#[allow(dead_code, unused_imports)]
mod ee;
