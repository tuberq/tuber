//! Logging setup, and a panic hook that routes panics into it.

/// Install the tracing subscriber and the panic hook. Call once, early, before
/// any thread that might panic.
pub fn init(console_level: tracing::Level) {
    tracing_subscriber::fmt()
        .with_max_level(console_level)
        .init();
    install_panic_hook();
}

/// Report panics through `tracing` rather than a bare stderr write.
///
/// A panic inside a per-connection tokio task unwinds that task alone: the
/// server keeps serving, `JoinHandle` swallows the error, and the default hook's
/// stderr line is the only trace it ever leaves — unstructured, and easy for a
/// log pipeline to miss. Emitting `error!` puts panics on the same path as every
/// other server error, at a level that alerting can key on.
///
/// The hook is global, so it covers connection tasks, the WAL/TOAST background
/// tasks, and the metrics server without any of them opting in.
fn install_panic_hook() {
    // Chain rather than replace: the default hook is what prints the panic in
    // test binaries and honours RUST_BACKTRACE, and dropping that would make
    // debugging a failing test worse to buy nothing.
    let default_hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |info| {
        // `info.payload()` is `&str` for `panic!("literal")` and `String` once
        // the message is formatted; anything else is a `panic_any` we can't read.
        let payload = info
            .payload()
            .downcast_ref::<&str>()
            .map(|s| &**s)
            .or_else(|| info.payload().downcast_ref::<String>().map(|s| s.as_str()))
            .unwrap_or("<non-string panic payload>");

        let location = info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "<unknown>".to_string());

        tracing::error!(
            panic.payload = payload,
            panic.location = %location,
            thread = std::thread::current().name().unwrap_or("<unnamed>"),
            "panicked",
        );

        default_hook(info);
    }));
}
