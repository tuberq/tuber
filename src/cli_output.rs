//! Output for the client subcommands (`put`, `stats`, `tubes`, `work`) that
//! survives its reader going away.
//!
//! `println!` and `eprintln!` panic when the pipe they write to has closed:
//! `tuber tubes | head -1`, `tuber work 2>&1 | grep -m1 completed`. Losing the
//! reader ends the output, not the command — puts still get made, the worker
//! keeps working, and the exit status is still the command's own.

use std::fmt::Arguments;
use std::io::{self, Write};

/// Write to stdout. A reader that has gone away is not an error; any other
/// write failure is.
pub fn out(args: Arguments<'_>) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    match stdout.write_fmt(args).and_then(|()| stdout.flush()) {
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        written => written,
    }
}

/// Write a line to stderr, dropping it if stderr can't be written: a log or
/// error line is never worth a panic.
pub fn err(args: Arguments<'_>) {
    let _ = writeln!(io::stderr(), "{args}");
}

/// `println!` that evaluates to `io::Result<()>`, and treats a closed stdout
/// as the end of output rather than an error.
#[macro_export]
macro_rules! outln {
    ($($arg:tt)*) => {
        $crate::cli_output::out(format_args!("{}\n", format_args!($($arg)*)))
    };
}

/// `eprintln!` that never panics.
#[macro_export]
macro_rules! errln {
    ($($arg:tt)*) => {
        $crate::cli_output::err(format_args!($($arg)*))
    };
}
