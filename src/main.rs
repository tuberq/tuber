#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    let cmd = tuber::cli::command().mut_subcommand("server", tuber::ee::replication::with_flags);
    tuber::cli::run(cmd, tuber::ee::replication::configure);
}
