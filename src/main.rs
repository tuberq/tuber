#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tuber::cli::run(
        |cmd| cmd.mut_subcommand("server", tuber::ee::replication::with_flags),
        tuber::ee::replication::configure,
    );
}
