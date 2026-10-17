#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tuber::cli::run(tuber::cli::command(), |_, _| Ok(()));
}
