use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The name of the bucket to use for the benchmark.
    #[arg(long, env = "GOOGLE_CLOUD_RUST_BENCHMARKS_BUCKET")]
    pub bucket_name: String,

    /// Number of warmup iterations.
    #[arg(long, default_value_t = 5)]
    pub warmup_iterations: usize,

    /// Number of measured iterations.
    #[arg(long, default_value_t = 100)]
    pub measured_iterations: usize,

    /// The size of the object to append.
    #[arg(long, default_value_t = 104_857_600)] // 100 MiB default
    pub object_size: usize,

    /// The size of each append chunk.
    #[arg(long, default_value_t = 262_144)] // 256 KiB default
    pub chunk_size: usize,

    /// Directory for raw CSV output.
    #[arg(long, default_value = ".")]
    pub output_dir: String,
}
