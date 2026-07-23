use crate::args::Args;
use crate::metrics::Metrics;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

pub fn report(
    metrics: Option<Metrics>,
    latencies: &[Duration],
    errors: usize,
    args: &Args,
) -> anyhow::Result<()> {
    if let Some(m) = metrics {
        println!("-----------------------------------------");
        println!("Mean Latency: {:?}", m.mean);
        println!("P50 (Median) Latency: {:?}", m.p50);
        println!("P90 Latency: {:?}", m.p90);
        println!("P99 Latency: {:?}", m.p99);
        println!("Errors Recorded: {}", errors);
        println!("-----------------------------------------");
    } else {
        println!("No metrics to report");
        println!("Errors Recorded: {}", errors);
    }

    // Output to CSV if directory provided
    if !args.output_dir.is_empty() && !latencies.is_empty() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let path = Path::new(&args.output_dir).join(format!(
            "scenario1_s{}_c{}_{}.csv",
            args.object_size, args.chunk_size, timestamp
        ));
        let mut file = File::create(&path)?;
        writeln!(file, "iteration,latency_ms")?;
        for (i, l) in latencies.iter().enumerate() {
            writeln!(file, "{},{}", i, l.as_millis())?;
        }
        println!("Raw latencies written to {}", path.display());
    }

    Ok(())
}
