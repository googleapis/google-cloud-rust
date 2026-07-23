use std::time::Duration;

#[derive(Debug)]
pub struct Metrics {
    pub mean: Duration,
    pub p50: Duration,
    pub p90: Duration,
    pub p99: Duration,
}

pub fn compute_metrics(mut latencies: Vec<Duration>) -> Option<Metrics> {
    if latencies.is_empty() {
        return None;
    }

    latencies.sort();

    let sum: Duration = latencies.iter().sum();
    let mean = sum / latencies.len() as u32;
    let p50 = latencies[(latencies.len() as f64 * 0.50).floor() as usize];
    let p90 = latencies[(latencies.len() as f64 * 0.90).floor() as usize];
    let p99 = latencies[(latencies.len() as f64 * 0.99).floor() as usize];

    Some(Metrics {
        mean,
        p50,
        p90,
        p99,
    })
}
