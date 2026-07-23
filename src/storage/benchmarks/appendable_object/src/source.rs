use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct StatelessSource {
    rng: StdRng,
}

impl StatelessSource {
    pub fn new() -> Self {
        Self {
            rng: StdRng::seed_from_u64(42), // Deterministic seed
        }
    }

    pub fn next_chunk(&mut self, size: usize) -> Bytes {
        let mut buffer = vec![0u8; size];
        self.rng.fill_bytes(&mut buffer);
        Bytes::from(buffer)
    }
}
