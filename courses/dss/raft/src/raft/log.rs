use crate::proto::raftpb::*;

#[derive(Clone, Default)]
pub struct Log {
    pub entries: Vec<Entry>,

    pub commit_index: u64,
    pub last_applied: u64,

    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>,
}

impl Log {
    pub fn new(num_peers: usize) -> Self {
        Log {
            next_index: vec![0; num_peers],
            match_index: vec![0; num_peers],
            ..Default::default()
        }
    }

    pub fn last_log_term(&self) -> u64 {
        self.entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default()
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        match index {
            0 => None,
            _ => self.entries.get(index - 1),
        }
    }
}
