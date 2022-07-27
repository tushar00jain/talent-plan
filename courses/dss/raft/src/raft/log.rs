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
    pub fn new(
        next_index: Vec<u64>,
        match_index: Vec<u64>,
    ) -> Self {
        Log {
            next_index,
            match_index,
            ..Default::default()
        }
    }

    pub fn get(&self, index: u64) -> Entry {
        match index {
            0 => Default::default(),
            _ => self.entries
                .get(index as usize - 1)
                .unwrap()
                .clone(),
        }
    }

    pub fn last_log_index(&self) -> u64 {
        self.entries.len() as u64
    }

    pub fn last_log_term(&self) -> u64 {
        self.entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or_default()
    }

    pub fn conflict_index(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
    ) -> Option<u64> {
        match self.last_log_index() {
            index if index < prev_log_index => Some(index),
            _ => {
                match self.get(prev_log_index).term {
                    term if term == prev_log_term => None,
                    term => (0..prev_log_index)
                        .rev()
                        .find(|&i| self.get(i).term != term)
                }
            }
        }
    }
}
