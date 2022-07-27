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
                        .find(|&i| self.get(i).term != term),
                }
            }
        }
    }

    pub fn next_commit_index_leader(&self, leader_server: usize) -> Option<u64> {
        (self.commit_index + 1..=self.last_log_index())
            .rev()
            .find(|&index| {
                let count = 1 + (0..self.match_index.len())
                    .into_iter()
                    .filter(|&server| server != leader_server)
                    .filter(|&server| self.match_index[server] >= index)
                    .count();

                count >= self.match_index.len() / 2 + 1
            })
    }

    pub fn next_commit_index_follower(&self, leader_commit_index: u64) -> Option<u64> {
        match leader_commit_index {
            index if index > self.commit_index => Some(self.last_log_index().min(index)),
            _ => None,
        }
    }

    pub fn commit<F>(
        &mut self, 
        next_commit_index: u64,
        func: F,
    ) where F : Fn(u64, Vec<u8>) {
        for index in self.commit_index + 1..=next_commit_index {
            func(index, self.get(index).data.to_vec());

            self.commit_index += 1;
            self.last_applied += 1;
        }
    }
}
