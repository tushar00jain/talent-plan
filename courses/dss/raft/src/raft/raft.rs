use std::sync::{Arc, Mutex};

use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{channel, Receiver};
use futures::future::join_all;

use futures::{select, FutureExt};
use futures_timer::Delay;
use std::time::{Duration, Instant};

use super::errors::*;
use super::persister::*;
use crate::proto::raftpb::*;

pub const RPC_TIMEOUT: u64 = 10;

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

// #[derive(Default)]
// pub struct Log {
//     pub command: u64,
//     pub term: u64,
// }

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,

    pub role: Role,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }
}

// A single Raft peer.
#[derive(Clone)]
pub struct Raft {
    // RPC end points of all peers
    pub peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    pub persister: Arc<Mutex<Box<dyn Persister>>>,
    // this peer's index into peers[]
    pub me: usize,
    pub state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    pub apply_ch: UnboundedSender<ApplyMsg>,

    pub voted_for: Option<u64>,
    pub log: Vec<Log>,

    pub commit_index: u64,
    pub last_applied: u64,

    pub next_index: Vec<u64>,
    pub match_index: Vec<u64>,

    pub last_heartbeat: Option<Instant>,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Mutex<Box<dyn Persister>>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.lock().unwrap().raft_state();

        let len = peers.len();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister: Arc::new(persister),
            me,
            state: Default::default(),
            log: Vec::default(),
            commit_index: 0,
            last_applied: 0,
            next_index: vec![0; len],
            match_index: vec![0; len],
            voted_for: Default::default(),
            last_heartbeat: Default::default(),
            apply_ch,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
        // crate::your_code_here((rf, apply_ch))
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/lib.rs for more details.
    pub fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel();
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    pub fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (tx, rx) = channel();
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    pub fn send_request_vote_to_all(
        &mut self,
    ) -> Receiver<Vec<RequestVoteReply>> {
        // might be guarded when called
        self.state.term += 1;
        self.voted_for = Some(self.me as u64);

        let client = self.peers[self.me].clone();

        let (tx, rx) = channel();

        let fut_replies = self.peers
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != self.me)
        .map(|(i, _)| {
            let last_log_term = self.log.last().unwrap_or(&Log{..Default::default()}).term;

            let args = RequestVoteArgs {
                term: self.state.term(),
                candidate_id: self.me as u64,
                last_log_index: self.log.len() as u64,
                last_log_term,
            };

            self.send_request_vote(i, args)
        })
        .map(|rx| async move {
            select! {
                r = rx.fuse() => r.unwrap().unwrap_or_default(),
                _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => Default::default()
            }
        })
        .collect::<Vec<_>>();

        client.spawn(async move {
            let replies = join_all(fut_replies).await;
            let _ = tx.send(replies);
        });
        rx
    }

    pub fn send_append_entries_to_all(
        &self,
    ) -> Receiver<Vec<(usize, AppendEntriesReply)>> {
        // might be guarded when called
        let client = self.peers[self.me].clone();

        let (tx, rx) = channel();

        let fut_replies = self.peers
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != self.me as usize)
            .map(|(i, _)| {
                let args = self.get_append_entries_args(i);
                (i, self.send_append_entries(i, args))
            })
            .map(|(i, rx)| async move {
                select! {
                    r = rx.fuse() => (i, r.unwrap().unwrap_or_default()),
                    _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => (i, Default::default()),
                }
            })
            .collect::<Vec<_>>();

        client.spawn(async move {
            let replies = join_all(fut_replies).await;
            let _ = tx.send(replies);
        });
        rx
    }

    pub fn get_append_entries_args(&self, server: usize) -> AppendEntriesArgs {
        let prev_log_index = self.next_index[server] - 1;

        let prev_log_term = match prev_log_index {
            0 => 0,
            _ => self.log.get(prev_log_index as usize - 1).unwrap().term,
        };

        let mut entries = Vec::default();

        if self.log.len() >= self.next_index[server] as usize {
            entries = self.log[(self.next_index[server] as usize - 1)..].to_vec();
        }

        AppendEntriesArgs {
            term: self.state.term(),
            leader_id: self.me as u64,
            leader_commit: self.commit_index,
            entries,
            prev_log_index,
            prev_log_term,
        }
    }

    pub fn to_leader(&mut self) {
        let state = &mut self.state;

        state.is_leader = true;
        state.role = Role::Leader;
        self.next_index = vec![self.log.len() as u64 + 1; self.peers.len()];
        self.match_index = vec![0; self.peers.len()];
    }

    pub fn to_follower(&mut self) {
        let state = &mut self.state;

        state.is_leader = false;
        state.role = Role::Follower;
        self.voted_for = None;
    }

    pub fn commit(&mut self) {
        let range = (self.commit_index + 1)..=self.log.len() as u64;
        let n = range.fold(self.commit_index, |acc1, commit_index| {
            let count = (0..self.peers.len())
                .into_iter()
                .filter(|&server| server != self.me as usize)
                .fold(1, |acc2, server| {
                    let match_index = self.match_index[server];

                    if match_index >= commit_index {
                        return acc2 + 1;
                    }

                    acc2
                });

            if count >= self.peers.len() / 2 + 1 {
                return commit_index;
            }

            acc1
        });

        if n > self.commit_index && self.log.get(n as usize - 1).unwrap().term == self.state.term {
            for index in self.commit_index + 1..=n {
                let _ = self.apply_ch.unbounded_send(ApplyMsg::Command {
                    data: self.log.get(index as usize - 1).unwrap().entry.to_vec(),
                    index,
                });

                self.commit_index += 1;
                self.last_applied += 1;
            }
        }
    }

    pub fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let is_leader = self.state.is_leader();

        if !is_leader {
            return Err(Error::NotLeader);
        }

        let index = self.log.len() as u64 + 1;
        let term = self.state.term();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;

        self.log.push(Log { entry: buf, term });

        let client = self.peers[self.me].clone();
        let clone = self.clone();

        // Your code here (2B).
        client.spawn(async move {
            let rx = clone.send_append_entries_to_all();
            let _ = rx.await;
        });

        Ok((index, term))
    }

    fn cond_install_snapshot(
        &mut self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here (2D).
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    fn snapshot(&mut self, index: u64, snapshot: &[u8]) {
        // Your code here (2D).
        crate::your_code_here((index, snapshot));
    }
}

impl Raft {
    /// Only for suppressing deadcode warnings.
    #[doc(hidden)]
    pub fn __suppress_deadcode(&mut self) {
        let _ = self.start(&0);
        let _ = self.cond_install_snapshot(0, 0, &[]);
        let _ = self.snapshot(0, &[]);
        let _ = self.send_request_vote(0, Default::default());
        let _ = self.send_append_entries(0, Default::default());
        self.persist();
        let _ = &self.state;
        let _ = &self.me;
        let _ = &self.persister;
        let _ = &self.peers;
        let _ = &self.log;
        let _ = &self.commit_index;
        let _ = &self.last_applied;
        let _ = &self.next_index;
        let _ = &self.match_index;
    }
}
