use futures::{StreamExt, select, FutureExt};
use futures::channel::mpsc::{unbounded, UnboundedSender};
use futures::executor::block_on;
use rand::Rng;
use std::sync::{Arc, Mutex};
use std::thread;

use futures_timer::Delay;
use std::time::Duration;

use super::errors::*;
use crate::proto::raftpb::*;

use super::raft::*;

pub const HEARTBEAT_TIMEOUT: u64 = 50;
pub const ELECTION_TIMEOUT: u64 = 200;

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    raft: Arc<Mutex<Raft>>,
    election_timeout_tx: Arc<UnboundedSender<()>>,
    heartbeat_timeout_tx: Arc<UnboundedSender<()>>,
}

impl Node {
    async fn start_leader_loop(&self) {
        let clone = &self.raft;

        let guard = clone.lock().unwrap();
        let last_index = guard.log.last_log_index();
        let rx = guard.send_append_entries_to_all();
        drop(guard);

        let replies = rx.await.unwrap();

        let max_term = replies
            .iter()
            .map(|(_, reply)| reply.term)
            .max()
            .unwrap();

        let mut guard = clone.lock().unwrap();
        let state = &mut guard.state;

        if !state.is_leader() {
            return;
        }

        if max_term > state.term {
            guard.to_follower(max_term);
            return;
        }

        replies
            .iter()
            .filter(|(_, reply)| reply.success)
            .for_each(|(server, _)| {
                guard.log.match_index[*server] = last_index;
                guard.log.next_index[*server] = last_index + 1;
            });

        replies
            .iter()
            .filter(|(_, reply)| reply.conflict)
            .for_each(|(server, reply)| {
                guard.log.next_index[*server] = (guard.log.match_index[*server] + 1).min(reply.conflict_index + 1);
            });

        guard.commit();

        let _ = self.heartbeat_timeout_tx.as_ref().unbounded_send(());
    }

    async fn start_candidate_loop(&self) {
        let clone = &self.raft;

        let mut guard = clone.lock().unwrap();
        guard.to_candidate();
        let rx = guard.send_request_vote_to_all();
        drop(guard);

        let replies = rx.await.unwrap();

        let votes_count = 1 + replies
            .iter()
            .filter(|reply| reply.vote_granted)
            .count();

        let max_term = replies
            .iter()
            .map(|reply| reply.term)
            .max()
            .unwrap();

        let mut guard = clone.lock().unwrap();
        let state = &mut guard.state;

        if state.role != Role::Candidate {
            return;
        }

        if max_term > state.term {
            guard.to_follower(max_term);
            return;
        }

        if votes_count >= guard.peers.len() / 2 + 1 {
            guard.to_leader();
            return;
        }
    }

    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let (election_timeout_tx, mut election_timeout_rx) = unbounded();
        let (heartbeat_timeout_tx, mut heartbeat_timeout_rx) = unbounded();

        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
            election_timeout_tx: Arc::new(election_timeout_tx),
            heartbeat_timeout_tx: Arc::new(heartbeat_timeout_tx),
        };

        let clone = node.clone();
        let delay = rand::thread_rng().gen_range(0, 100);

        thread::spawn(move || {
            block_on(async move {
                loop {
                    select! {
                        _ = Delay::new(Duration::from_millis(ELECTION_TIMEOUT + delay)).fuse() => {
                            let role = { clone.raft.lock().unwrap().state.role };
                            
                            match role {
                                Role::Follower | Role::Candidate => clone.start_candidate_loop().await,
                                // _ => unreachable!(),
                                Role::Leader => clone.start_leader_loop().await,
                            }
                        },
                        _ = election_timeout_rx.select_next_some() => {
                            let role = { clone.raft.lock().unwrap().state.role };

                            match role {
                                Role::Leader => clone.start_leader_loop().await,
                                // _ => unreachable!(),
                                Role::Candidate => clone.start_candidate_loop().await,
                                _ => (),
                            }
                        },
                        _ = heartbeat_timeout_rx.select_next_some() => {
                            let role = { clone.raft.lock().unwrap().state.role };

                            match role {
                                Role::Leader => {
                                    Delay::new(Duration::from_millis(HEARTBEAT_TIMEOUT)).await;
                                    clone.start_leader_loop().await
                                },
                                // _ => unreachable!(),
                                Role::Candidate => clone.start_candidate_loop().await,
                                _ => (),
                            }
                        },
                    };
                }
            });
        });

        node
    }

    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        let mut raft = self.raft.lock().unwrap();
        raft.start(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.lock().unwrap().state.term()
        // crate::your_code_here(())
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.lock().unwrap().state.is_leader()
        // crate::your_code_here(())
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            ..Default::default()
        }
    }

    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        // Your code here.
        // Example:
        // self.raft.cond_install_snapshot(last_included_term, last_included_index, snapshot)
        crate::your_code_here((last_included_term, last_included_index, snapshot));
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub fn snapshot(&self, index: u64, snapshot: &[u8]) {
        // Your code here.
        // Example:
        // self.raft.snapshot(index, snapshot)
        crate::your_code_here((index, snapshot));
    }
}

#[async_trait::async_trait]
impl RaftService for Node {
    // example RequestVote RPC handler.
    //
    // CAVEATS: Please avoid locking or sleeping here, it may jam the network.
    async fn request_vote(&self, args: RequestVoteArgs) -> labrpc::Result<RequestVoteReply> {
        // Your code here (2A, 2B).
        let mut guard = self.raft.lock().unwrap();
        let state = &mut guard.state;

        if args.term < state.term {
            return Ok(RequestVoteReply {
                term: state.term,
                vote_granted: false,
            });
        }

        if args.term > state.term {
            guard.to_follower(args.term);
        }

        let can_vote = guard.voted_for
            .map_or(true, |id| id == args.candidate_id);

        if can_vote && guard.log.is_up_to_date_candidate(args.last_log_index, args.last_log_term) {
            guard.voted_for = Some(args.candidate_id);

            let _ = self.election_timeout_tx.as_ref().unbounded_send(());

            guard.persist();

            return Ok(RequestVoteReply {
                term: args.term,
                vote_granted: true,
            });
        }

        Ok(RequestVoteReply {
            term: args.term,
            vote_granted: false,
        })
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> labrpc::Result<AppendEntriesReply> {
        let mut guard = self.raft.lock().unwrap();
        let state = &mut guard.state;

        if args.term < state.term {
            return Ok(AppendEntriesReply {
                term: state.term,
                success: false,
                ..Default::default()
            });
        }

        if args.term > state.term {
            guard.to_follower(args.term);
        }

        let _ = self.election_timeout_tx.as_ref().unbounded_send(());
        
        if let Some(conflict_index) = guard.log.conflict_index_follower(args.prev_log_index, args.prev_log_term) {
            return Ok(AppendEntriesReply {
                term: args.term,
                success: false,
                conflict: true,
                conflict_index,
            });
        }

        guard.log.entries.truncate(args.prev_log_index as usize);
        guard.log.entries.extend(args.entries);
        guard.persist();

        let apply_ch = guard.apply_ch.clone();

        if let Some(next_commit_index) = guard.log.next_commit_index_follower(args.leader_commit) {
            guard.log.commit(
                next_commit_index,
                |index, data| {
                    let _ = apply_ch.unbounded_send(ApplyMsg::Command {
                        data,
                        index,
                    });
                }
            );
        }

        Ok(AppendEntriesReply {
            term: args.term,
            success: true,
            ..Default::default()
        })
    }
}
