use rand::Rng;
use std::cmp::{max, min};
use std::sync::{Arc, Mutex};

use futures::future::join_all;

use futures::{select, FutureExt};
use futures_timer::Delay;
use std::time::{Duration, Instant};

use super::errors::*;
use crate::proto::raftpb::*;

use super::raft::*;

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
}

impl Node {
    async fn start_leader_loop(&self) {
        let clone = &self.raft;

        let candidate_id = { clone.lock().unwrap().me as u64 };
        let peers = { clone.lock().unwrap().peers.clone() };

        loop {
            let is_leader = { clone.lock().unwrap().state.is_leader() };

            if !is_leader {
                return;
            }

            Delay::new(Duration::from_millis(20)).await;

            let fut_replies = (0..peers.len())
                .into_iter()
                .filter(|&i| i != candidate_id as usize)
                .map(|i| {
                    let guard = clone.lock().unwrap();

                    let args = guard.get_append_entries_args(i);

                    (i, guard.send_append_entries(i, args))
                })
                .map(|(i, rx)| async move {
                    select! {
                        r = rx.fuse() => (i, r.unwrap().unwrap_or_default()),
                        _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => (i, Default::default()),
                    }
                })
                .collect::<Vec<_>>();

            let replies = join_all(fut_replies).await;

            let max_term = replies.iter().fold(0, |acc, (_, reply)| max(acc, reply.term));

            let mut guard = clone.lock().unwrap();
            let state = &mut guard.state;

            if max_term > state.term {
                state.term = max_term;
                state.is_leader = false;
                guard.voted_for = None;
                return;
            }

            for (server, reply) in replies {
                if reply.success {
                    guard.match_index[server] = guard.log.len() as u64;
                    guard.next_index[server] = guard.log.len() as u64 + 1;
                }

                if reply.conflict {
                    guard.next_index[server] -= 1;
                }
            }

            guard.commit();
        }
    }

    async fn start_follower_loop(&self) {
        let clone = &self.raft;

        let delay = rand::thread_rng().gen_range(50, 100);

        loop {
            let deadline = Instant::now();

            Delay::new(Duration::from_millis(200 + delay)).await;

            let guard = clone.lock().unwrap();

            if guard.last_heartbeat.is_none() || guard.last_heartbeat.unwrap() < deadline {
                return;
            }
        }
    }

    async fn start_candidate_loop(&self) {
        let clone = &self.raft;

        let candidate_id = { clone.lock().unwrap().me as u64 };
        let peers = { clone.lock().unwrap().peers.clone() };

        let delay = rand::thread_rng().gen_range(0, 5);

        loop {
            Delay::new(Duration::from_millis(5 + delay)).await;

            let term = {
                let mut guard = clone.lock().unwrap();

                guard.state.term += 1;
                guard.voted_for = Some(candidate_id);
                guard.state.term
            };

            let fut_votes = (0..peers.len())
                .into_iter()
                .filter(|&i| i != candidate_id as usize)
                .map(|i| {
                    let guard = clone.lock().unwrap();
                    
                    let last_log_term = guard.log.last().unwrap_or(&Log{..Default::default()}).term;

                    let args = RequestVoteArgs {
                        term,
                        candidate_id,
                        last_log_index: guard.log.len() as u64,
                        last_log_term,
                    };

                    guard.send_request_vote(i, args)
                })
                .map(|rx| async move {
                    select! {
                        r = rx.fuse() => r.unwrap().unwrap_or_default(),
                        _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => Default::default()
                    }
                })
                .collect::<Vec<_>>();

            let votes = join_all(fut_votes).await;

            let max_term = votes.iter().fold(0, |acc, reply| max(acc, reply.term));

            let mut guard = clone.lock().unwrap();
            let state = &mut guard.state;

            if max_term > state.term {
                state.term = max_term;
                state.is_leader = false;
                guard.voted_for = None;
                return;
            }

            // state got corrupted while requesting vote
            // i.e. someone else requested vote
            if state.term > term {
                return;
            }

            let votes_count = votes.iter().fold(1, |acc, reply| {
                if reply.vote_granted {
                    return acc + 1;
                }

                acc
            });

            if votes_count >= peers.len() / 2 + 1 {
                state.is_leader = true;
                guard.next_index = vec![guard.log.len() as u64 + 1; peers.len()];
                guard.match_index = vec![0; peers.len()];
                return;
            }
        }
    }

    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let client = raft.peers[raft.me].clone();
        let node = Node {
            raft: Arc::new(Mutex::new(raft)),
        };

        let clone = node.clone();

        client.spawn(async move {
            loop {
                clone.start_leader_loop().await;
                clone.start_follower_loop().await;
                clone.start_candidate_loop().await;
            }
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
            state.term = args.term;
            state.is_leader = false;
            guard.voted_for = None;
        }

        let last_log_term = guard
            .log
            .last()
            .unwrap_or(&Log {
                ..Default::default()
            })
            .term;

        let can_vote = guard.voted_for.is_none() || guard.voted_for == Some(args.candidate_id);
        let is_up_to_date = args.last_log_term > last_log_term
            || (args.last_log_term == last_log_term
                && args.last_log_index as usize >= guard.log.len());

        if can_vote && is_up_to_date {
            guard.voted_for = Some(args.candidate_id);

            guard.last_heartbeat = Some(Instant::now());

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
            state.term = args.term;
            state.is_leader = false;
            guard.voted_for = None;
        }

        let prev_log = match args.prev_log_index {
            0 => None,
            _ => guard.log.get(args.prev_log_index as usize - 1),
        };

        if prev_log.is_some() && prev_log.unwrap().term != args.prev_log_term {
            return Ok(AppendEntriesReply {
                term: args.term,
                success: false,
                conflict: true,
            });
        }

        if prev_log.is_none() && args.prev_log_index > guard.log.len() as u64 {
            return Ok(AppendEntriesReply {
                term: args.term,
                success: false,
                conflict: true,
            });
        }

        guard.log = guard.log[..args.prev_log_index as usize].to_vec();

        for entry in &args.entries {
            guard.log.push(entry.clone());
        }

        if args.leader_commit > guard.commit_index {
            guard.commit_index = min(args.leader_commit, guard.log.len() as u64);
        }

        if guard.commit_index > guard.last_applied {
            for index in (guard.last_applied + 1)..=guard.commit_index {
                let _ = guard.apply_ch.unbounded_send(ApplyMsg::Command {
                    data: guard.log.get(index as usize - 1).unwrap().entry.to_vec(),
                    index,
                });

                guard.last_applied += 1;
            }
        }

        guard.last_heartbeat = Some(Instant::now());

        Ok(AppendEntriesReply {
            term: args.term,
            success: true,
            ..Default::default()
        })
    }
}
