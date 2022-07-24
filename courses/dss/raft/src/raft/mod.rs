use std::cmp::max;
use std::sync::{Arc, Mutex};
use std::thread;

use futures::channel::mpsc::UnboundedSender;
use futures::channel::oneshot::{Receiver, channel};
use futures::executor::block_on;
use futures::future::join_all;

use futures::{select, FutureExt};
use futures_timer::Delay;
use rand::Rng;
use std::time::{Duration, Instant};

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use crate::proto::raftpb::*;

const RPC_TIMEOUT: u64 = 50;

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
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Mutex<Box<dyn Persister>>,
    // this peer's index into peers[]
    me: usize,
    state: State,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    apply_ch: UnboundedSender<ApplyMsg>,

    voted_for: Option<u64>,
    log: Vec<Log>,

    commit_index: u64,
    last_applied: u64,

    next_index: Vec<u64>,
    match_index: Vec<u64>,

    last_heartbeat: Option<Instant>,
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

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Default::default(),
            log: Vec::default(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::default(),
            match_index: Vec::default(),
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
    fn send_request_vote(
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

    fn send_append_entries(
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

    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let is_leader = self.state.is_leader();

        if !is_leader {
            return Err(Error::NotLeader)
        }

        let index = self.log.len() as u64 + 1;
        let term = self.state.term();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        
        let buf_clone = buf.clone();
        // Your code here (2B).
        block_on(async {
            self.log.push(Log{
                entry: buf,
                term,
            });

            let fut_replies = (0..self.peers.len())
                .into_iter()
                .filter(|&i| i != self.me as usize)
                .map(|i| {
                    let args = AppendEntriesArgs{
                        term,
                        leader_id: self.me as u64,
                        leader_commit: self.commit_index,
                        prev_log_index: index - 1,
                        prev_log_term: {
                            match self.log.get(index as usize - 1) {
                                Some(log) => log.term,
                                None => 0,
                            }
                        },
                        entries: self.log.to_vec(),
                    };

                    self.send_append_entries(i, args)
                })
                .map(|rx| async move {
                    select! {
                        r = rx.fuse() => r.unwrap().unwrap_or_default(),
                        _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => Default::default(),
                    }
                })
                .collect::<Vec<_>>();

            join_all(fut_replies).await;
            let _ = self.apply_ch.unbounded_send(ApplyMsg::Command {
                data: buf_clone,
                index,
            });
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

            let term = { clone.lock().unwrap().state.term() };

            let fut_replies = (0..peers.len())
                .into_iter()
                .filter(|&i| i != candidate_id as usize)
                .map(|i| {
                    let guard = clone.lock().unwrap();

                    let args = AppendEntriesArgs{
                        term,
                        leader_id: candidate_id,
                        ..Default::default()
                    };

                    guard.send_append_entries(i, args)
                })
                .map(|rx| async move {
                    select! {
                        r = rx.fuse() => r.unwrap().unwrap_or_default(),
                        _ = Delay::new(Duration::from_millis(RPC_TIMEOUT)).fuse() => Default::default(),
                    }
                })
                .collect::<Vec<_>>();

            let replies = join_all(fut_replies).await;

            let max_term = replies.iter().fold(0, |acc, reply| max(acc, reply.term));

            let mut guard = clone.lock().unwrap();
            let state = &mut guard.state;

            if max_term > state.term {
                state.term = max_term;
                state.is_leader = false;
                guard.voted_for = None;
            }
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
                return
            }
        }
    }

    async fn start_candidate_loop(&self) {
        let clone = &self.raft;

        let candidate_id = { clone.lock().unwrap().me as u64 };
        let peers = { clone.lock().unwrap().peers.clone() };

        loop {
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
                return;
            }
        }
    }

    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node { raft: Arc::new(Mutex::new(raft)) };

        let clone = node.clone();

        thread::spawn(move || {
            block_on(async {
                loop {
                    clone.start_leader_loop().await;
                    clone.start_follower_loop().await;
                    clone.start_candidate_loop().await;
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
            state.term = args.term;
            state.is_leader = false;
            guard.voted_for = None;
        }

        let last_log_term = guard.log.last().unwrap_or(&Log{..Default::default()}).term;

        let can_vote = guard.voted_for.is_none() || guard.voted_for == Some(args.candidate_id);
        let is_up_to_date = args.last_log_term > last_log_term || (args.last_log_term == last_log_term && args.last_log_index as usize >= guard.log.len());

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
            })
        }

        if args.term > state.term {
            state.term = args.term;
            state.is_leader = false;
            guard.voted_for = None;
        }

        if !args.entries.is_empty() {
            let prev_log = match args.prev_log_index {
                0 => None,
                _ => guard.log.get(args.prev_log_index as usize - 1),
            };

            if prev_log.is_some() && prev_log.unwrap().term != args.prev_log_term {
                return Ok(AppendEntriesReply {
                    term: args.term,
                    success: false,
                })
            }

            for i in 0..args.entries.len() {
                let log = args.entries.get(i).unwrap();

                if guard.log.get(i).is_none() {
                    guard.log.push(log.clone());
                }

                if guard.log.get(i).unwrap().term != log.term {
                    guard.log = guard.log[..i].to_vec();
                }
            }

            let _ = guard.apply_ch.unbounded_send(ApplyMsg::Command {
                data: guard.log.last().unwrap().entry.to_vec(),
                index: guard.log.len() as u64,
            });
        }

        guard.last_heartbeat = Some(Instant::now());

        Ok(AppendEntriesReply {
            term: args.term,
            success: true,
        })
    }
}
