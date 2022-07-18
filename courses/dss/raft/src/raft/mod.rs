use std::cmp::max;
use std::sync::{Arc, Mutex};

use futures::{StreamExt, select};
use futures::channel::mpsc::{UnboundedSender, channel, Receiver};
use futures::executor::ThreadPool;
use futures::future::join_all;

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

pub struct Log {
    pub command: Vec<u8>,
    pub term: u64,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,

    pub voted_for: Option<u64>,
    pub last_heartbeat: Option<Instant>,
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
    state: Arc<Mutex<State>>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    log: Vec<Log>,
    commit_index: u64,
    last_applied: u64,
    next_index: Vec<u64>,
    match_index: Vec<u64>,
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
        _apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.lock().unwrap().raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            log: Vec::default(),
            commit_index: 0,
            last_applied: 0,
            next_index: Vec::default(),
            match_index: Vec::default(),
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
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (mut tx, rx) = channel(1000);
        peer.spawn(async move {
            let res = peer_clone.request_vote(&args).await.map_err(Error::Rpc);
            tx.try_send(res).unwrap();
        });
        rx
        // ```
        // let (tx, rx) = sync_channel::<Result<RequestVoteReply>>(1);
        // crate::your_code_here((server, args, tx, rx))
    }

    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let peer_clone = peer.clone();
        let (mut tx, rx) = channel(1000);
        peer.spawn(async move {
            let res = peer_clone.append_entries(&args).await.map_err(Error::Rpc);
            tx.try_send(res).unwrap();
        });
        rx
    }

    fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = 0;
        let term = 0;
        let is_leader = true;
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).

        if is_leader {
            Ok((index, term))
        } else {
            Err(Error::NotLeader)
        }
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
    /// Create a new raft service.
    pub fn new(rf: Raft) -> Node {
        // Your code here.
        let candidate_id = rf.me as u64;
        let peers = rf.peers.len();

        let raft = Arc::new(Mutex::new(rf));
        let clone = raft.clone();

        let pool = ThreadPool::new().unwrap();

        let mut rng = rand::thread_rng();
        let delay = rng.gen_range(50, 100);

        pool.spawn_ok(async move {
            loop {
                let t = Instant::now();
                Delay::new(Duration::from_millis(200 + delay)).await;

                let is_leader = { clone.lock().unwrap().state.lock().unwrap().is_leader() };

                let term = { clone.lock().unwrap().state.lock().unwrap().term() };

                if is_leader {
                    let args = AppendEntriesArgs{
                        term,
                        leader_id: candidate_id,
                        prev_log_index: 0,
                        prev_log_term: 0,
                        entries: Default::default(),
                        leader_commit: 0,
                    };

                    let fut_replies = (0..peers)
                        .into_iter()
                        .filter(|&i| i != candidate_id as usize)
                        .map(|i| {
                            debug!("send_append_entries {} -> {}", candidate_id, i);
                            clone.lock().unwrap().send_append_entries(i, args.clone())
                        })
                        .map(|mut rx| async move {
                            select! {
                                response = rx.next() => response.unwrap().unwrap(),
                            }
                        })
                        .collect::<Vec<_>>();

                    let replies = join_all(fut_replies).await;

                    let max_term = replies.iter().fold(term, |acc, reply| max(acc, reply.term));

                    if max_term > term {
                        let guard = clone.lock().unwrap();
                        let mut state = guard.state.lock().unwrap();
                        state.term = max_term;
                        state.voted_for = None;
                        state.is_leader = false;
                    }

                    continue;
                }

                let last_heartbeat = { clone.lock().unwrap().state.lock().unwrap().last_heartbeat };

                match last_heartbeat {
                    Some(i) => {
                        if i > t {
                            continue;
                        }
                    }
                    _ => {}
                }

                let term = {
                    let guard = clone.lock().unwrap();
                    let mut state = guard.state.lock().unwrap();
                    state.voted_for = Some(candidate_id);
                    state.term += 1;
                    state.term
                };

                let args = RequestVoteArgs {
                    term,
                    candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                };

                let fut_votes = (0..peers)
                    .into_iter()
                    .filter(|&i| i != candidate_id as usize)
                    .map(|i| {
                        debug!("send_request_vote {} -> {}", candidate_id, i);
                        clone.lock().unwrap().send_request_vote(i, args.clone())
                    })
                    .map(|mut rx| async move {
                        select! {
                            response = rx.next() => response.unwrap().unwrap(),
                        }
                    })
                    .collect::<Vec<_>>();

                let votes = join_all(fut_votes).await;

                if votes.iter().any(|reply| reply.term > term) {
                    continue;
                }

                let votes_count = votes.iter().fold(1, |acc, reply| {
                    if reply.vote_granted {
                        return acc + 1;
                    }

                    acc
                });

                let guard = clone.lock().unwrap();
                let mut state = guard.state.lock().unwrap();

                if votes_count >= peers / 2 + 1 {
                    state.is_leader = true;
                }

                debug!("votes_count {} : {}", candidate_id, votes_count);
            }
        });

        Node { raft }
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
        crate::your_code_here(command)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        self.raft.to_owned().lock().unwrap().state.to_owned().lock().unwrap().term()
        // crate::your_code_here(())
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        self.raft.to_owned().lock().unwrap().state.to_owned().lock().unwrap().is_leader()
        // crate::your_code_here(())
    }

    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            last_heartbeat: self.raft.lock().unwrap().state.lock().unwrap().last_heartbeat,
            voted_for: self.raft.lock().unwrap().state.lock().unwrap().voted_for,
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
        let clone = self.raft.clone();
        let raft = clone.lock().unwrap();
        let mut state = raft.state.lock().unwrap();

        if args.term < state.term {
            return Ok(RequestVoteReply {
                term: state.term,
                vote_granted: false,
            });
        }

        if args.term > state.term {
            state.term = args.term;
            state.is_leader = false;
            state.voted_for = None;
        }
        
        if state.voted_for.is_none() || state.voted_for == Some(args.candidate_id) {
            state.voted_for = Some(args.candidate_id);

            // TODO
            state.last_heartbeat = Some(Instant::now());

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
        let clone = self.raft.clone();
        let guard = clone.lock().unwrap();
        let mut state = guard.state.lock().unwrap();

        if args.term < state.term {
            return Ok(AppendEntriesReply {
                term: state.term,
                success: false,
            })
        }

        if args.term > state.term {
            state.term = args.term;
            state.is_leader = false;
        }

        state.last_heartbeat = Some(Instant::now());

        Ok(AppendEntriesReply {
            term: args.term,
            success: true,
        })
    }
}
