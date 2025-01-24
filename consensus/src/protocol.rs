//! Contains the network-message-types for the consensus protocol and banking application.
use crate::network::Channel;

use std::time::{Duration, Instant, self};
use rand::Rng;

/// Message-type of the network protocol.
#[derive(Debug, Clone)]
pub enum Command {
	/// Open an account with a unique name.
	Open { account: String },
	
	/// Deposit money into an account.
	Deposit { account: String, amount: usize },
	
	/// Withdraw money from an account.
	Withdraw { account: String, amount: usize },
	
	/// Transfer money between accounts.
	Transfer { src: String, dst: String, amount: usize },
	
	/// Accept a new network connection.
	Accept(Channel<Command>),

	RaftMessage(Box<RaftMessage>),
	
	// TODO: add other useful control messages
}





// TODO: add other useful structures and implementations
use std::collections::HashMap;
use std::ops::Sub;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use tracing::debug;

/// Raft states
#[derive(Debug, Clone, PartialEq)]
pub enum RaftState {
	Follower,
	Candidate,
	Leader,
}

/// Raft message types
#[derive(Debug, Clone)]
pub enum RaftMessage {
	RequestVote {
		term: u64,
		candidate_id: u64,
		last_log_index: u64,
		last_log_term: u64,
	},
	VoteResponse {
		term: u64,
		vote_granted: bool,
	},
	AppendEntries {
		term: u64,
		leader_id: u64,
		prev_log_index: u64,
		prev_log_term: u64,
		entries: Vec<LogEntry>,
		leader_commit: u64,
	},
	AppendResponse {
		term: u64,
		index: u64,
		id: u64,
		success: bool,
	},
	ClientRequest(Command),
}

/// Log entry structure
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LogEntry {
	pub term: u64,
	pub command: String,
}

/// Raft node structure
pub struct RaftNode {
	pub id: u64,
	pub state: RaftState,
	pub current_term: u64,
	pub voted_for: Option<u64>,
	pub log: Vec<LogEntry>,
	pub commit_index: u64,
	pub last_applied: u64,
	pub next_index: HashMap<u64, u64>,
	pub match_index: HashMap<u64, u64>,
	pub peers: Vec<u64>,
	pub message_queue: Arc<Mutex<Vec<(u64, RaftMessage)>>>,
	pub votes: HashMap<u64, bool>,
	pub leader_id: u64,
	last_heartbeat: Instant,
}

impl RaftNode {
	pub fn new(id: u64, peers: Vec<u64>) -> Self {

		Self {
			id: (id + 1),
			state: Self::select_first_leader(id),
			current_term: 0,
			voted_for: None,
			log: Vec::new(),
			commit_index: 0,
			last_applied: 0,
			next_index: HashMap::new(),
			match_index: HashMap::new(),
			peers,
			message_queue: Arc::new(Mutex::new(Vec::new())),
			votes: HashMap::new(),
			leader_id: 0,
			last_heartbeat: Instant::now(),
		}
	}

	fn select_first_leader(id: u64) -> RaftState{
		if id != 0 {return RaftState::Follower}
		debug!("Leader selected: {}", id);
		RaftState::Leader
	}

	/// Handles incoming messages
	pub fn handle_message(&mut self, src: u64, message: RaftMessage) {
		if src == self.leader_id {
			match message {
				RaftMessage::RequestVote {
					term,
					candidate_id,
					last_log_index,
					last_log_term,
				} => {
					self.handle_request_vote(src, term, candidate_id, last_log_index, last_log_term);
				}
				RaftMessage::AppendEntries {
					term,
					leader_id,
					prev_log_index,
					prev_log_term,
					entries,
					leader_commit,
				} => {
					self.handle_append_entries(
						src,
						term,
						leader_id,
						prev_log_index,
						prev_log_term,
						entries,
						leader_commit,
					);
				}
				_ => {}
			}
		}
		else { debug!("MESSAGE NOT FROM LEADER!") }
	}

	/// Handles RequestVote RPC
	fn handle_request_vote(&mut self, _src: u64, term: u64, candidate_id: u64, last_log_index: u64, last_log_term: u64) {
		if term > self.current_term {
			self.current_term = term;
			self.voted_for = None;
			self.state = RaftState::Follower;
		}

		let vote_granted = if self.voted_for.is_none() || self.voted_for == Some(candidate_id) {
			let log_ok = self.log.last().map_or(true, |entry| {
				entry.term < last_log_term || (entry.term == last_log_term && self.log.len() as u64 <= last_log_index)
			});
			log_ok
		} else {
			false
		};

		if vote_granted {
			self.voted_for = Some(candidate_id);
		}

		self.send_message(
			_src,
			RaftMessage::VoteResponse {
				term: self.current_term,
				vote_granted,
			},
		);
	}

	/// Handles AppendEntries RPC
	fn handle_append_entries(&mut self, _src: u64, term: u64, leader_id: u64, prev_log_index: u64, prev_log_term: u64, entries: Vec<LogEntry>, leader_commit: u64) {
		if term >= self.current_term {
			self.current_term = term;
			self.state = RaftState::Follower;
			self.leader_id = leader_id;
		}

		let success = if let Some(entry) = self.log.get(prev_log_index as usize) {
			entry.term == prev_log_term
		} else {
			prev_log_index == 0
		};

		if success {
			self.log.truncate(prev_log_index as usize + 1);
			self.log.extend(entries);
			self.commit_index = leader_commit.min(self.log.len() as u64);
		}

		self.send_message(
			_src,
			RaftMessage::AppendResponse {
				term: self.current_term,
				index: prev_log_index + 1,
				id: self.id,
				success,
			},
		);
	}

	/// Sends a message to a specific peer
	fn send_message(&self, target: u64, message: RaftMessage) {
		let mut queue = self.message_queue.lock().unwrap();
		queue.push((target, message));
	}
	/// Starts a new election by transitioning to Candidate state.
	pub fn start_election(&mut self) {
		self.current_term += 1;
		self.voted_for = Some(self.id);
		self.state = RaftState::Candidate;

		let votes_needed = (self.peers.len() as u64 + 1) / 2 + 1; // Majority
		let votes = 1; // Self-vote
		let mut log_length: u64 = 0;
		if self.log.len() > 0 {log_length = self.log.len() as u64 - 1}

		// Send RequestVote to all peers
		for &peer in &self.peers {
			self.send_message(
				peer,
				RaftMessage::RequestVote {
					term: self.current_term,
					candidate_id: self.id,
					last_log_index: log_length,
					last_log_term: self.log.last().map_or(0, |entry| entry.term),
				},
			);
		}

		// Collect votes
		if votes >= votes_needed {
			self.become_leader();
		}
	}

	/// Handles a VoteResponse.
	pub fn handle_vote_response(&mut self, _src: u64, term: u64, vote_granted: bool) {
		if term == self.current_term && vote_granted {
			let votes_needed = (self.peers.len() as u64 + 1) / 2 + 1;
			let votes: u64 = self.votes.iter().filter(|&(_, &v)| v).count() as u64;

			if votes >= votes_needed {
				self.become_leader();
			}
		} else if term > self.current_term {
			self.become_follower(term);
		}
	}

	/// Becomes the leader.
	pub fn become_leader(&mut self) {
		if self.state == RaftState::Candidate {
			self.state = RaftState::Leader;
			self.leader_id = self.id;
			for &peer in &self.peers {
				self.next_index.insert(peer, self.log.len() as u64 + 1);
				self.match_index.insert(peer, 0);
			}
			self.send_heartbeat();
		}
	}
	/// Becomes a follower.
	pub fn become_follower(&mut self, term: u64) {
		self.state = RaftState::Follower;
		self.current_term = term;
		self.voted_for = None;
	}

	/// Sends periodic heartbeats.
	pub fn send_heartbeat(&mut self) {
		let mut log_len: u64 = 0;
		if self.log.len() > 0 {log_len = self.log.len() as u64 - 1}
		for &peer in &self.peers {
			self.send_message(
				peer,
				RaftMessage::AppendEntries {
					term: self.current_term,
					leader_id: self.id,
					prev_log_index: log_len,
					prev_log_term: self.log.last().map_or(0, |entry| entry.term),
					entries: vec![],
					leader_commit: self.commit_index,
				},
			);
		}
	}

	pub fn check_leader_condition(&mut self) {
		if self.state == RaftState::Leader {self.send_heartbeat()}
		else {
			let heartbeat_threshold = time::Duration::from_millis(1000);
			// let time_elapsed = Instant::now() - self.last_heartbeat;
			if (self.leader_id == 0 ){
				self.start_election()
			}
		}

	}

	/// Handles timeouts for follower state.
	pub fn handle_timeout(&mut self) {
		if self.state == RaftState::Follower {
			self.start_election();
		}
	}
}


/// Helper macro for defining test-scenarios.
/// 
/// The basic idea is to write test-cases and then observe the behavior of the
/// simulated network through the tracing mechanism for debugging purposes.
/// 
/// The macro defines a mini-language to easily express sequences of commands
/// which are executed concurrently unless you explicitly pass time between them.
/// The script needs some collection of channels to operate over which has to be
/// provided as the first command (see next section).
/// Commands are separated by semicolons and are either requests (open an
/// account, deposit money, withdraw money and transfer money between accounts)
/// or other commands (currently only sleep).
/// 
/// # Examples
/// 
/// The following script creates two accounts (Foo and Bar) in different branch
/// offices, deposits money in the Foo-account, waits a second, transfers it to
/// bar, waits another half second and withdraws the money. The waiting periods
/// are critical, because we need to give the consensus-protocol time to confirm
/// the sequence of transactions before referring to changes made in a different
/// branch office. Within one branch office the timing is not important since
/// the commands are always delivered in sequence.
/// 
/// ```rust
///     let channels: Vec<Channel<_>>;
///     script! {
///         use channels;
///         [0] "Foo" => open(), deposit(10);
///         [1] "Bar" => open();
///         sleep();   // the argument defaults to 1 second
///         [0] "Foo" => transfer("Bar", 10);
///         sleep(0.5);// may also sleep for fractions of a second
///         [1] "Bar" => withdraw(10);
///     }
/// ```
#[macro_export]
macro_rules! script {
	// empty base case
	(@expand $chan_vec:ident .) => {};
	
	// meta-rule for customer requests
	(@expand $chan_vec:ident . [$id:expr] $acc:expr => $($cmd:ident($($arg:expr),*)),+; $($tail:tt)*) => {
		$(
			$chan_vec[$id].send(
				script! { @request $cmd($acc, $($arg),*) }
			);
		)*
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// meta-rule for other commands
	(@expand $chan_vec:ident . $cmd:ident($($arg:expr),*); $($tail:tt)*) => {
		script! { @command $cmd($($arg),*) }
		script! { @expand $chan_vec . $($tail)* }
	};
	
	// customer requests
	(@request open($holder:expr,)) => {
		$crate::protocol::Command::Open {
			account: $holder.into()
		}
	};
	(@request deposit($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Deposit {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request withdraw($holder:expr, $amount:expr)) => {
		$crate::protocol::Command::Withdraw {
			account: $holder.into(),
			amount: $amount
		}
	};
	(@request transfer($src:expr, $dst:expr, $amount:expr)) => {
		$crate::protocol::Command::Transfer {
			src: $src.into(),
			dst: $dst.into(),
			amount: $amount
		}
	};
	
	// other commands
	(@command sleep($time:expr)) => {
		std::thread::sleep(std::time::Duration::from_millis(($time as f64 * 1000.0) as u64));
	};
	(@command sleep()) => {
		std::thread::sleep(std::time::Duration::from_millis(1000));
	};
	
	// entry point for the user
	(use $chan_vec:expr; $($tail:tt)*) => {
		let ref channels = $chan_vec;
		script! { @expand channels . $($tail)* }	
	};
	
	// rudimentary error diagnostics
	(@request $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean one of open, deposit, withdraw or transfer?")
	};
	(@command $cmd:ident $($tail:tt)*) => {
		compile_error!("maybe you mean sleep or forgot the branch index?")
	};
	(@expand $($tail:tt)*) => {
		compile_error!("illegal command syntax")
	};
	($($tail:tt)*) => {
		compile_error!("missing initial 'use <channels>;'")
	};
}
