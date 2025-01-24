//! Implementation of the Raft Consensus Protocol for a banking application.

use std::{env::args, fs, io, thread, time};
use std::thread::sleep;
use std::time::Duration;
use rand::prelude::*;
#[allow(unused_imports)]
use tracing::{debug, info, Level, trace, trace_span};
use std::collections::HashMap;
use std::ops::{Add, Sub};
use crate::protocol::{RaftNode, RaftState};
use network::{Channel, daemon, NetworkNode};
use protocol::Command;

pub mod network;
pub mod protocol;


/// Creates and connects a number of branch offices for the bank.
pub fn setup_offices(office_count: usize, log_path: &str) -> io::Result<Vec<Channel<Command>>> {
	let mut channels = Vec::with_capacity(office_count);
	
	// create the log directory if needed
	fs::create_dir_all(log_path)?;
	
	// create various network nodes and start them
	for address in 0..office_count {
		let mut node: NetworkNode<Command> = NetworkNode::new(address, office_count, &log_path)?;
		debug!("Node: {}", node.address);
		channels.push(node.channel());
		
		thread::spawn(move || {
			// configure a span to associate log-entries with this network node
			let _guard = trace_span!("NetworkNode", id = node.address);
			let _guard = _guard.enter();

			// State for this branch office
			let mut accounts: HashMap<String, usize> = HashMap::new();

			// dispatching event loop
			while let Ok(cmd) = node.decode(None) {
				if node.raft_node.state == RaftState::Leader {
					match cmd {
						// customer requests
						Command::Open { account } => {
							if accounts.contains_key(&account) {
								debug!("Account {:?} already exists", account);
							} else {
								accounts.insert(account.clone(), 0);
								debug!("Opened account for {:?}", account);

							}
						}
						Command::Deposit { account, amount} => {
							if let Some(balance) = accounts.get_mut(&account) {
								*balance += amount;
								debug!("Deposited {} to {:?}, new balance: {}", amount, account, *balance);
							} else {
								debug!("Account {:?} does not exist", account);
							}
						}
						Command::Withdraw { account, amount } => {
							if let Some(balance) = accounts.get_mut(&account) {
								if *balance >= amount {
									*balance -= amount;
									debug!("Withdrew {} from {:?}, new balance: {}", amount, account, *balance);
								} else {
									debug!("Insufficient funds in account {:?}", account);
								}
							} else {
								debug!("Account {:?} does not exist", account);
							}
						}
						Command::Transfer { src, dst, amount } => {
							if src == dst || !accounts.contains_key(&dst) || !accounts.contains_key(&src){
								debug!("Cannot transfer to the same account or account(s) do not exist");
							} else {
								let src_balance = accounts.get(&src).cloned();
								if let Some(balance) = src_balance {
									if balance >= amount{
										accounts.get_mut(&src).unwrap().sub(amount);
										accounts.get_mut(&dst).unwrap().add(amount);
										debug!("Transferred {} from {:?} to {:?}", amount, src, dst);
									} else if balance < amount {
										debug!("Insufficient funds in source account {:?}", src);
									} else {
										debug!("Destination account {:?} does not exist", dst);
									}
								} else {
									debug!("Source account {:?} does not exist", src);
								}
							}
						}

						// control messages
						Command::Accept(channel) => {
							trace!(origin = channel.address, "accepted connection");
						},
						_ => { debug!{"Command not found"} }
					}
					node.raft_node.send_heartbeat();
				}
				else {
					if node.raft_node.leader_id == 0 {
						node.raft_node.start_election()
					}
					node.forward_to_leader(cmd);
					sleep(time::Duration::from_millis(1000));
					debug!("Forward to leader")
				}
			}
		});
	}
	
	// connect the network nodes in random order
	let mut rng = thread_rng();
	for src in channels.iter() {
		for dst in channels.iter().choose_multiple(&mut rng, office_count) {
			if src.address == dst.address { continue; }
			src.send(Command::Accept(dst.clone()));
		}
	}
	
	Ok(channels)
}

fn main() -> io::Result<()> {
	use tracing_subscriber::{FmtSubscriber, fmt::time::ChronoLocal};
	let log_path = args().nth(1).unwrap_or("logs".to_string());
	
	// initialize the tracer
	FmtSubscriber::builder()
		.with_timer(ChronoLocal::new("[%Mm %Ss]".to_string()))
		.with_max_level(Level::TRACE)
		.init();
	
	// create and connect a number of offices
	let channels = setup_offices(6, &log_path)?;
	let copy = channels.clone();

	// wait for a short while such that all server are connected
	sleep(Duration::from_millis(1000));

	// activate the thread responsible for the disruption of connections
	thread::spawn(move || daemon(copy, 0.0, 0.0));
	// sample script for your convenience
	script! {
		// tell the macro which collection of channels to use
		use channels;

		
		// customer requests start with the branch office index,
		// followed by the source account name and a list of requests
		[0] "Weber"   => open(), deposit( 50);
		[1] "Redlich" => open(), deposit(100);
		sleep();
		[2] "Redlich" => transfer("Weber", 20);
		sleep();
		[3] "Weber"   => withdraw(60);
		sleep(2);
	}
	
	Ok(())
}
