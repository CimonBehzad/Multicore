use std::sync::{Mutex,Arc};
use std::thread;

extern crate rand;
use rand::Rng;

fn main() {
	let start_balance = 1000;
	let num_transactions = 1000;
	let num_threads = 10;
	let num_accounts = 1000;
	let max_amount = 20;
	let mut threads = vec![];
	let mut accounts = vec![];

	for _ in 0 .. num_accounts {
		accounts.push(Arc::new(Mutex::new(start_balance)));
	}

	let acc = Arc::new(Mutex::new(accounts));

	for _ in 0 .. num_threads {
		let a = acc.clone();
		let h = thread::spawn(move || {
			let mut rng = rand::thread_rng();

			for _ in 0 .. num_transactions / num_threads {
				let i0 : usize = rng.gen();
				let i = i0 % num_accounts;

				let j0 : usize = rng.gen();
				let j = j0 % num_accounts;

				let amount0 : i32 = rng.gen();
				let amount = amount0 % max_amount;

				println!("i = {}", i);
				//println!("j = {}", j);
				//println!("amount = {}", amount);
				let array = a.lock().unwrap();

				{
					let mut from = array[i].lock().unwrap();
					*from -= amount;
				}

				{
					let mut to = array[j].lock().unwrap();
					*to += amount;
				}

			}
		});
		threads.push(h);
	}

	for h in threads {
		h.join().unwrap();
	}

	let mut sum = 0;
	let array = acc.lock().unwrap();
	for i in 0 .. num_accounts {
		let x = array[i].lock().unwrap();
		sum += *x;
	}

	if (sum == (num_accounts as i32) * start_balance) {
		println!("PASS");
	} else {
		println!("FAIL");
	}
	
	
}
