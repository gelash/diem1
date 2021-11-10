use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use num_cpus;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));

    let writes = [0.0, 0.25, 0.5, 0.75, 1.0];
    let acts = [100];
    let txns = [1000, 10000];

    let mut measurements = Vec::new();

    for block_size in txns {
        for num_accounts in acts {
            for write_rate in writes {
                let mut times = bencher.manual_parallel(num_accounts, block_size, write_rate);
                times.sort();
                measurements.push(times);
            }
        }
    }

    println!("CPUS = {}", num_cpus::get());

    let mut i = 0;
    for block_size in txns {
        for num_accounts in acts {
            for write_rate in writes {
                println!(
                    "keep write rate = {:?}, num act = {}, num txns in block = {}",
                    write_rate, num_accounts, block_size
                );
                println!("times: par exec, inference = {:?}", measurements[i]);
                i = i + 1;
            }
        }
    }
}
