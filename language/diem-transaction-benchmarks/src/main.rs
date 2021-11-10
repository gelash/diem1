use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));

    let writes = [0.0, 0.25, 0.5, 0.75, 1.0];
    let acts = [100];
    let txns = [1000, 10000];

    let mut measurements = Vec::new();

    for write_rate in writes {
        for num_accounts in acts {
            for block_size in txns {
                let mut times = bencher.manual_parallel(num_accounts, block_size, write_rate);
                times.sort();
                measurements.push(times);
            }
        }
    }

    let mut i = 0;
    for write_rate in writes {
        for num_accounts in acts {
            for block_size in txns {
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
