use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use num_cpus;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));

    let writes = [0.0, 0.6, 0.95, 1.0];
    let reads = [0.0, 1.0];
    let acts = [100];
    let txns = [1000, 10000];
    let num_warmups = 2;
    let num_runs = 7;

    let mut measurements = Vec::new();

    for block_size in txns {
        for read_rate in reads {
            for num_accounts in acts {
                for write_rate in writes {
                    let mut times = bencher.manual_parallel(
                        num_accounts,
                        block_size,
                        write_rate,
                        read_rate,
                        num_warmups,
                        num_runs,
                    );
                    times.sort();
                    measurements.push(times);
                }
            }
        }
    }

    println!("CPUS = {}", num_cpus::get());

    let mut i = 0;
    for block_size in txns {
        for read_rate in reads {
            for num_accounts in acts {
                for write_rate in writes {
                    println!(
                        "PARAMS: (keep ratio) writes: {:?}, reads: {:?}, \
                         num accounts = {}, num txns in block = {}",
                        write_rate, read_rate, num_accounts, block_size
                    );
                    println!(
                        "TIMES: (parallel exec, r/w inference) = {:?}",
                        measurements[i]
                    );
                    i = i + 1;
                }
                println!();
            }
        }
    }
}
