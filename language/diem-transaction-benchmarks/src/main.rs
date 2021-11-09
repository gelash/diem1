use diem_transaction_benchmarks::transactions::TransactionBencher;
use language_e2e_tests::account_universe::P2PTransferGen;
use proptest::prelude::*;

fn main() {
    let bencher = TransactionBencher::new(any_with::<P2PTransferGen>((1_000, 1_000_000)));
    let measurements = bencher.manual_parallel();
    println!("{:?}", measurements);
}
