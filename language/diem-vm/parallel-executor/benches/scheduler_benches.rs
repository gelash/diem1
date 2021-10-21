// Copyright (c) The Diem Core Contributors
// SPDX-License-Identifier: Apache-2.0

use criterion::{criterion_group, criterion_main, Criterion};
use diem_parallel_executor::proptest_types::bencher::Bencher;
use proptest::prelude::*;

//
// Transaction benchmarks
//

fn random_benches(c: &mut Criterion) {
    c.bench_function("random_benches", |b| {
        let bencher = Bencher::<[u8; 1], [u8; 1]>::new(10000, 2, 0.5, 0.0); // number of txns, number of accounts, percentage of unestimated write, percentage of unestimated read
        bencher.bench(&any::<[u8; 1]>(), b)
    });
}

criterion_group!(benches, random_benches);

criterion_main!(benches);
