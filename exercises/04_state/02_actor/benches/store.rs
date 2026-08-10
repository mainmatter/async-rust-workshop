//! Two designs, one workload: many tasks hammering a single store.
//!
//! Run it with `cargo bench`. It is not part of the exercise and `wr` never runs it.
//!
//! The store, and for the actor the task that owns it, are built once and live outside the timed
//! block, so what is measured is `TASKS` tasks making `REQUESTS` requests each against a store that
//! is already running. Those two constants are the knobs: raise `TASKS` for more contention, raise
//! `REQUESTS` for a longer run per task, and watch which design the answer moves towards.

use std::sync::Arc;

use criterion::{Criterion, criterion_group, criterion_main};
use state_actor::{
    Bucket, Key, Store, Value,
    actor::StoreHandle,
    protocol::{Request, Response},
    server::apply,
};
use tokio::{runtime::Runtime, sync::Mutex};

const TASKS: usize = 64;
const REQUESTS: usize = 16;

fn contended(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = c.benchmark_group("one store, many tasks");

    group.bench_function("mutex", |b| {
        let store = Arc::new(Mutex::new(Store::new()));

        b.to_async(&runtime).iter(|| {
            let store = Arc::clone(&store);

            async move {
                let tasks = (0..TASKS)
                    .map(|task| {
                        let store = Arc::clone(&store);
                        tokio::spawn(async move {
                            for i in 0..REQUESTS {
                                let mut store = store.lock().await;
                                apply(request(task, i), &mut store);
                            }
                        })
                    })
                    .collect::<Vec<_>>();

                for task in tasks {
                    task.await.unwrap();
                }
            }
        })
    });

    group.bench_function("actor", |b| {
        let store = runtime.block_on(async { StoreHandle::spawn(Store::new()) });

        b.to_async(&runtime).iter(|| {
            let store = store.clone();

            async move {
                let tasks = (0..TASKS)
                    .map(|task| {
                        let store = store.clone();
                        tokio::spawn(async move {
                            for i in 0..REQUESTS {
                                let answer = store.apply(request(task, i)).await;
                                debug_assert_eq!(answer, Response::Ok);
                            }
                        })
                    })
                    .collect::<Vec<_>>();

                for task in tasks {
                    task.await.unwrap();
                }
            }
        })
    });

    group.finish();
}

fn request(task: usize, i: usize) -> Request {
    Request::Set {
        bucket: Bucket::parse("users").unwrap(),
        key: Key::parse(&format!("user-{task}-{i}")).unwrap(),
        value: Value::parse("hello").unwrap(),
    }
}

criterion_group!(benches, contended);
criterion_main!(benches);
