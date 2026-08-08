//! Two designs, one workload: many tasks hammering a single store.
//!
//! Run it with `cargo bench`. It is not part of the exercise and `wr` never runs it.

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
        b.to_async(&runtime).iter(|| async {
            let store = Arc::new(Mutex::new(Store::new()));

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
        })
    });

    group.bench_function("actor", |b| {
        b.to_async(&runtime).iter(|| async {
            let store = StoreHandle::spawn(Store::new());

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
