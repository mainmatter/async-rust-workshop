# Backpressure

A queue with no limit is not a queue, it is a memory leak with good manners.

`minidb`'s store task can apply so many requests per second. If clients ask for more than that, the
extra has to go somewhere, and an unbounded `mpsc` channel says "in RAM, all of it". The service
looks fine for a while: latency climbs, the queue grows, memory grows, and then either the OOM
killer arrives or the queue drains hours of work nobody is waiting for any more.

**Backpressure** is the opposite arrangement: when a stage cannot keep up, the pressure travels
back up the pipeline to whoever is producing, and the producer slows down or is told no.

## Where it comes from in Tokio

Bounded channels give it to you almost for free:

```rust
let (commands, inbox) = mpsc::channel(MAILBOX);
```

`Sender::send` on a full channel waits until there is room. The task calling it makes no progress
until the store task has caught up, which is the pressure, travelling backwards. That connection
stops reading from its socket, its TCP receive window fills, and the client's own `write` starts to
block. The chain reaches all the way to the other machine without anybody writing a line of code to
make it happen.

Every real system has this property somewhere, and the question is only whether you chose where.

## Waiting, refusing, or dropping

Three responses to "full", and they are not interchangeable:

- **Wait** (`send().await`). Correct when the producer has nowhere better to be and the work must
  happen. Latency grows, nothing is lost.
- **Refuse** (`try_send`, then tell the client). Correct when the request has a deadline anyway. This
  is **load shedding**: latency stays bounded for the requests you do accept, and the client finds
  out immediately.
- **Drop** (oldest, newest, or by priority). Correct for data where fresh matters more than complete:
  metrics, sensor readings, progress updates.

The first exercise is refusing. The second is a different lever entirely: limiting how many
connections exist at all, so the queue is not the only thing standing between a burst and the heap.

## The number

`MAILBOX = 32` is a guess, and so is every other capacity in every system you have worked on. What
makes it a defensible guess is knowing what it means: the queue length is latency, at the rate the
consumer drains it. Thirty-two requests at a millisecond each is thirty-two milliseconds of queueing
delay when full, which is a sentence you can check against your latency budget.

The queue depth is also the single most useful thing to put on a dashboard. A queue that is
occasionally deep is absorbing bursts, which is its job. A queue that is permanently deep is a
consumer that is too slow, and no capacity will fix it.
