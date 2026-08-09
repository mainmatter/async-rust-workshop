# Summary

[Welcome](00_intro/00_welcome.md)

- [What the runtime actually does](01_runtime/00_intro.md)

- [Tasks](02_tasks/00_intro.md)
  - [Fetching things at the same time](02_tasks/01_spawn.md)
  - [Work that will not yield](02_tasks/02_blocking.md)

- [A server](03_server/00_intro.md)
  - [The accept loop](03_server/01_accept.md)
  - [One task per connection](03_server/02_concurrent.md)

- [Who owns the state](04_state/00_intro.md)
  - [A store every connection can reach](04_state/01_mutex.md)
  - [An actor](04_state/02_actor.md)

- [Cancellation](05_cancellation/00_intro.md)
  - [An idle timeout](05_cancellation/01_select.md)
  - [Cancel safety](05_cancellation/02_cancel_safe.md)
  - [Bounding the work](05_cancellation/03_timeout.md)

- [Backpressure](06_backpressure/00_intro.md)
  - [Refusing rather than queueing](06_backpressure/01_bounded.md)
  - [Admission control](06_backpressure/02_limits.md)

- [Shutdown and supervision](07_shutdown/00_intro.md)
  - [Draining](07_shutdown/01_graceful.md)
  - [Supervision](07_shutdown/02_supervision.md)

- [Testing async code](08_testing/00_intro.md)
  - [Retrying, and testing that it waited](08_testing/01_time.md)
  - [Seeing inside](08_testing/02_tracing.md)

- [Surviving a restart](09_wal/00_intro.md)
  - [Write ahead](09_wal/01_append.md)
  - [Group commit](09_wal/02_group_commit.md)
  - [Replay](09_wal/03_replay.md)
