# Welcome

Welcome to Mainmatter's **Async Rust** workshop!

You have written async Rust before. You know what `async fn` and `.await` do, you have used Tokio, and
you have at some point stared at a program that was not doing what you told it to. This course is
about the part that comes after the syntax: who owns your state, what happens when a future is
dropped halfway through, and what your server does when it is asked for more than it can deliver.

Everything runs on [Tokio](https://tokio.rs). Futures, `poll` and `Pin` get one chapter of
explanation and no exercises, because this is a workshop about using async rather than implementing
it.

## What you will build

You will take **`minidb`**, a small in-memory key-value store, and turn it into a networked one:
concurrent, cancellable, back-pressured, shut down cleanly, and durable across a restart.

By the end it speaks a line protocol over TCP, one task owns the data and everything else asks it
nicely, a slow client cannot starve a fast one, an overloaded server says so instead of falling over,
and a restart picks up where the last one left off.

Each exercise is a complete, standalone copy of the library at that point in its evolution. You never
have to carry a broken state forward.

## Methodology

This is a hands-on workshop. Expect to spend at least half the day writing code.

Exercises are test-driven: each one ships a set of tests that describe the behaviour you are supposed
to build, and your job is to make them pass. Some exercises hand you a `todo!()` to replace. In others
there is nothing to replace: the text at the top of the file tells you what to change, and the tests
show you its shape.

> ⚠️ **Do not modify the tests.** They are the specification. Change the code under test, not the test.

If you get stuck for more than ten minutes, grab a trainer. We are here to help. You can also find
solutions to all exercises in the `solutions` branch of this repository.

## Setup

You need a recent stable Rust toolchain:

```bash
rustup update stable
```

Clone the repository and create a branch to work on:

```bash
git clone https://github.com/mainmatter/async-rust-workshop
cd async-rust-workshop
git checkout -b my-solutions
```

Then install the workshop runner, the tool that walks you through the exercises:

```bash
cargo install --locked workshop-runner
```

## The workflow

From the root of the repository, run:

```bash
wr
```

`wr` finds the first exercise you have not solved yet, compiles it, runs its tests, and either
congratulates you or shows you what went wrong. It will not let you move on until the current exercise
passes. Solve it, run `wr` again, and it opens the next one.

From chapter 3 onwards, every exercise that has a server in it also builds two binaries, so you can
talk to the thing you have just written. The two that introduce a chapter rather than change the
server, `03_server/00_intro` and `04_state/00_intro`, have no binaries and nothing to run. From
inside the exercise's directory:

```bash
cargo run                 # your server, on port 7878
cargo run --bin client    # in a second terminal
```

The client sends one line per request and prints the reply. `nc localhost 7878` does the same job if
you would rather not have a third terminal running `cargo`.

Building the whole workspace at once warns about several binaries sharing the names `server` and
`client`. That is expected: every exercise carries its own pair.

That is the whole loop. Let's make sure it works.
