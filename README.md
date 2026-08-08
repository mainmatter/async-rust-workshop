# Async Rust

> Concurrency you can reason about

Async Rust is easy to start and hard to finish. The syntax is a day's work; the rest is knowing who
owns your state, what happens when a future is dropped halfway through, and what your server does
when it is asked for more than it can deliver. This workshop is about the rest.

You will work through a series of test-driven exercises, turning `minidb`, a small embedded key-value
store, into a networked one: concurrent, cancellable, back-pressured, shut down cleanly, and durable
across a restart. Everything runs on [Tokio](https://tokio.rs).

This workshop is designed for people who have written some async Rust and want to stop guessing.

> [!NOTE]
> This workshop has been written by [Mainmatter](https://mainmatter.com/rust-consulting/).\
> It's one of the trainings in [our portfolio of Rust workshops](https://mainmatter.com/services/workshops/rust/).\
> Check out our [landing page](https://mainmatter.com/rust-consulting/) if you're looking for Rust consulting or training!

## Getting started

Open the companion book for this course in your browser. Follow the instructions there to get started.

## Requirements

- **Rust** (follow instructions [here](https://www.rust-lang.org/tools/install)).\
  If Rust is already installed on your system, make sure you are running on the latest compiler version (`cargo --version`).\
  If not, update using `rustup update` (or another appropriate command depending on how you installed Rust on your system).
- _(Optional)_ An IDE with Rust autocompletion support.
  We recommend one of the following:
  - [RustRover](https://www.jetbrains.com/rust/);
  - [Visual Studio Code](https://code.visualstudio.com) with the [`rust-analyzer`](https://marketplace.visualstudio.com/items?itemName=matklad.rust-analyzer) extension.

## Solutions

You can find the solutions to the exercises in the `solutions` branch of this repository.

## References

Throughout the workshop, the following resources might turn out to be useful:

- [Tokio documentation](https://docs.rs/tokio/) and the [Tokio tutorial](https://tokio.rs/tokio/tutorial)
- [Asynchronous Programming in Rust](https://rust-lang.github.io/async-book/)
- [Rust documentation](https://doc.rust-lang.org/std/) (you can also open the documentation offline with `rustup doc`!)
- [Alice Ryhl on actors with Tokio](https://ryhl.io/blog/actors-with-tokio/)

# License

Copyright © 2026- Mainmatter GmbH (https://mainmatter.com), released under the
[Creative Commons Attribution-NonCommercial 4.0 International license](https://creativecommons.org/licenses/by-nc/4.0/).
