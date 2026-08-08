//! # Exercise
//!
//! Make the test at the bottom of this file pass.
//!
//! It is not a hard one. The point is to check that your toolchain works, that Tokio builds on your
//! machine, and that you know the loop: run `wr`, read the failure, fix the code, run `wr` again.
//!
//! Some exercises hand you a `todo!()` to replace. In others there is nothing to replace: the text
//! above tells you what to add, and the tests show you its shape. You can also run the tests
//! directly with `cargo test` from this exercise's directory, which is what `wr` does for you.
//!
//! From chapter 3 onwards each exercise also builds two binaries, so you can talk to what you have
//! written: `cargo run` starts the server and `cargo run --bin client` opens a client against it.
//!
//! The tests are the specification: read them first, and never change them.

/// Reports whether you are ready to start.
pub async fn ready() -> bool {
    todo!("this one really is a one-liner")
}

#[cfg(test)]
mod tests {
    use crate::ready;

    #[tokio::test]
    async fn starting_block() {
        assert!(
            ready().await,
            "Make `ready` return `true` and run `wr` again."
        );
    }
}
