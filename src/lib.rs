//! SQLite schema and item/job transitions.
//! The daemon, CLI, git, and adapters are later slices. Each public transition
//! runs its own immediate transaction on the connection the caller passes.

pub mod db;
pub mod state;

pub use db::{open, open_in_memory, Error, Result};
pub use rusqlite;
