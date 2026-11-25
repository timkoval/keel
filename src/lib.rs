//! SQLite schema, item and job transitions, and the local daemon.
//! The CLI talks to the daemon over a Unix socket. It does not open the database.
//! The document port is GET-only.

pub(crate) mod adapters;
pub(crate) mod claim;
pub mod cli;
pub(crate) mod config;
pub(crate) mod daemon;
pub mod db;
pub(crate) mod git;
pub(crate) mod http;
pub(crate) mod proc;
pub(crate) mod render;
pub mod state;
pub(crate) mod tui;

pub use db::{open, Error, Result};
