use std::path::Path;
use std::time::Duration;

use rusqlite::{Connection, TransactionBehavior};

/// Schema version written by [`migrate`]. Later SQL files append to this list
/// and run in the same transaction.
const MIGRATIONS: &[(i32, &str)] = &[
    (1, include_str!("../migrations/001_init.sql")),
    (2, include_str!("../migrations/002_claim_snapshot.sql")),
];

pub(crate) const USER_VERSION: i32 = 2;

const LATEST: i32 = USER_VERSION;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// Illegal transition. `state` is the row's current state. Maps to HTTP 409.
    Conflict {
        state: String,
        message: String,
    },
    NotFound(String),
    Invalid(String),
    Sqlite(rusqlite::Error),
    Io(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Conflict { state, message } => write!(f, "409 {state}: {message}"),
            Error::NotFound(id) => write!(f, "not found: {id}"),
            Error::Invalid(message) => write!(f, "invalid: {message}"),
            Error::Sqlite(err) => write!(f, "sqlite: {err}"),
            Error::Io(err) => write!(f, "io: {err}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Sqlite(err) => Some(err),
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        Error::Sqlite(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}

/// Open the daemon's one database file and apply pending migrations.
///
/// `foreign_keys` is not persisted by SQLite, so every open turns it on.
/// Otherwise the schema's REFERENCES are not enforced.
///
/// An existing file whose `user_version` is behind this binary is copied
/// aside before `migrate`. The copy uses the daily bak path, so a later
/// startup does not replace that pre-migration file with the new schema.
/// A fresh file is not copied: opening it creates an empty database.
pub fn open(path: &Path) -> Result<Connection> {
    let existed = path.exists();
    let mut conn = Connection::open(path)?;
    configure(&conn)?;
    let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > LATEST || version < 0 {
        return Err(Error::Invalid(format!(
            "database user_version {version} is newer than this binary ({LATEST})"
        )));
    }
    if existed && version < LATEST {
        crate::doctor::backup_if_stale(&conn, path)?;
    }
    migrate(&mut conn)?;
    Ok(conn)
}

#[cfg(test)]
pub(crate) fn open_in_memory() -> Result<Connection> {
    let mut conn = Connection::open_in_memory()?;
    configure(&conn)?;
    migrate(&mut conn)?;
    Ok(conn)
}

fn configure(conn: &Connection) -> Result<()> {
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.busy_timeout(Duration::from_millis(2000))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn migrate(conn: &mut Connection) -> Result<()> {
    let version: i32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version == LATEST {
        return Ok(());
    }
    if version > LATEST || version < 0 {
        return Err(Error::Invalid(format!(
            "database user_version {version} is newer than this binary ({LATEST})"
        )));
    }
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut version = version;
    for (target, sql) in MIGRATIONS {
        if version >= *target {
            continue;
        }
        if version != target - 1 {
            return Err(Error::Invalid(format!(
                "cannot migrate from user_version {version} to {target}"
            )));
        }
        tx.execute_batch(sql)?;
        tx.pragma_update(None, "user_version", *target)?;
        version = *target;
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn file_db_is_wal_version_one_and_reopens() {
        let dir = std::env::temp_dir().join(format!(
            "keel-schema-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("keel.db");
        let conn = open(&path).unwrap();
        let mode: String = conn
            .pragma_query_value(None, "journal_mode", |row| row.get(0))
            .unwrap();
        assert_eq!(mode, "wal");
        let sync: i64 = conn
            .pragma_query_value(None, "synchronous", |row| row.get(0))
            .unwrap();
        assert_eq!(sync, 1);
        let busy: i64 = conn
            .pragma_query_value(None, "busy_timeout", |row| row.get(0))
            .unwrap();
        assert_eq!(busy, 2000);
        let fk: i64 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .unwrap();
        assert_eq!(fk, 1);
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
        let column: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('items') WHERE name = 'claim_snapshot'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(column, 1);
        let index: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_items_state'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(index, "idx_items_state");
        drop(conn);

        let conn = open(&path).unwrap();
        let version: i64 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, 2);
        drop(conn);

        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);
        match open(&path) {
            Err(Error::Invalid(message)) => assert!(message.contains("99"), "{message}"),
            other => panic!("expected invalid user_version, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
