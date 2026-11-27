//! Process identity for the reaper.
//!
//! Linux reads `/proc/<pid>/stat`. macOS has no `/proc`; it asks `proc_pidinfo`
//! for the BSD process record. A starttime only has to stay stable on one OS,
//! so a reused pid does not compare equal to the process Keel spawned.

#[cfg(target_os = "linux")]
use std::fs;

#[cfg(target_os = "linux")]
pub(crate) fn read_starttime(pid: u32) -> Option<String> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let end = text.rfind(')')?;
    let fields: Vec<&str> = text[end + 1..].split_whitespace().collect();
    // Field 22 of `/proc/pid/stat`. `comm` may contain spaces, so count after `)`.
    fields.get(19).map(|value| (*value).to_string())
}

#[cfg(target_os = "linux")]
pub(crate) fn read_pgrp(pid: u32) -> Option<i64> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let end = text.rfind(')')?;
    let fields: Vec<&str> = text[end + 1..].split_whitespace().collect();
    // Field 5, the third field after `comm`.
    fields.get(2)?.parse().ok()
}

/// The pid still has a `/proc` directory, including a zombie that has not been reaped.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn pid_visible(pid: i64) -> bool {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return false;
    }
    std::path::Path::new(&format!("/proc/{pid}")).exists()
}

/// Sleeping, running, or disk-sleep. A zombie has been signaled and not reaped.
#[cfg(all(test, target_os = "linux"))]
pub(crate) fn still_running(pid: i64) -> bool {
    let Ok(text) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return false;
    };
    let Some(end) = text.rfind(')') else {
        return false;
    };
    let state = text[end + 1..]
        .split_whitespace()
        .next()
        .and_then(|state| state.chars().next());
    matches!(state, Some('S') | Some('R') | Some('D'))
}

/// Gone, or a zombie that still occupies the pid until the parent waits.
#[cfg(target_os = "linux")]
pub(crate) fn leader_dead(pid: i64) -> bool {
    let Ok(text) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return true;
    };
    let Some(end) = text.rfind(')') else {
        return true;
    };
    matches!(text[end + 1..].split_whitespace().next(), Some("Z") | None)
}

// macOS `proc_bsdinfo.pbi_status` uses the xnu `p_stat` values.
#[cfg(target_os = "macos")]
const SRUN: u32 = 2;
#[cfg(target_os = "macos")]
const SSLEEP: u32 = 3;
#[cfg(target_os = "macos")]
const SZOMB: u32 = 5;

/// `arg == 0` does not search the zombie list, so a zombie looks absent.
/// That matches the Linux reaper treating state `Z` as dead.
#[cfg(target_os = "macos")]
fn bsdinfo(pid: u32) -> Option<libc::proc_bsdinfo> {
    if pid == 0 {
        return None;
    }
    let mut info = libc::proc_bsdinfo::default();
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as i32;
    let n = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut libc::proc_bsdinfo as *mut libc::c_void,
            size,
        )
    };
    if n < size {
        return None;
    }
    Some(info)
}

#[cfg(target_os = "macos")]
pub(crate) fn read_starttime(pid: u32) -> Option<String> {
    let info = bsdinfo(pid)?;
    if info.pbi_status == SZOMB {
        return None;
    }
    Some(format!(
        "{}.{:06}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(target_os = "macos")]
pub(crate) fn read_pgrp(pid: u32) -> Option<i64> {
    let info = bsdinfo(pid)?;
    if info.pbi_status == SZOMB || info.pbi_pgid == 0 {
        return None;
    }
    Some(i64::from(info.pbi_pgid))
}

#[cfg(all(test, target_os = "macos"))]
pub(crate) fn pid_visible(pid: i64) -> bool {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return false;
    }
    bsdinfo(pid as u32).is_some()
}

#[cfg(all(test, target_os = "macos"))]
pub(crate) fn still_running(pid: i64) -> bool {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return false;
    }
    let Some(info) = bsdinfo(pid as u32) else {
        return false;
    };
    matches!(info.pbi_status, SRUN | SSLEEP)
}

#[cfg(target_os = "macos")]
pub(crate) fn leader_dead(pid: i64) -> bool {
    if pid <= 0 || pid > i64::from(u32::MAX) {
        return true;
    }
    match bsdinfo(pid as u32) {
        None => true,
        Some(info) => info.pbi_status == SZOMB,
    }
}
