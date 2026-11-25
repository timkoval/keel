//! Binary intake against a scratch home. This must not write the operator's artifacts.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

struct KillLock(PathBuf);

impl Drop for KillLock {
    fn drop(&mut self) {
        let Ok(text) = fs::read_to_string(&self.0) else {
            return;
        };
        let Some(pid) = text.split_whitespace().next() else {
            return;
        };
        let Ok(pid) = pid.parse::<i32>() else {
            return;
        };
        if pid <= 1 {
            return;
        }
        unsafe { kill(pid, 15) };
        for _ in 0..50 {
            if fs::metadata(format!("/proc/{pid}")).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        unsafe { kill(pid, 9) };
    }
}

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}

struct Tree {
    root: PathBuf,
}

impl Drop for Tree {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn scratch() -> Tree {
    let root = std::env::temp_dir().join(format!(
        "keel-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&root).unwrap();
    Tree { root }
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn run(mut cmd: Command) -> Output {
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(cmd.output());
    });
    match rx.recv_timeout(Duration::from_secs(20)) {
        Ok(Ok(output)) => output,
        Ok(Err(err)) => panic!("spawn failed: {err}"),
        Err(_) => panic!("command timed out"),
    }
}

fn keel(bin: &str, tree: &Path, port: u16) -> Command {
    let mut cmd = Command::new(bin);
    let path = std::env::var("PATH").unwrap_or_default();
    cmd.env_remove("KEEL_PORT")
        .env_remove("KEEL_CONFIG")
        .env_remove("KEEL_PROFILE")
        .env("HOME", tree.join("home"))
        .env("XDG_DATA_HOME", tree.join("data"))
        .env("XDG_RUNTIME_DIR", tree.join("run"))
        .env("XDG_CONFIG_HOME", tree.join("config"))
        .env("KEEL_PUBLISH_ROOT", tree.join("pub"))
        .env("KEEL_OPEN_LOG", tree.join("opened"))
        .env("PATH", format!("{}:{path}", tree.join("bin").display()))
        .stdin(Stdio::null());
    let _ = port;
    cmd
}

fn write_config(tree: &Path, port: u16) {
    let dir = tree.join("config/keel");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("config.toml"),
        format!("profile = \"default\"\nport = {port}\n"),
    )
    .unwrap();
}

fn exchange(port: u16, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(req.as_bytes()).unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, text)
}

#[test]
fn url_board_prints_a_url_ending_in_board() {
    let tree = scratch();
    let bin = env!("CARGO_BIN_EXE_keel");
    fs::create_dir_all(tree.root.join("bin")).unwrap();
    fs::write(
        tree.root.join("bin/xdg-open"),
        "#!/bin/sh\nprintf '%s\\n' \"$1\" > \"$KEEL_OPEN_LOG\"\n",
    )
    .unwrap();
    let mut perms = fs::metadata(tree.root.join("bin/xdg-open"))
        .unwrap()
        .permissions();
    #[allow(clippy::permissions_set_readonly)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
        fs::set_permissions(tree.root.join("bin/xdg-open"), perms).unwrap();
    }
    let port = free_port();
    write_config(&tree.root, port);
    let mut url_cmd = keel(bin, &tree.root, port);
    url_cmd.args(["url", "--board"]);
    let output = run(url_cmd);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let printed = String::from_utf8(output.stdout).unwrap();
    let printed = printed.trim();
    assert_eq!(printed, format!("http://127.0.0.1:{port}/board"));
    assert!(printed.ends_with("/board"));

    let mut open_cmd = keel(bin, &tree.root, port);
    open_cmd.args(["open", "--board"]);
    let opened = run(open_cmd);
    assert!(
        opened.status.success(),
        "{}",
        String::from_utf8_lossy(&opened.stderr)
    );
    let opened_out = String::from_utf8(opened.stdout).unwrap();
    assert_eq!(opened_out.trim(), printed);
    let log = fs::read_to_string(tree.root.join("opened")).unwrap();
    assert_eq!(log.trim(), printed);
}

#[test]
fn cli_replaces_a_stale_socket_and_publishes_the_card() {
    let tree = scratch();
    let bin = env!("CARGO_BIN_EXE_keel");
    fs::create_dir_all(tree.root.join("bin")).unwrap();
    fs::write(
        tree.root.join("bin/xdg-open"),
        "#!/bin/sh\nprintf '%s\\n' \"$1\" > \"$KEEL_OPEN_LOG\"\n",
    )
    .unwrap();
    let mut perms = fs::metadata(tree.root.join("bin/xdg-open"))
        .unwrap()
        .permissions();
    #[allow(clippy::permissions_set_readonly)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o755);
        fs::set_permissions(tree.root.join("bin/xdg-open"), perms).unwrap();
    }

    let mut last_stderr = String::new();
    for _ in 0..4 {
        let port = free_port();
        write_config(&tree.root, port);
        let runtime = tree.root.join("run/keel/default");
        fs::create_dir_all(&runtime).unwrap();
        let sock = runtime.join("keel.sock");
        let _ = fs::remove_file(&sock);
        let listener = UnixListener::bind(&sock).unwrap();
        drop(listener);
        let _kill = KillLock(tree.root.join("data/keel/default/keel.lock"));

        let mut add = keel(bin, &tree.root, port);
        add.args(["add", "--body", "hello <b>", "Card title"]);
        let output = run(add);
        if !output.status.success() {
            last_stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            if last_stderr.contains("bind 127.0.0.1") {
                continue;
            }
            panic!("keel add failed: {last_stderr}");
        }
        let stdout = String::from_utf8(output.stdout).unwrap();
        let mut lines = stdout.lines();
        let id = lines.next().unwrap().to_string();
        let url = lines.next().unwrap().to_string();
        assert_eq!(url, format!("http://127.0.0.1:{port}/i/{id}/card"));
        let published = tree.root.join("pub/keel").join(&id).join("card.html");
        let html = fs::read_to_string(&published).unwrap();
        assert!(html.contains("id=\"keel-doc\""));
        assert!(html.contains("hello &lt;b&gt;"));
        assert!(html.contains("--bg: #0B0D10"));
        assert!(!tree
            .root
            .join("home/agent-artifacts/local/artifacts")
            .exists());

        let (health, health_body) = exchange(port, "GET", "/health", "");
        assert_eq!(health, 200);
        assert!(health_body.contains("{\"ok\":true}"));
        assert!(!health_body.contains("inbox"));
        let (denied, _) = exchange(port, "POST", "/v1/items", "{\"title\":\"no\"}");
        assert_eq!(denied, 405);

        let mut show_cmd = keel(bin, &tree.root, port);
        show_cmd.args(["show", &id]);
        let show = run(show_cmd);
        assert!(
            show.status.success(),
            "{}",
            String::from_utf8_lossy(&show.stderr)
        );
        let show_out = String::from_utf8(show.stdout).unwrap();
        let events = show_out.split("events:\n").nth(1).unwrap();
        let event_lines: Vec<&str> = events.lines().filter(|line| !line.is_empty()).collect();
        assert_eq!(event_lines.len(), 1);
        assert!(event_lines[0].contains("create"));
        assert!(event_lines[0].contains("inbox"));

        let mut inbox_cmd = keel(bin, &tree.root, port);
        inbox_cmd.arg("inbox");
        let listed = run(inbox_cmd);
        let listed_out = String::from_utf8(listed.stdout).unwrap();
        assert!(listed_out.contains(&id));
        assert!(listed_out.contains("Card title"));

        let mut status_cmd = keel(bin, &tree.root, port);
        status_cmd.arg("status");
        let status = run(status_cmd);
        let status_out = String::from_utf8(status.stdout).unwrap();
        assert!(status_out.contains("inbox: 1\n"), "{status_out}");
        assert!(
            status_out.contains(&format!("port: {port}\n")),
            "{status_out}"
        );

        let mut url_cmd = keel(bin, &tree.root, port);
        url_cmd.args(["url", &id]);
        let url_out = run(url_cmd);
        assert_eq!(String::from_utf8(url_out.stdout).unwrap().trim(), url);

        let mut open_cmd = keel(bin, &tree.root, port);
        open_cmd.args(["open", &id]);
        let opened = run(open_cmd);
        assert!(
            opened.status.success(),
            "{}",
            String::from_utf8_lossy(&opened.stderr)
        );
        let logged = fs::read_to_string(tree.root.join("opened")).unwrap();
        assert_eq!(logged.trim(), url);

        let mut second_cmd = keel(bin, &tree.root, port);
        second_cmd.arg("serve");
        let second = run(second_cmd);
        assert!(!second.status.success());
        let err = String::from_utf8_lossy(&second.stderr);
        assert!(err.contains("already running"), "{err}");
        return;
    }
    panic!("could not bind a document port: {last_stderr}");
}
