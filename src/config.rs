//! Profile paths and the document port. The CLI and the daemon share one resolution
//! so a command and the `keel serve` it starts cannot point at different files.

use std::env;
use std::fs;
use std::path::PathBuf;

use crate::db::{Error, Result};

const DEFAULT_PROFILE: &str = "default";
const DEFAULT_PORT: u16 = 7431;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub profile: String,
    pub port: u16,
    pub data_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub socket_path: PathBuf,
    pub lock_path: PathBuf,
    pub db_path: PathBuf,
    pub docs_dir: PathBuf,
    pub publish_root: PathBuf,
}

impl Config {
    pub fn card_url(&self, id: &str) -> String {
        format!("http://127.0.0.1:{}/i/{id}/card", self.port)
    }

    pub fn origin(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

/// Values that override `config.toml`. Empty optional fields mean "not set".
struct Spec {
    home: PathBuf,
    xdg_data_home: Option<PathBuf>,
    xdg_runtime_dir: Option<PathBuf>,
    xdg_config_home: Option<PathBuf>,
    /// `KEEL_CONFIG`. When set, the file must exist.
    config_path: Option<PathBuf>,
    profile: Option<String>,
    port: Option<String>,
    publish_root: Option<PathBuf>,
}

pub fn load() -> Result<Config> {
    let home = env_path("HOME").ok_or_else(|| Error::Invalid("HOME is not set".into()))?;
    let spec = Spec {
        home,
        xdg_data_home: env_path("XDG_DATA_HOME"),
        xdg_runtime_dir: env_path("XDG_RUNTIME_DIR"),
        xdg_config_home: env_path("XDG_CONFIG_HOME"),
        config_path: env_path("KEEL_CONFIG"),
        profile: env_string("KEEL_PROFILE"),
        port: env_string("KEEL_PORT"),
        publish_root: env_path("KEEL_PUBLISH_ROOT"),
    };
    resolve(spec)
}

fn resolve(spec: Spec) -> Result<Config> {
    let file_path = match &spec.config_path {
        Some(path) => Some(path.clone()),
        None => Some(default_config_path(&spec)),
    };
    let mut profile = DEFAULT_PROFILE.to_string();
    let mut port = DEFAULT_PORT;
    let mut publish_root = spec.home.join("agent-artifacts/local/artifacts");
    if let Some(path) = file_path {
        let explicit = spec.config_path.is_some();
        match fs::read_to_string(&path) {
            Ok(text) => {
                let keys = parse_config(&text)
                    .map_err(|err| Error::Invalid(format!("{}: {err}", path.display())))?;
                if let Some(value) = keys.get("profile") {
                    profile = value.clone();
                }
                if let Some(value) = keys.get("port") {
                    port = parse_port(value)?;
                }
                if let Some(value) = keys.get("publish_root") {
                    publish_root = PathBuf::from(value);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !explicit => {}
            Err(err) => return Err(err.into()),
        }
    }
    if let Some(value) = spec.profile.as_deref() {
        profile = value.to_string();
    }
    if let Some(value) = spec.port.as_deref() {
        port = parse_port(value)?;
    }
    if let Some(path) = spec.publish_root {
        publish_root = path;
    }
    validate_profile(&profile)?;
    let data_root = spec
        .xdg_data_home
        .unwrap_or_else(|| spec.home.join(".local/share"));
    let data_dir = data_root.join("keel").join(&profile);
    // No runtime dir: the socket sits next to the database, still mode 0600.
    let runtime_dir = match spec.xdg_runtime_dir {
        Some(dir) => dir.join("keel").join(&profile),
        None => data_dir.clone(),
    };
    Ok(Config {
        profile,
        port,
        socket_path: runtime_dir.join("keel.sock"),
        lock_path: runtime_dir.join("keel.lock"),
        db_path: data_dir.join("keel.db"),
        docs_dir: data_dir.join("docs"),
        data_dir,
        runtime_dir,
        publish_root,
    })
}

fn default_config_path(spec: &Spec) -> PathBuf {
    let base = spec
        .xdg_config_home
        .clone()
        .unwrap_or_else(|| spec.home.join(".config"));
    base.join("keel").join("config.toml")
}

fn validate_profile(profile: &str) -> Result<()> {
    let ok = !profile.is_empty()
        && profile.len() <= 64
        && profile
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(Error::Invalid(format!(
            "profile {profile:?} must be ASCII letters, digits, '_' or '-'"
        )));
    }
    Ok(())
}

fn parse_port(value: &str) -> Result<u16> {
    let port: u16 = value
        .trim()
        .parse()
        .map_err(|_| Error::Invalid(format!("port {value:?} is not a number")))?;
    if port == 0 {
        return Err(Error::Invalid("port 0 is not a fixed document port".into()));
    }
    Ok(port)
}

/// Flat `key = value` pairs. Enough for profile, port, and publish_root.
fn parse_config(text: &str) -> Result<std::collections::BTreeMap<String, String>> {
    let mut map = std::collections::BTreeMap::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = strip_comment(raw).trim().to_string();
        if line.is_empty() || line.starts_with('[') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(Error::Invalid(format!(
                "line {}: expected key = value",
                idx + 1
            )));
        };
        let key = key.trim();
        if key.is_empty() || key.chars().any(|c| c.is_whitespace()) {
            return Err(Error::Invalid(format!("line {}: bad key", idx + 1)));
        }
        let value = parse_scalar(value.trim())
            .map_err(|err| Error::Invalid(format!("line {}: {err}", idx + 1)))?;
        map.insert(key.to_string(), value);
    }
    Ok(map)
}

fn strip_comment(raw: &str) -> &str {
    let mut quote: Option<char> = None;
    for (idx, ch) in raw.char_indices() {
        match (quote, ch) {
            (None, '#') => return &raw[..idx],
            (None, '"' | '\'') => quote = Some(ch),
            (Some(q), c) if c == q => quote = None,
            _ => {}
        }
    }
    raw
}

fn parse_scalar(value: &str) -> Result<String> {
    if value.is_empty() {
        return Err(Error::Invalid("missing value".into()));
    }
    let bytes = value.as_bytes();
    if (bytes[0] == b'"' || bytes[0] == b'\'')
        && value.ends_with(bytes[0] as char)
        && value.len() >= 2
    {
        return Ok(value[1..value.len() - 1].to_string());
    }
    if value.chars().any(|c| c.is_whitespace()) {
        return Err(Error::Invalid(format!("unquoted value {value:?}")));
    }
    Ok(value.to_string())
}

fn env_string(key: &str) -> Option<String> {
    env::var(key).ok().filter(|value| !value.is_empty())
}

fn env_path(key: &str) -> Option<PathBuf> {
    env::var_os(key)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[cfg(test)]
pub(crate) fn isolated(root: &std::path::Path, port: u16) -> Config {
    let data_dir = root.join("data");
    let runtime_dir = root.join("run");
    Config {
        profile: "default".into(),
        port,
        data_dir: data_dir.clone(),
        runtime_dir: runtime_dir.clone(),
        socket_path: runtime_dir.join("keel.sock"),
        lock_path: runtime_dir.join("keel.lock"),
        db_path: data_dir.join("keel.db"),
        docs_dir: data_dir.join("docs"),
        publish_root: root.join("artifacts"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(home: &str) -> Spec {
        Spec {
            home: PathBuf::from(home),
            xdg_data_home: None,
            xdg_runtime_dir: None,
            xdg_config_home: None,
            config_path: None,
            profile: None,
            port: None,
            publish_root: None,
        }
    }

    #[test]
    fn default_profile_paths_and_runtime_fallback() {
        let cfg = resolve(spec("/home/op")).unwrap();
        assert_eq!(cfg.profile, "default");
        assert_eq!(cfg.port, 7431);
        assert_eq!(
            cfg.data_dir,
            PathBuf::from("/home/op/.local/share/keel/default")
        );
        assert_eq!(
            cfg.db_path,
            PathBuf::from("/home/op/.local/share/keel/default/keel.db")
        );
        assert_eq!(
            cfg.socket_path,
            PathBuf::from("/home/op/.local/share/keel/default/keel.sock")
        );
        assert_eq!(cfg.lock_path, cfg.data_dir.join("keel.lock"));
        assert_eq!(
            cfg.publish_root,
            PathBuf::from("/home/op/agent-artifacts/local/artifacts")
        );
    }

    #[test]
    fn xdg_dirs_split_socket_from_data() {
        let mut spec = spec("/home/op");
        spec.xdg_data_home = Some(PathBuf::from("/data"));
        spec.xdg_runtime_dir = Some(PathBuf::from("/run/user/1"));
        spec.profile = Some("work".into());
        spec.port = Some("8000".into());
        spec.publish_root = Some(PathBuf::from("/tmp/pub"));
        let cfg = resolve(spec).unwrap();
        assert_eq!(cfg.data_dir, PathBuf::from("/data/keel/work"));
        assert_eq!(
            cfg.socket_path,
            PathBuf::from("/run/user/1/keel/work/keel.sock")
        );
        assert_eq!(
            cfg.lock_path,
            PathBuf::from("/run/user/1/keel/work/keel.lock")
        );
        assert_eq!(cfg.port, 8000);
        assert_eq!(cfg.publish_root, PathBuf::from("/tmp/pub"));
        assert_eq!(cfg.card_url("k_abc"), "http://127.0.0.1:8000/i/k_abc/card");
    }

    #[test]
    fn config_file_then_env_override() {
        let dir = std::env::temp_dir().join(format!(
            "keel-cfg-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(dir.join("cfg/keel")).unwrap();
        fs::write(
            dir.join("cfg/keel/config.toml"),
            "profile = \"lab\"\nport = 7440\npublish_root = \"/cfg/pub\"\n# comment\n",
        )
        .unwrap();
        let mut from_file = spec("/home/op");
        from_file.xdg_config_home = Some(dir.join("cfg"));
        let cfg = resolve(from_file).unwrap();
        assert_eq!(cfg.profile, "lab");
        assert_eq!(cfg.port, 7440);
        assert_eq!(cfg.publish_root, PathBuf::from("/cfg/pub"));

        let mut from_env = spec("/home/op");
        from_env.xdg_config_home = Some(dir.join("cfg"));
        from_env.profile = Some("other".into());
        from_env.port = Some("7441".into());
        from_env.publish_root = Some(PathBuf::from("/env/pub"));
        let cfg = resolve(from_env).unwrap();
        assert_eq!(cfg.profile, "other");
        assert_eq!(cfg.port, 7441);
        assert_eq!(cfg.publish_root, PathBuf::from("/env/pub"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn rejects_bad_profile_and_port() {
        let mut bad_profile = spec("/home/op");
        bad_profile.profile = Some("../x".into());
        assert!(resolve(bad_profile).is_err());
        let mut bad_port = spec("/home/op");
        bad_port.port = Some("0".into());
        assert!(resolve(bad_port).is_err());
    }
}
