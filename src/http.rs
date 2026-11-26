//! HTTP/1.1 just large enough for one JSON or HTML body. No chunked encoding.

use std::io::{Read, Write};

use crate::db::{Error, Result};

pub(crate) const HEADER_MAX: usize = 16 * 1024;
/// Decoded item bodies stop at 32 KiB. A JSON `\uXXXX` escape is 6 bytes per input byte,
/// plus the title and the rest of the object. A plan document adds its own 256 KiB cap.
pub(crate) const BODY_MAX: usize = 256 * 1024 + 32 * 1024 * 6 + 16 * 1024;

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: Vec<(String, String)>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub extra: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn bytes(status: u16, content_type: &'static str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type,
            extra: Vec::new(),
            body,
        }
    }

    pub fn text(status: u16, content_type: &'static str, body: impl Into<String>) -> Self {
        Self::bytes(status, content_type, body.into().into_bytes())
    }

    pub fn json(status: u16, body: serde_json::Value) -> Self {
        let bytes = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());
        Self::bytes(status, "application/json", bytes)
    }

    pub fn html(body: String) -> Self {
        let mut resp = Self::text(200, "text/html; charset=utf-8", body);
        resp.extra.push((
            "Content-Security-Policy".into(),
            "default-src 'none'; style-src 'unsafe-inline'; img-src data:; script-src 'none'"
                .into(),
        ));
        resp
    }
}

pub fn error_response(status: u16, err: &Error) -> Response {
    Response::json(status, serde_json::json!({ "error": err.to_string() }))
}

pub fn status_code(err: &Error) -> u16 {
    match err {
        Error::Invalid(_) => 400,
        Error::NotFound(_) => 404,
        Error::Conflict { .. } => 409,
        Error::Sqlite(_) | Error::Io(_) => 500,
    }
}

#[cfg(test)]
pub fn exchange<S: Read + Write>(
    stream: &mut S,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<Response> {
    exchange_with(stream, method, path, body, &[])
}

pub fn exchange_with<S: Read + Write>(
    stream: &mut S,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    headers: &[(&str, &str)],
) -> Result<Response> {
    write_request_with(stream, method, path, body, headers)?;
    read_response(stream)
}

#[cfg(test)]
pub fn write_request<S: Write>(
    stream: &mut S,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<()> {
    write_request_with(stream, method, path, body, &[])
}

pub fn write_request_with<S: Write>(
    stream: &mut S,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
    headers: &[(&str, &str)],
) -> Result<()> {
    let body = body.unwrap_or(b"");
    let mut head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/json\r\n",
        body.len()
    );
    for (name, value) in headers {
        if name.is_empty()
            || value.bytes().any(|byte| byte == b'\r' || byte == b'\n')
            || name
                .bytes()
                .any(|byte| byte == b'\r' || byte == b'\n' || byte == b':')
        {
            return Err(Error::Invalid("bad header".into()));
        }
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()?;
    Ok(())
}

pub fn read_request<S: Read>(stream: &mut S) -> Result<Request> {
    let (head, prefetched) = read_head(stream)?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| Error::Invalid("bad request line".into()))?
        .to_string();
    let target = parts
        .next()
        .ok_or_else(|| Error::Invalid("bad request line".into()))?;
    if method.is_empty() || !method.chars().all(|c| c.is_ascii_alphabetic()) {
        return Err(Error::Invalid("bad method".into()));
    }
    let (path, query) = split_target(target);
    let mut content_length = 0usize;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::Invalid("bad header".into()));
        };
        let name = name.trim();
        let value = value.trim();
        if name.is_empty() {
            return Err(Error::Invalid("bad header".into()));
        }
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value
                .parse()
                .map_err(|_| Error::Invalid("bad content-length".into()))?;
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(Error::Invalid("chunked bodies are not accepted".into()));
        }
        headers.push((name.to_string(), value.to_string()));
    }
    if content_length > BODY_MAX {
        return Err(Error::Invalid("request exceeds the socket body cap".into()));
    }
    let body = read_exact_body(stream, prefetched, content_length)?;
    Ok(Request {
        method,
        path,
        query,
        headers,
        body,
    })
}

pub fn write_response<S: Write>(stream: &mut S, response: &Response) -> Result<()> {
    let reason = reason(response.status);
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    for (name, value) in &response.extra {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes())?;
    stream.write_all(&response.body)?;
    stream.flush()?;
    Ok(())
}

pub fn read_response<S: Read>(stream: &mut S) -> Result<Response> {
    let (head, prefetched) = read_head(stream)?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let mut parts = status_line.split_whitespace();
    let _version = parts
        .next()
        .ok_or_else(|| Error::Invalid("bad status line".into()))?;
    let status: u16 = parts
        .next()
        .ok_or_else(|| Error::Invalid("bad status line".into()))?
        .parse()
        .map_err(|_| Error::Invalid("bad status code".into()))?;
    let mut content_length = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::Invalid("bad header".into()));
        };
        if name.eq_ignore_ascii_case("content-length") {
            content_length = Some(
                value
                    .trim()
                    .parse()
                    .map_err(|_| Error::Invalid("bad content-length".into()))?,
            );
        }
    }
    let len =
        content_length.ok_or_else(|| Error::Invalid("response missing content-length".into()))?;
    if len > 1024 * 1024 {
        return Err(Error::Invalid("response is too large".into()));
    }
    let body = read_exact_body(stream, prefetched, len)?;
    Ok(Response {
        status,
        content_type: "application/octet-stream",
        extra: Vec::new(),
        body,
    })
}

fn read_head<S: Read>(stream: &mut S) -> Result<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        if buf.len() > HEADER_MAX {
            return Err(Error::Invalid("headers too large".into()));
        }
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            if buf.is_empty() {
                return Err(Error::Invalid("eof".into()));
            }
            return Err(Error::Invalid("truncated headers".into()));
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_sep(&buf) {
            let head = std::str::from_utf8(&buf[..pos])
                .map_err(|_| Error::Invalid("headers are not utf-8".into()))?;
            let rest = buf[pos + 4..].to_vec();
            return Ok((head.to_string(), rest));
        }
    }
}

fn read_exact_body<S: Read>(
    stream: &mut S,
    mut prefetched: Vec<u8>,
    len: usize,
) -> Result<Vec<u8>> {
    if prefetched.len() >= len {
        prefetched.truncate(len);
        return Ok(prefetched);
    }
    let mut rest = vec![0u8; len - prefetched.len()];
    stream.read_exact(&mut rest)?;
    prefetched.extend_from_slice(&rest);
    Ok(prefetched)
}

fn find_sep(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn split_target(target: &str) -> (String, Vec<(String, String)>) {
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let query = if raw_query.is_empty() {
        Vec::new()
    } else {
        raw_query
            .split('&')
            .filter(|part| !part.is_empty())
            .map(|part| {
                let (key, value) = part.split_once('=').unwrap_or((part, ""));
                (percent_decode(key), percent_decode(value))
            })
            .collect()
    };
    (path.to_string(), query)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3]) {
                if let Ok(byte) = u8::from_str_radix(hex, 16) {
                    out.push(byte);
                    i += 3;
                    continue;
                }
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn query<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.as_str())
}

pub fn header<'a>(pairs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    pairs
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        _ => "Error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn request_roundtrip_preserves_body_and_query() {
        let mut buf = Vec::new();
        write_request(
            &mut buf,
            "POST",
            "/v1/items?state=a+b%20c",
            Some(br#"{"title":"a"}"#),
        )
        .unwrap();
        let mut cur = Cursor::new(buf);
        let req = read_request(&mut cur).unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/v1/items");
        assert_eq!(query(&req.query, "state"), Some("a+b c"));
        assert_eq!(req.body, br#"{"title":"a"}"#);
        assert!(req.headers.iter().any(|(name, _)| name == "Host"));
    }

    #[test]
    fn surface_header_roundtrip() {
        let mut buf = Vec::new();
        write_request_with(
            &mut buf,
            "POST",
            "/v1/items/k_1/release",
            Some(b"{}"),
            &[("x-keel-surface", "cli")],
        )
        .unwrap();
        let mut cur = Cursor::new(buf);
        let req = read_request(&mut cur).unwrap();
        assert_eq!(header(&req.headers, "X-Keel-Surface"), Some("cli"));
        assert_eq!(req.body, b"{}");
    }

    #[test]
    fn socket_cap_accepts_an_escaped_32kib_body() {
        let body = "\u{1}".repeat(32 * 1024);
        let payload = serde_json::to_vec(&serde_json::json!({"title": "t", "body": body})).unwrap();
        assert!(payload.len() > 64 * 1024, "{}", payload.len());
        assert!(payload.len() <= BODY_MAX, "{}", payload.len());
        let mut buf = Vec::new();
        write_request(&mut buf, "POST", "/v1/items", Some(&payload)).unwrap();
        let mut cur = Cursor::new(buf);
        let req = read_request(&mut cur).unwrap();
        assert_eq!(req.body, payload);
    }

    #[test]
    fn response_roundtrip() {
        let resp = Response::json(200, serde_json::json!({"ok": true}));
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).unwrap();
        let mut cur = Cursor::new(buf);
        let got = read_response(&mut cur).unwrap();
        assert_eq!(got.status, 200);
        assert_eq!(got.body, br#"{"ok":true}"#);
    }
}
