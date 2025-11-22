use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub version: String,
    pub headers: HashMap<String, String>,
    pub body: Option<String>,
}

impl HttpRequest {
    pub fn parse(raw: &str) -> Option<Self> {
        let mut lines = raw.lines();
        let start_line = lines.next()?.split_whitespace().collect::<Vec<_>>();
        if start_line.len() < 3 {
            return None;
        }
        let method = start_line[0].to_string();
        let path = start_line[1].to_string();
        let version = start_line[2].to_string();

        let mut headers = HashMap::new();
        for line in lines.by_ref() {
            if line.is_empty() {
                break;
            }
            if let Some((key, val)) = line.split_once(':') {
                headers.insert(key.trim().to_string(), val.trim().to_string());
            }
        }

        let body = lines.collect::<Vec<_>>().join("\n");
        let body = if body.is_empty() { None } else { Some(body) };

        Some(Self {
            method,
            path,
            version,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::HttpRequest;

    #[test]
    fn parses_basic_request() {
        let raw = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let req = HttpRequest::parse(raw).expect("should parse");
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/health");
    }
}
