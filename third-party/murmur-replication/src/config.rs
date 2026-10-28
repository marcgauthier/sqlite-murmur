use crate::*;
use rusqlite::Connection;
use std::io::BufReader;

#[derive(Clone)]
pub(crate) struct Credentials {
    pub chain: Vec<Vec<u8>>,
    pub private_key: zeroize::Zeroizing<Vec<u8>>,
    pub roots: Vec<Vec<u8>>,
}
#[derive(Clone)]
pub(crate) struct Config {
    pub group: NodeId,
    pub node: NodeId,
    pub site: NodeId,
    pub bind: SocketAddr,
    pub advertise: Option<SocketAddr>,
    pub credentials: Credentials,
    pub allowed_peers: BTreeSet<NodeId>,
    pub allowed_networks: Vec<ipnet::IpNet>,
    pub bootstrap: Vec<SocketAddr>,
    pub excluded: BTreeSet<NodeId>,
    pub incarnation: u64,
    pub sessions: usize,
    pub queue_bytes: usize,
    pub batch_bytes: usize,
    pub max_batch_bytes: usize,
    pub staging_bytes: usize,
    pub staging_timeout_ms: u64,
    pub poll_ms: u64,
    pub anti_entropy_ms: u64,
}
pub(crate) fn default_value(name: &str) -> Option<&'static str> {
    Some(match name {
        "group_id" | "advertise" | "certificate" | "private_key" | "ca" | "crsqlite_extension" => {
            ""
        }
        "bind" => "0.0.0.0:7443",
        "bootstrap" | "allowed_peers" | "allowed_networks" => "[]",
        "poll_ms" => "100",
        "anti_entropy_ms" => "10000",
        "sessions" => "32",
        "batch_bytes" => "1048576",
        "max_batch_bytes" => "67108864",
        "queue_bytes" => "67108864",
        "staging_bytes" => "268435456",
        "staging_timeout_ms" => "300000",
        _ => return None,
    })
}
pub(crate) fn setting(db: &Connection, name: &str) -> Result<String> {
    use rusqlite::OptionalExtension;
    let default = default_value(name)
        .ok_or_else(|| Error::Invalid(format!("unknown PRAGMA murmur_{name}")))?;
    Ok(db
        .query_row(
            "SELECT value FROM murmur_config WHERE name=?",
            [name],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or_else(|| default.into()))
}
pub(crate) fn validate(name: &str, value: &str) -> Result<()> {
    default_value(name).ok_or_else(|| Error::Invalid(format!("unknown PRAGMA murmur_{name}")))?;
    if value.len() > 65536 || value.contains('\0') {
        return Err(Error::Invalid("setting too large or contains NUL".into()));
    }
    match name {
        "group_id" => {
            parse_id(value)?;
        }
        "bind" | "advertise" => {
            if name == "bind" || !value.is_empty() {
                value
                    .parse::<SocketAddr>()
                    .map_err(|_| Error::Invalid("expected IP:port".into()))?;
            }
        }
        "bootstrap" => {
            let v: Vec<SocketAddr> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
        }
        "allowed_peers" => {
            let v: Vec<String> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
            for s in v {
                parse_id(&s)?;
            }
        }
        "allowed_networks" => {
            let v: Vec<ipnet::IpNet> =
                serde_json::from_str(value).map_err(|e| Error::Invalid(e.to_string()))?;
            if v.len() > 4096 {
                return Err(Error::Overloaded);
            }
        }
        "poll_ms" | "anti_entropy_ms" | "staging_timeout_ms" => {
            let n = value
                .parse::<u64>()
                .map_err(|_| Error::Invalid("expected milliseconds".into()))?;
            let minimum = if name == "staging_timeout_ms" {
                60000
            } else {
                10
            };
            if !(minimum..=86400000).contains(&n) {
                return Err(Error::Invalid(format!(
                    "interval outside {minimum}..86400000 ms"
                )));
            }
        }
        "sessions" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected session count".into()))?;
            if !(1..=256).contains(&n) {
                return Err(Error::Invalid("sessions outside 1..256".into()));
            }
        }
        "batch_bytes" | "max_batch_bytes" | "queue_bytes" | "staging_bytes" => {
            let n = value
                .parse::<usize>()
                .map_err(|_| Error::Invalid("expected byte count".into()))?;
            if !(65536..=1073741824).contains(&n) {
                return Err(Error::Invalid("byte budget outside 64 KiB..1 GiB".into()));
            }
        }
        _ => {}
    }
    Ok(())
}
fn certificates(path: &str) -> Result<Vec<Vec<u8>>> {
    let bytes = std::fs::read(path)?;
    let certs = rustls_pemfile::certs(&mut BufReader::new(bytes.as_slice()))
        .map(|r| r.map(|c| c.to_vec()))
        .collect::<std::io::Result<Vec<_>>>()?;
    if certs.is_empty() {
        return Err(Error::Invalid("empty PEM certificate file".into()));
    }
    Ok(certs)
}
impl Config {
    pub fn load(db: &Connection, site: NodeId) -> Result<Self> {
        let get = |s| setting(db, s);
        let num = |s| -> Result<usize> {
            get(s)?
                .parse()
                .map_err(|_| Error::Invalid(format!("invalid {s}")))
        };
        let chain = certificates(&get("certificate")?)?;
        let node = transport::node_id(&chain[0])?;
        let roots = certificates(&get("ca")?)?;
        let bytes = zeroize::Zeroizing::new(std::fs::read(get("private_key")?)?);
        let key = rustls_pemfile::private_key(&mut BufReader::new(bytes.as_slice()))?
            .ok_or_else(|| Error::Invalid("missing PEM private key".into()))?;
        let incarnation: i64 = db.query_row(
            "UPDATE murmur_identity SET incarnation=incarnation+1 WHERE id=1 RETURNING incarnation",
            [],
            |r| r.get(0),
        )?;
        let cfg = Self {
            group: parse_id(&get("group_id")?)?,
            node,
            site,
            bind: get("bind")?
                .parse()
                .map_err(|_| Error::Invalid("bind".into()))?,
            advertise: if get("advertise")?.is_empty() {
                None
            } else {
                Some(
                    get("advertise")?
                        .parse()
                        .map_err(|_| Error::Invalid("advertise".into()))?,
                )
            },
            credentials: Credentials {
                chain,
                roots,
                private_key: zeroize::Zeroizing::new(key.secret_der().to_vec()),
            },
            allowed_peers: serde_json::from_str::<Vec<String>>(&get("allowed_peers")?)
                .map_err(|e| Error::Invalid(e.to_string()))?
                .iter()
                .map(|s| parse_id(s))
                .collect::<Result<_>>()?,
            allowed_networks: serde_json::from_str(&get("allowed_networks")?)
                .map_err(|e| Error::Invalid(e.to_string()))?,
            bootstrap: serde_json::from_str(&get("bootstrap")?)
                .map_err(|e| Error::Invalid(e.to_string()))?,
            excluded: {
                let mut s = db.prepare("SELECT node FROM murmur_excluded")?;
                let rows = s.query_map([], |r| r.get::<_, Vec<u8>>(0))?;
                rows.map(|r| {
                    r.map_err(Error::from).and_then(|v| {
                        v.try_into()
                            .map_err(|_| Error::Invalid("excluded identity".into()))
                    })
                })
                .collect::<Result<_>>()?
            },
            incarnation: incarnation as u64,
            sessions: num("sessions")?,
            queue_bytes: num("queue_bytes")?,
            batch_bytes: num("batch_bytes")?,
            max_batch_bytes: num("max_batch_bytes")?,
            staging_bytes: num("staging_bytes")?,
            staging_timeout_ms: num("staging_timeout_ms")? as u64,
            poll_ms: num("poll_ms")? as u64,
            anti_entropy_ms: num("anti_entropy_ms")? as u64,
        };
        if cfg.batch_bytes > cfg.max_batch_bytes
            || cfg.max_batch_bytes > 64 << 20
            || cfg.max_batch_bytes > cfg.queue_bytes
            || cfg.max_batch_bytes > cfg.staging_bytes
        {
            return Err(Error::Invalid(
                "require batch_bytes <= max_batch_bytes <= min(queue_bytes, staging_bytes, 64 MiB)"
                    .into(),
            ));
        }
        let old: Option<Vec<u8>> =
            db.query_row("SELECT node FROM murmur_identity WHERE id=1", [], |r| {
                r.get(0)
            })?;
        if old.as_deref().is_some_and(|v| v != node) {
            return Err(Error::Auth(
                "certificate changed node identity for this database".into(),
            ));
        }
        db.execute(
            "UPDATE murmur_identity SET node=? WHERE id=1",
            [node.as_slice()],
        )?;
        Ok(cfg)
    }
}
