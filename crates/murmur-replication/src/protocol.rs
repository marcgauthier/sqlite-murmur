//! A separate CRR protocol: intentionally incompatible with Murmur Spool frames.
use crate::*;
use crate::crypto_provider::Sha256;
pub const ALPN: &[u8] = b"murmur-crsqlite/1";
pub const FRAME_BYTES: usize = 128 << 10;
pub const CHUNK_BYTES: usize = 64 << 10;
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Cursor {
    pub from: i64,
    pub through: Option<i64>,
    pub after: Option<Key>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Key {
    pub version: i64,
    pub seq: i64,
    pub table: String,
    pub pk: Vec<u8>,
    pub cid: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    Hello {
        group: NodeId,
        node: NodeId,
        site: NodeId,
        schema: [u8; 32],
        incarnation: u64,
        address: SocketAddr,
    },
    Pull {
        group: NodeId,
        node: NodeId,
        site: NodeId,
        schema: [u8; 32],
        expected_site: Option<NodeId>,
        cursor: Cursor,
    },
    Reset {
        site: NodeId,
    },
    Idle,
    Batch {
        site: NodeId,
        cursor: Cursor,
        bytes: u64,
        digest: [u8; 32],
    },
    Chunk {
        index: u32,
        data: Vec<u8>,
    },
    SnapshotRequest {
        group: NodeId,
        node: NodeId,
        site: NodeId,
        schema: [u8; 32],
    },
    SnapshotManifest {
        manifest: Vec<u8>,
    },
    SchemaRequest,
    SchemaManifest {
        manifest: Vec<u8>,
    },
    SchemaAck {
        digest: [u8; 32],
        sequence: u64,
    },
    End,
    Ack {
        digest: [u8; 32],
    },
    Error(String),
}
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// One decoded CRSB change cell. Layout mirrors `Store::export` exactly:
/// field(table) + field(pk) + field(cid) + tagged val + col_version +
/// db_version + field(site_id) + cl + seq, where field = u32le len +
/// bytes and val = 0 null | 1 i64le | 2 f64le | 3 field | 4 field.
/// The header is `CRSB` + version 1 + 3 reserved bytes + u32le count.
#[derive(Clone, Debug, PartialEq)]
pub struct Cell {
    pub table: String,
    pub pk: Vec<u8>,
    pub cid: String,
    pub val: CellVal,
    pub col_version: i64,
    pub db_version: i64,
    pub site_id: Vec<u8>,
    pub cl: i64,
    pub seq: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CellVal {
    Null,
    Integer(i64),
    Real(f64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

fn take<'a>(rest: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if rest.len() < n {
        return Err(Error::Protocol("CRSB truncated".into()));
    }
    let (head, tail) = rest.split_at(n);
    *rest = tail;
    Ok(head)
}

fn take_field(rest: &mut &[u8]) -> Result<Vec<u8>> {
    let len = u32::from_le_bytes(take(rest, 4)?.try_into().unwrap()) as usize;
    Ok(take(rest, len)?.to_vec())
}

fn take_i64(rest: &mut &[u8]) -> Result<i64> {
    Ok(i64::from_le_bytes(take(rest, 8)?.try_into().unwrap()))
}

/// Strict decode: magic, version, and cell count are verified and no
/// trailing bytes are allowed. Malformed input rejects the batch.
pub fn decode_cells(blob: &[u8]) -> Result<Vec<Cell>> {
    let mut rest = blob;
    let magic = take(&mut rest, 4)?;
    if magic != b"CRSB" {
        return Err(Error::Protocol("CRSB magic".into()));
    }
    if take(&mut rest, 4)? != [1, 0, 0, 0] {
        return Err(Error::Protocol("CRSB version".into()));
    }
    let count = u32::from_le_bytes(take(&mut rest, 4)?.try_into().unwrap()) as usize;
    let mut cells = Vec::with_capacity(count.min(1 << 20));
    for _ in 0..count {
        let table = String::from_utf8(take_field(&mut rest)?)
            .map_err(|_| Error::Protocol("CRSB table".into()))?;
        let pk = take_field(&mut rest)?;
        let cid = String::from_utf8(take_field(&mut rest)?)
            .map_err(|_| Error::Protocol("CRSB cid".into()))?;
        let val = match take(&mut rest, 1)?[0] {
            0 => CellVal::Null,
            1 => CellVal::Integer(take_i64(&mut rest)?),
            2 => CellVal::Real(f64::from_le_bytes(take(&mut rest, 8)?.try_into().unwrap())),
            3 => CellVal::Text(take_field(&mut rest)?),
            4 => CellVal::Blob(take_field(&mut rest)?),
            _ => return Err(Error::Protocol("CRSB value tag".into())),
        };
        let col_version = take_i64(&mut rest)?;
        let db_version = take_i64(&mut rest)?;
        let site_id = take_field(&mut rest)?;
        let cl = take_i64(&mut rest)?;
        let seq = take_i64(&mut rest)?;
        cells.push(Cell {
            table,
            pk,
            cid,
            val,
            col_version,
            db_version,
            site_id,
            cl,
            seq,
        });
    }
    if !rest.is_empty() {
        return Err(Error::Protocol("CRSB trailing bytes".into()));
    }
    Ok(cells)
}

fn put_field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend((value.len() as u32).to_le_bytes());
    out.extend(value);
}

/// Encode cells back to wire bytes. Decoding an export and re-encoding
/// reproduces the input byte-for-byte (round-trip tested).
pub fn encode_cells(cells: &[Cell]) -> Vec<u8> {
    let mut blob = b"CRSB\x01\0\0\0\0\0\0\0".to_vec();
    blob[8..12].copy_from_slice(&(cells.len() as u32).to_le_bytes());
    for c in cells {
        put_field(&mut blob, c.table.as_bytes());
        put_field(&mut blob, &c.pk);
        put_field(&mut blob, c.cid.as_bytes());
        match &c.val {
            CellVal::Null => blob.push(0),
            CellVal::Integer(v) => {
                blob.push(1);
                blob.extend(v.to_le_bytes());
            }
            CellVal::Real(v) => {
                blob.push(2);
                blob.extend(v.to_le_bytes());
            }
            CellVal::Text(v) => {
                blob.push(3);
                put_field(&mut blob, v);
            }
            CellVal::Blob(v) => {
                blob.push(4);
                put_field(&mut blob, v);
            }
        }
        blob.extend(c.col_version.to_le_bytes());
        blob.extend(c.db_version.to_le_bytes());
        put_field(&mut blob, &c.site_id);
        blob.extend(c.cl.to_le_bytes());
        blob.extend(c.seq.to_le_bytes());
    }
    blob
}
pub async fn write(stream: &mut quinn::SendStream, msg: &Message) -> Result<()> {
    let data = encode(msg)?;
    if data.len() > FRAME_BYTES {
        return Err(Error::Overloaded);
    }
    stream
        .write_all(&(data.len() as u32).to_be_bytes())
        .await
        .map_err(|e| Error::Protocol(e.to_string()))?;
    stream
        .write_all(&data)
        .await
        .map_err(|e| Error::Protocol(e.to_string()))?;
    Ok(())
}
pub async fn read(stream: &mut quinn::RecvStream) -> Result<Message> {
    let mut h = [0; 4];
    stream
        .read_exact(&mut h)
        .await
        .map_err(|e| Error::Protocol(e.to_string()))?;
    let n = u32::from_be_bytes(h) as usize;
    if n == 0 || n > FRAME_BYTES {
        return Err(Error::Protocol("frame length".into()));
    }
    let mut b = vec![0; n];
    stream
        .read_exact(&mut b)
        .await
        .map_err(|e| Error::Protocol(e.to_string()))?;
    decode(&b)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample_cells() -> Vec<Cell> {
        vec![
            Cell {
                table: "docs".into(),
                pk: vec![1, 9, 1],
                cid: "title".into(),
                val: CellVal::Text(b"hello".to_vec()),
                col_version: 2,
                db_version: 7,
                site_id: vec![3; 16],
                cl: 1,
                seq: 0,
            },
            Cell {
                table: "docs".into(),
                pk: vec![1, 9, 2],
                cid: "n".into(),
                val: CellVal::Integer(-42),
                col_version: 3,
                db_version: 8,
                site_id: vec![3; 16],
                cl: 1,
                seq: 1,
            },
            Cell {
                table: "vecs".into(),
                pk: vec![1, 9, 1],
                cid: "x".into(),
                val: CellVal::Real(1.5),
                col_version: 1,
                db_version: 9,
                site_id: vec![4; 16],
                cl: 2,
                seq: 0,
            },
            Cell {
                table: "vecs".into(),
                pk: vec![1, 9, 1],
                cid: "raw".into(),
                val: CellVal::Blob(vec![0xff, 0x00]),
                col_version: 1,
                db_version: 9,
                site_id: vec![4; 16],
                cl: 2,
                seq: 1,
            },
            Cell {
                table: "docs".into(),
                pk: vec![1, 9, 3],
                cid: "title".into(),
                val: CellVal::Null,
                col_version: 4,
                db_version: 10,
                site_id: vec![3; 16],
                cl: 1,
                seq: 2,
            },
        ]
    }
    #[test]
    fn cells_round_trip_byte_exact() {
        let cells = sample_cells();
        let blob = encode_cells(&cells);
        assert_eq!(decode_cells(&blob).unwrap(), cells);
        assert_eq!(encode_cells(&decode_cells(&blob).unwrap()), blob);
        assert!(blob.starts_with(b"CRSB\x01\0\0\0"));
        assert_eq!(u32::from_le_bytes(blob[8..12].try_into().unwrap()), 5);
    }
    #[test]
    fn cells_reject_malformed() {
        assert!(decode_cells(b"BAD!").is_err());
        let good = encode_cells(&sample_cells());
        let mut bad_magic = good.clone();
        bad_magic[0] = b'X';
        assert!(decode_cells(&bad_magic).is_err());
        let mut bad_ver = good.clone();
        bad_ver[4] = 2;
        assert!(decode_cells(&bad_ver).is_err());
        assert!(decode_cells(&good[..good.len() - 1]).is_err());
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(decode_cells(&trailing).is_err());
        let mut bad_count = good.clone();
        bad_count[8..12].copy_from_slice(&99u32.to_le_bytes());
        assert!(decode_cells(&bad_count).is_err());
        let mut bad_tag = good.clone();
        // First cell value tag sits after header + 3 fields; corrupt it.
        // Header 12 + table(4+4) + pk(4+3) + cid(4+5) = 36.
        bad_tag[36] = 9;
        assert!(decode_cells(&bad_tag).is_err());
        assert!(decode_cells(&[]).is_err());
    }
    async fn read_wire(bytes: Vec<u8>) -> Result<Message> {
        use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let der = CertificateDer::from(cert.cert.der().to_vec());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));
        let server = quinn::Endpoint::server(
            quinn::ServerConfig::with_single_cert(vec![der.clone()], key).unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(der).unwrap();
        let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        client.set_default_client_config(
            quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let connecting = client
            .connect(server.local_addr().unwrap(), "localhost")
            .unwrap();
        let incoming = async { server.accept().await.unwrap().await.unwrap() };
        let (client_conn, server_conn) = tokio::join!(connecting, incoming);
        let client_conn = client_conn.unwrap();
        let receive = tokio::spawn(async move {
            let (_send, mut recv) = server_conn.accept_bi().await.unwrap();
            read(&mut recv).await
        });
        let (mut send, _) = client_conn.open_bi().await.unwrap();
        send.write_all(&bytes).await.unwrap();
        send.finish().unwrap();
        tokio::time::timeout(Duration::from_secs(5), receive)
            .await
            .unwrap()
            .unwrap()
    }
    #[tokio::test]
    async fn rejects_oversized_empty_truncated_and_invalid_frames() {
        assert!(
            read_wire(((FRAME_BYTES + 1) as u32).to_be_bytes().to_vec())
                .await
                .is_err()
        );
        assert!(read_wire(0u32.to_be_bytes().to_vec()).await.is_err());
        assert!(read_wire(vec![0, 0, 0, 3, 0xff]).await.is_err());
        assert!(read_wire(vec![0, 0, 0, 1, 0xff]).await.is_err());
    }
}
