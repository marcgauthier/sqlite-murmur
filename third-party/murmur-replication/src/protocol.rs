//! A separate CRR protocol: intentionally incompatible with Murmur Spool frames.
use crate::*;
use sha2::{Digest, Sha256};
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
    End,
    Ack {
        digest: [u8; 32],
    },
    Error(String),
}
pub fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
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
