//! Connection/session orchestration adapted from Murmur, with CRR pull windows.
use crate::{
    backend::CrrBackend,
    membership::{Member, Packet, State, View},
    protocol::{self, Message},
    snapshot,
    storage::{ADMISSION_TIMEOUT, Worker},
    *,
};
use murmur_tx::Backend as _;
use serde_json::json;
use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{Notify, Semaphore, watch};
#[derive(Clone)]
struct Peer {
    connection: quinn::Connection,
    outgoing: bool,
    incarnation: u64,
    schema_sent: Option<([u8; 32], u64)>,
}
pub struct Live {
    pub cfg: Config,
    worker: Arc<Worker>,
    rt: tokio::runtime::Handle,
    snapshot_status: Mutex<Option<String>>,
    endpoint: quinn::Endpoint,
    peers: Mutex<BTreeMap<NodeId, Peer>>,
    view: Mutex<View>,
    seeds: Mutex<BTreeSet<SocketAddr>>,
    relays: Mutex<BTreeMap<(NodeId, u64), (NodeId, std::time::Instant)>>,
    pub error: Mutex<Option<String>>,
    pub sync: Notify,
    serving: Arc<Semaphore>,
    receiving: Arc<Semaphore>,
    sessions: Arc<Semaphore>,
    applied: std::sync::atomic::AtomicU64,
    sent: std::sync::atomic::AtomicU64,
    schema_adoptions: std::sync::atomic::AtomicU64,
    stopped: AtomicBool,
    persisted_incarnation: std::sync::atomic::AtomicU64,
    vetted: Arc<Mutex<BTreeMap<(NodeId, [u8; 32]), bool>>>,
    vet_ok: Arc<std::sync::atomic::AtomicU64>,
    vet_no: Arc<std::sync::atomic::AtomicU64>,
    vet_fail: Arc<std::sync::atomic::AtomicU64>,
    serve_ok: Arc<std::sync::atomic::AtomicU64>,
    serve_reject: Arc<std::sync::atomic::AtomicU64>,
    serve_error: Arc<std::sync::atomic::AtomicU64>,
}
pub struct Handle {
    pub live: Arc<Live>,
    stop: watch::Sender<bool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Handle {
    pub fn start(cfg: Config, worker: Arc<Worker>) -> Result<Self> {
        let (stop, rx) = watch::channel(false);
        let (tx, ready) = std::sync::mpsc::sync_channel(1);
        let thread = std::thread::Builder::new()
            .name("murmur-quic".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string()));
                        return;
                    }
                };
                rt.block_on(async move {
                    let endpoint = match transport::endpoint(&cfg, cfg.node) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = tx.send(Err(e.to_string()));
                            return;
                        }
                    };
                    let address = cfg
                        .advertise
                        .unwrap_or_else(|| endpoint.local_addr().unwrap());
                    let mut view = View::new(Member {
                        node: cfg.node,
                        address,
                        incarnation: cfg.incarnation,
                        state: State::Alive,
                    });
                    view.excluded = cfg.excluded.clone();
                    let rt = tokio::runtime::Handle::current();
                    let live = Arc::new(Live {
                        rt,
                        snapshot_status: Mutex::new(None),
                        view: Mutex::new(view),
                        peers: Mutex::new(BTreeMap::new()),
                        seeds: Mutex::new(cfg.bootstrap.iter().copied().collect()),
                        relays: Mutex::new(BTreeMap::new()),
                        error: Mutex::new(None),
                        sync: Notify::new(),
                        serving: Arc::new(Semaphore::new(cfg.queue_bytes)),
                        receiving: Arc::new(Semaphore::new(cfg.queue_bytes)),
                        sessions: Arc::new(Semaphore::new(cfg.sessions)),
                        applied: 0.into(),
                        sent: 0.into(),
                        schema_adoptions: 0.into(),
                        stopped: AtomicBool::new(false),
                        persisted_incarnation: cfg.incarnation.into(),
                        vetted: Arc::new(Mutex::new(BTreeMap::new())),
                        vet_ok: Arc::new(0.into()),
                        vet_no: Arc::new(0.into()),
                        vet_fail: Arc::new(0.into()),
                        serve_ok: Arc::new(0.into()),
                        serve_reject: Arc::new(0.into()),
                        serve_error: Arc::new(0.into()),
                        endpoint,
                        worker,
                        cfg,
                    });
                    let _ = tx.send(Ok(live.clone()));
                    live.run(rx).await;
                });
                // Let interrupted blocking SQLite calls finish before dropping their runtime.
                rt.shutdown_timeout(Duration::from_secs(5));
            })?;
        let live = ready
            .recv()
            .map_err(|_| Error::Unavailable("network startup".into()))?
            .map_err(Error::Unavailable)?;
        Ok(Self {
            live,
            stop,
            thread: Some(thread),
        })
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}
impl Live {
    pub fn status(&self) -> serde_json::Value {
        json!({"running":!self.stopped.load(Ordering::Relaxed),"node_id":hex(&self.cfg.node),"site_id":hex(&self.cfg.site),"group_id":hex(&self.cfg.group),"bind":self.endpoint.local_addr().ok().map(|a|a.to_string()),"connected_peers":self.peers.lock().unwrap().len(),"applied_batches":self.applied.load(Ordering::Relaxed),"sent_batches":self.sent.load(Ordering::Relaxed),"snapshot":self.snapshot_status.lock().unwrap().clone(),"schema_adoptions":self.schema_adoptions.load(Ordering::Relaxed),"vet_ok":self.vet_ok.load(Ordering::Relaxed),"vet_no":self.vet_no.load(Ordering::Relaxed),"vet_fail":self.vet_fail.load(Ordering::Relaxed),"serve_ok":self.serve_ok.load(Ordering::Relaxed),"serve_reject":self.serve_reject.load(Ordering::Relaxed),"serve_error":self.serve_error.load(Ordering::Relaxed),"last_error":self.error.lock().unwrap().clone()})
    }
    /// Bytes currently reserved from the serving budget (health only).
    pub fn serving_in_use(&self) -> usize {
        self.cfg
            .queue_bytes
            .saturating_sub(self.serving.available_permits())
    }
    /// Bytes currently reserved from the receiving budget (health only).
    pub fn receiving_in_use(&self) -> usize {
        self.cfg
            .queue_bytes
            .saturating_sub(self.receiving.available_permits())
    }
    /// Record peer contact for retention floors on a blocking thread
    /// (Backend methods take worker calls and must not run on async
    /// workers). Best-effort: a retired peer or stopped worker must not
    /// fail the transfer that just completed.
    async fn touch_peer(
        worker: &Arc<Worker>,
        cfg: &Config,
        node: NodeId,
        progress: Vec<murmur_tx::protocol::Progress>,
    ) {
        let worker = worker.clone();
        let cfg = cfg.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
            let _ = murmur_tx::retention::touch(&backend, node, &progress);
        })
        .await;
    }
    pub fn members(&self) -> Vec<serde_json::Value> {
        let peers = self.peers.lock().unwrap();
        let view = self.view.lock().unwrap();
        view.members.values().map(|m|json!({"node_id":hex(&m.node),"address":m.address.to_string(),"state":format!("{:?}",m.state).to_lowercase(),"connected":peers.contains_key(&m.node),"incarnation":m.incarnation})).collect()
    }
    /// Fetch dial targets: uniform-port derivation (peer IP, our
    /// fetch_bind port). The gossip address is the REPLICATION
    /// endpoint, but fetch binds its own UDP socket, so dialing it
    /// as-is would miss the fetch endpoint on every peer. Correct
    /// for uniform-port deployments (same fetch port on every host)
    /// and single-node; localhost tests use distinct 127.0.0.x with
    /// uniform ports. Skips self, excluded, and Dead/Left members;
    /// Suspect stays included (suspicion often resolves; fetch
    /// falls through per-source on failure).
    pub fn fetch_sources(&self, fetch_port: u16) -> Vec<(NodeId, SocketAddr)> {
        let view = self.view.lock().unwrap();
        view.members
            .values()
            .filter(|m| m.node != view.local.node)
            .filter(|m| !view.excluded.contains(&m.node))
            .filter(|m| matches!(m.state, State::Alive | State::Suspect))
            .map(|m| {
                let mut addr = m.address;
                addr.set_port(fetch_port);
                (m.node, addr)
            })
            .collect()
    }
    pub fn add(&self, address: SocketAddr) -> Result<()> {
        let mut seeds = self.seeds.lock().unwrap();
        if seeds.len() >= 4096 && !seeds.contains(&address) {
            return Err(Error::Overloaded);
        }
        seeds.insert(address);
        self.sync.notify_waiters();
        Ok(())
    }
    pub fn remove(&self, node: NodeId) {
        let address = {
            let mut view = self.view.lock().unwrap();
            view.excluded.insert(node);
            view.pending.remove(&node);
            view.suspect.remove(&node);
            view.members.get_mut(&node).map(|m| {
                m.state = State::Left;
                m.address
            })
        };
        if let Some(address) = address {
            self.seeds.lock().unwrap().remove(&address);
        }
        if let Some(p) = self.peers.lock().unwrap().remove(&node) {
            p.connection.close(0u32.into(), b"administratively removed");
        }
    }
    fn record(&self, e: impl std::fmt::Display) {
        *self.error.lock().unwrap() = Some(e.to_string());
    }
    async fn run(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let mut tasks = tokio::task::JoinSet::new();
        let mut gossip = tokio::time::interval(Duration::from_millis(200));
        let mut probes = tokio::time::interval(Duration::from_secs(1));
        let mut dial = tokio::time::interval(Duration::from_secs(1));
        let mut retries = BTreeMap::<SocketAddr, (std::time::Instant, u32)>::new();
        let mut pending = BTreeSet::new();
        let (tx, mut results) = tokio::sync::mpsc::channel(256);
        for tick in [&mut gossip, &mut probes, &mut dial] {
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        }
        loop {
            tokio::select! {
                _=stop.changed()=>break,
                Some(incoming)=self.endpoint.accept()=>{
                    if tasks.len()>=self.cfg.sessions*4 {incoming.refuse();continue;}
                    let me=self.clone();tasks.spawn(async move{match tokio::time::timeout(Duration::from_secs(10),incoming).await{Ok(Ok(c))=>{me.session(c,false).await;},Ok(Err(e))=>me.record(e),Err(_)=>me.record("incoming TLS deadline")}});
                },
                _=gossip.tick()=>self.gossip(),
                _=probes.tick()=>self.probe(),
                _=dial.tick()=>{
                    let connected:BTreeSet<_>=self.peers.lock().unwrap().values().map(|p|p.connection.remote_address()).collect();
                    pending.retain(|address|!connected.contains(address));
                    let mut addresses=self.seeds.lock().unwrap().clone();
                    {let view=self.view.lock().unwrap();addresses.extend(view.members.values().filter(|m|m.state==State::Alive&&!view.excluded.contains(&m.node)).map(|m|m.address));for m in view.members.values().filter(|m|view.excluded.contains(&m.node)){addresses.remove(&m.address);}}
                    use rand::seq::SliceRandom;let mut addresses:Vec<_>=addresses.into_iter().collect();addresses.shuffle(&mut rand::thread_rng());
                    for address in addresses{
                        if tasks.len()>=self.cfg.sessions*4||pending.len()>=8||self.peers.lock().unwrap().len()>=self.cfg.sessions{break;}
                        if !transport::network_allowed(&self.cfg,address)||connected.contains(&address)||pending.contains(&address)||retries.get(&address).is_some_and(|(at,_)|*at>std::time::Instant::now()){continue;}
                        pending.insert(address);let me=self.clone();let tx=tx.clone();
                        tasks.spawn(async move{
                            let result=match me.endpoint.connect(address,"murmur"){Ok(c)=>tokio::time::timeout(Duration::from_secs(10),c).await.map_err(|_|Error::Unavailable("QUIC connect deadline".into())).and_then(|c|c.map_err(|e|Error::Unavailable(e.to_string()))),Err(e)=>Err(Error::Unavailable(e.to_string()))};
                            let success=match result{Ok(c)=>me.session(c,true).await,Err(e)=>{me.record(e);false}};
                            let _=tx.send((address,success)).await;
                        });
                    }
                },
                Some((address,success))=results.recv()=>{
                    pending.remove(&address);let attempt=if success{0}else{retries.get(&address).map_or(1,|(_,n)|(n+1).min(6))};
                    let wait=(1u64<<attempt).min(60)*1000+rand::random::<u64>()%500;
                    retries.insert(address,(std::time::Instant::now()+Duration::from_millis(wait),attempt));
                },
                Some(_)=tasks.join_next()=>{},
            }
        }
        self.stopped.store(true, Ordering::Relaxed);
        let members = {
            let mut v = self.view.lock().unwrap();
            v.local.state = State::Left;
            v.gossip()
        };
        self.broadcast(Packet::Gossip {
            database: self.cfg.group,
            sender: self.cfg.node,
            members,
        });
        self.endpoint.close(0u32.into(), b"murmur stopped");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    async fn session(self: Arc<Self>, connection: quinn::Connection, outgoing: bool) -> bool {
        let id = match transport::peer_id(&connection, &self.cfg) {
            Ok(id) if id != self.cfg.node => id,
            Ok(_) => {
                connection.close(1u32.into(), b"duplicate local identity");
                return false;
            }
            Err(e) => {
                self.record(e);
                connection.close(1u32.into(), b"identity rejected");
                return false;
            }
        };
        if self.view.lock().unwrap().excluded.contains(&id) {
            connection.close(1u32.into(), b"excluded");
            return false;
        }
        let hello = tokio::time::timeout(
            Duration::from_secs(10),
            self.handshake(&connection, outgoing, id),
        )
        .await;
        let (incarnation, address) = match hello {
            Ok(Ok(value)) => value,
            result => {
                let msg = match result {
                    Ok(Err(e)) => match connection.close_reason() {
                        Some(reason) => format!("{e}: {reason}"),
                        None => e.to_string(),
                    },
                    _ => "replication handshake deadline".into(),
                };
                self.record(&msg);
                connection.close(1u32.into(), msg.as_bytes());
                return false;
            }
        };
        self.view.lock().unwrap().merge(Member {
            node: id,
            address,
            incarnation,
            state: State::Alive,
        });
        {
            let mut peers = self.peers.lock().unwrap();
            if let Some(old) = peers.get(&id) {
                let preferred = outgoing == (self.cfg.node < id);
                if old.connection.close_reason().is_none()
                    && (incarnation < old.incarnation
                        || (incarnation == old.incarnation
                            && (!preferred || old.outgoing == outgoing)))
                {
                    connection.close(0u32.into(), b"duplicate");
                    return false;
                }
                old.connection.close(0u32.into(), b"superseded");
            }
            if !peers.contains_key(&id) && peers.len() >= self.cfg.sessions {
                connection.close(1u32.into(), b"session capacity");
                return false;
            }
            peers.insert(
                id,
                Peer {
                    connection: connection.clone(),
                    outgoing,
                    incarnation,
                    schema_sent: None,
                },
            );
        }
        let me = self.clone();
        let c = connection.clone();
        let pull = tokio::spawn(async move {
            me.pull_loop(id, c).await;
        });
        let me = self.clone();
        let c = connection.clone();
        let packets = tokio::spawn(async move {
            while let Ok(bytes) = c.read_datagram().await {
                if let Err(e) = me.packet(id, &bytes).await {
                    me.record(e);
                }
            }
        });
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let first = match tokio::time::timeout(
                Duration::from_secs(60),
                protocol::read(&mut recv),
            )
            .await
            {
                Ok(Ok(m)) => m,
                Ok(Err(e)) => {
                    self.record(&e);
                    let _ = protocol::write(&mut send, &Message::Error(e.to_string())).await;
                    let _ = send.finish();
                    continue;
                }
                Err(_) => {
                    self.record("serve deadline");
                    let _ = send.reset(1u32.into());
                    let _ = send.finish();
                    continue;
                }
            };
            // Pulls keep the 60s total budget; snapshot streams are
            // recipient-paced and bounded by snapshot_bytes instead
            // (a 60s cap would fail any large join).
            let result = match first {
                Message::Pull {
                    group,
                    node,
                    site,
                    schema,
                    expected_site,
                    cursor,
                } => {
                    match tokio::time::timeout(
                        Duration::from_secs(60),
                        self.serve_pull(
                            id,
                            &connection,
                            &mut send,
                            &mut recv,
                            group,
                            node,
                            site,
                            schema,
                            expected_site,
                            cursor,
                        ),
                    )
                    .await
                    {
                        Ok(r) => {
                            match &r {
                                Ok(()) => self.serve_ok.fetch_add(1, Ordering::Relaxed),
                                Err(Error::Protocol(m)) if m.contains("mismatch") => {
                                    self.serve_reject.fetch_add(1, Ordering::Relaxed)
                                }
                                Err(_) => self.serve_error.fetch_add(1, Ordering::Relaxed),
                            };
                            r
                        }
                        Err(_) => {
                            self.record("pull deadline");
                            let _ = send.reset(1u32.into());
                            let _ = send.finish();
                            continue;
                        }
                    }
                }
                Message::SnapshotRequest {
                    group,
                    node,
                    site,
                    schema,
                } => {
                    self.serve_snapshot(id, &connection, &mut send, &mut recv, group, node, site, schema)
                        .await
                }
                // Schema verbs are small and fast (one manifest build or
                // vet); no outer timeout beyond the first-read budget.
                Message::SchemaRequest => self.serve_schema_request(id, &mut send).await,
                Message::SchemaManifest { manifest } => {
                    self.receive_schema_push(id, &mut send, manifest).await
                }
                _ => Err(Error::Protocol(
                    "expected pull, snapshot, or schema request".into(),
                )),
            };
            if let Err(e) = result {
                self.record(&e);
                let _ = protocol::write(&mut send, &Message::Error(e.to_string())).await;
            }
            let _ = send.finish();
        }
        pull.abort();
        packets.abort();
        let _ = pull.await;
        let _ = packets.await;
        let mut peers = self.peers.lock().unwrap();
        if peers
            .get(&id)
            .is_some_and(|p| p.connection.stable_id() == connection.stable_id())
        {
            peers.remove(&id);
        }
        true
    }
    async fn handshake(
        &self,
        c: &quinn::Connection,
        outgoing: bool,
        id: NodeId,
    ) -> Result<(u64, SocketAddr)> {
        let schema = self.worker.run(|s| s.schema()).await?;
        let local = self.view.lock().unwrap().local.clone();
        let hello = Message::Hello {
            group: self.cfg.group,
            node: self.cfg.node,
            site: self.cfg.site,
            schema,
            incarnation: local.incarnation,
            address: local.address,
        };
        let (mut send, mut recv) = if outgoing {
            c.open_bi().await
        } else {
            c.accept_bi().await
        }
        .map_err(|e| Error::Unavailable(e.to_string()))?;
        if outgoing {
            protocol::write(&mut send, &hello).await?;
        }
        let remote = protocol::read(&mut recv).await?;
        let Message::Hello {
            group,
            node,
            site,
            schema: remote_schema,
            incarnation,
            address,
        } = remote
        else {
            return Err(Error::Protocol("expected session hello".into()));
        };
        if group != self.cfg.group || node != id || site == self.cfg.site || site == [0; 16] {
            return Err(Error::Auth("session group or identity mismatch".into()));
        }
        // Handshake schemas are a fast-fail hint only when adoption is
        // off; adopters proceed and converge through manifests.
        if schema != remote_schema {
            let accept = self
                .worker
                .run(|s| Ok(schema::params_from(&s.db)?.accept_remote_schema))
                .await?;
            if !accept {
                return Err(Error::Protocol(
                    "CRR schema mismatch; provision matching table definitions".into(),
                ));
            }
        }
        if !outgoing {
            protocol::write(&mut send, &hello).await?;
        }
        let _ = send.finish();
        Ok((incarnation, address))
    }
    async fn pull_loop(self: Arc<Self>, id: NodeId, c: quinn::Connection) {
        let mut poll = tokio::time::interval(Duration::from_millis(self.cfg.poll_ms));
        poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {_=poll.tick()=>{},_=self.sync.notified()=>{},_=c.closed()=>break}
            if c.close_reason().is_some() {
                break;
            }
            match tokio::time::timeout(Duration::from_secs(60), self.pull(id, &c)).await {
                Ok(Ok(more)) => {
                    if more {
                        poll.reset_immediately();
                    }
                    self.push_schema(id, &c).await;
                }
                Ok(Err(e)) => {
                    self.record(e);
                    tokio::time::sleep(Duration::from_millis(self.cfg.anti_entropy_ms)).await;
                }
                Err(_) => {
                    self.record("pull deadline");
                    c.close(1u32.into(), b"pull deadline");
                    break;
                }
            }
        }
    }
    /// Pull with one schema-adopt retry: when the donor rejects us on
    /// schema mismatch and adoption is enabled, ingest its manifest and
    /// retry once against the adopted schema.
    async fn pull(&self, id: NodeId, c: &quinn::Connection) -> Result<bool> {
        match self.pull_once(id, c).await {
            Err(Error::Protocol(e)) if e.contains("schema mismatch") => {
                let accept = self
                    .worker
                    .run(|s| Ok(schema::params_from(&s.db)?.accept_remote_schema))
                    .await?;
                if !accept {
                    return Err(Error::Protocol(e));
                }
                self.adopt_from_peer(id, c).await?;
                self.pull_once(id, c).await
            }
            other => other,
        }
    }
    async fn pull_once(&self, id: NodeId, c: &quinn::Connection) -> Result<bool> {
        let (expected, cursor, schema) = self
            .worker
            .run(move |s| {
                s.flush()?;
                let (site, cursor) = s.progress(id)?;
                Ok((site, cursor, s.schema()?))
            })
            .await?;
        let (mut send, mut recv) = c
            .open_bi()
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        protocol::write(
            &mut send,
            &Message::Pull {
                group: self.cfg.group,
                node: self.cfg.node,
                site: self.cfg.site,
                schema,
                expected_site: expected,
                cursor: cursor.clone(),
            },
        )
        .await?;
        match protocol::read(&mut recv).await? {
            Message::Idle => Ok(false),
            Message::Reset { site } => {
                if site == self.cfg.site {
                    return Err(Error::Auth("remote cloned local site identity".into()));
                }
                self.worker
                    .run_admitted(
                        murmur_sched::WriterClass::Remote,
                        ADMISSION_TIMEOUT,
                        move |s| s.reset(id, site),
                    )
                    .await?;
                Ok(true)
            }
            Message::Batch {
                site,
                cursor: next,
                bytes,
                digest,
            } => {
                if Some(site) != expected
                    || site == self.cfg.site
                    || bytes < 12
                    || bytes > self.cfg.max_batch_bytes as u64
                {
                    return Err(Error::Protocol("batch identity or length".into()));
                }
                if next.from < cursor.from
                    || next.through.is_some_and(|h| h < next.from)
                    || next
                        .after
                        .as_ref()
                        .is_some_and(|k| Some(k.version) > next.through)
                {
                    return Err(Error::Protocol("batch cursor".into()));
                }
                // Batch byte permit: spans stage_begin/stage/apply jobs, so it
                // stays on the caller rather than moving into one job.
                // Single-job payload permits arrive with a reviewed
                // shared payload budget (follow-up).
                let _permit = self
                    .receiving
                    .clone()
                    .acquire_many_owned(bytes as u32)
                    .await
                    .map_err(|_| Error::Overloaded)?;
                let cfg = self.cfg.clone();
                self.worker
                    .run_admitted(
                        murmur_sched::WriterClass::Remote,
                        ADMISSION_TIMEOUT,
                        move |s| s.stage_begin(id, &cfg),
                    )
                    .await?;
                let mut received = 0usize;
                let mut index = 0u32;
                loop {
                    match protocol::read(&mut recv).await? {
                        Message::Chunk { index: i, data }
                            if i == index
                                && !data.is_empty()
                                && data.len() <= protocol::CHUNK_BYTES =>
                        {
                            if received + data.len() > bytes as usize {
                                return Err(Error::Protocol("excess chunk bytes".into()));
                            }
                            received += data.len();
                            let budget = self.cfg.staging_bytes;
                            self.worker
                                .run_admitted(
                                    murmur_sched::WriterClass::Remote,
                                    ADMISSION_TIMEOUT,
                                    move |s| s.stage(id, index, data, budget),
                                )
                                .await?;
                            index += 1;
                        }
                        Message::End => break,
                        _ => {
                            return Err(Error::Protocol("expected sequential batch chunks".into()));
                        }
                    }
                }
                if received != bytes as usize {
                    return Err(Error::Protocol("incomplete batch".into()));
                }
                let more = next.through.is_some();
                let changed = next != cursor;
                self.worker
                    .run_admitted(
                        murmur_sched::WriterClass::Remote,
                        ADMISSION_TIMEOUT,
                        move |s| s.apply(id, site, cursor, next, bytes as usize, digest),
                    )
                    .await?;
                // Liveness for retention floors (inbound batches carry
                // no new ack knowledge about the sender).
                Self::touch_peer(&self.worker, &self.cfg, id, Vec::new()).await;
                protocol::write(&mut send, &Message::Ack { digest }).await?;
                let _ = send.finish();
                if changed {
                    self.applied.fetch_add(1, Ordering::Relaxed);
                }
                Ok(more)
            }
            Message::Error(e) => Err(Error::Protocol(e)),
            _ => Err(Error::Protocol("expected batch header".into())),
        }
    }
    async fn serve_pull(
        &self,
        id: NodeId,
        c: &quinn::Connection,
        send: &mut quinn::SendStream,
        recv: &mut quinn::RecvStream,
        group: NodeId,
        node: NodeId,
        site: NodeId,
        schema: [u8; 32],
        expected_site: Option<NodeId>,
        cursor: protocol::Cursor,
    ) -> Result<()> {
        let _session = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        if group != self.cfg.group || node != id || site == self.cfg.site {
            return Err(Error::Auth("replication group or identity mismatch".into()));
        }
        let local_schema = self.worker.run(|s| s.schema()).await?;
        if local_schema != schema && !self.compat_or_spawn(id, c, schema) {
            return Err(Error::Protocol(
                "CRR schema mismatch; provision matching table definitions".into(),
            ));
        }
        if expected_site != Some(self.cfg.site) {
            protocol::write(
                send,
                &Message::Reset {
                    site: self.cfg.site,
                },
            )
            .await?;
            return Ok(());
        }
        // Export byte budget: retained through export + send/ACK, so it
        // stays on the caller rather than moving into the export job.
        let _budget = self
            .serving
            .clone()
            .acquire_many_owned(self.cfg.max_batch_bytes as u32)
            .await
            .map_err(|_| Error::Overloaded)?;
        let cfg = self.cfg.clone();
        let previous = cursor.clone();
        // GC watermark: cursors older than pruned history fail loud so
        // the peer snapshots instead of converging through a silent gap.
        let watermark = self.worker.run(|s| s.gc_watermark()).await?;
        // Tombstones at or below the watermark are gone; a cursor below
        // it missed deletes. Equality is safe (nothing pruned above).
        if (previous.from.max(0) as u64) < watermark {
            return Err(Error::Protocol("cursor pruned; snapshot required".into()));
        }
        let (next, data) = self.worker.run(move |s| s.export(cursor, &cfg)).await?;
        if next == previous && data.len() == 12 {
            protocol::write(send, &Message::Idle).await?;
            return Ok(());
        }
        let digest = protocol::digest(&data);
        protocol::write(
            send,
            &Message::Batch {
                site: self.cfg.site,
                cursor: next.clone(),
                bytes: data.len() as u64,
                digest,
            },
        )
        .await?;
        for (index, chunk) in data.chunks(protocol::CHUNK_BYTES).enumerate() {
            protocol::write(
                send,
                &Message::Chunk {
                    index: index as u32,
                    data: chunk.to_vec(),
                },
            )
            .await?;
        }
        protocol::write(send, &Message::End).await?;
        match protocol::read(recv).await? {
            Message::Ack { digest: d } if d == digest => {
                // The puller applied our data through its request cursor:
                // record it as observed self-progress for waits/retention.
                // Idle pulls skip this (a worker round trip per poll).
                let acked = previous.from.max(0) as u64;
                let acked_through = next.through.unwrap_or(next.from).max(0) as u64;
                let local = self.cfg.node;
                self.worker
                    .run_admitted(
                        murmur_sched::WriterClass::Remote,
                        ADMISSION_TIMEOUT,
                        move |s| {
                            s.db.execute(
                                "UPDATE murmur_progress SET sent=?1 WHERE node=?2",
                                rusqlite::params![encode(&next)?, id.as_slice()],
                            )?;
                            s.observed_max(local, acked)?;
                            s.flush()
                        },
                    )
                    .await?;
                Self::touch_peer(
                    &self.worker,
                    &self.cfg,
                    id,
                    vec![murmur_tx::protocol::Progress {
                        origin: local,
                        applied: acked_through,
                        observed: acked_through,
                        retained_from: 0,
                        retained_through: 0,
                    }],
                )
                .await;
                self.sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            _ => Err(Error::Protocol("batch acknowledgement mismatch".into())),
        }
    }
    /// Donor side: build a snapshot for the requesting peer and stream
    /// OUT-kv chunks. Serves any authenticated group member; the
    /// recipient decides trust. No serving byte budget: a snapshot can
    /// exceed the byte semaphore's capacity (acquiring over capacity
    /// would wedge forever), so snapshot_bytes + the sessions cap bound
    /// it instead.
    async fn serve_snapshot(
        &self,
        id: NodeId,
        c: &quinn::Connection,
        send: &mut quinn::SendStream,
        _recv: &mut quinn::RecvStream,
        group: NodeId,
        node: NodeId,
        site: NodeId,
        schema: [u8; 32],
    ) -> Result<()> {
        let _session = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        if group != self.cfg.group || node != id || site == self.cfg.site {
            return Err(Error::Auth(
                "replication group or identity mismatch".into(),
            ));
        }
        let local_schema = self.worker.run(|s| s.schema()).await?;
        if local_schema != schema && !self.compat_or_spawn(id, c, schema) {
            return Err(Error::Protocol(
                "CRR schema mismatch; provision matching table definitions".into(),
            ));
        }
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        let manifest = tokio::task::spawn_blocking(move || {
            let backend = CrrBackend::new(worker.clone(), cfg.group, cfg.node, cfg.clone());
            let params = worker.call(|s| snapshot::params_from(&s.db))?;
            snapshot::build_for(
                &worker,
                &backend,
                cfg.clone(),
                &params,
                id,
                murmur_tx::retention::now_millis(),
            )
            .map_err(crate::wait::backend_err)
        })
        .await
        .map_err(|e| Error::Unavailable(format!("snapshot build: {e}")))??;
        let encoded = murmur_tx::encode(&manifest).map_err(crate::wait::backend_err)?;
        protocol::write(send, &Message::SnapshotManifest { manifest: encoded }).await?;
        for (index, _) in manifest.chunks.iter().enumerate() {
            let worker = self.worker.clone();
            let cfg = self.cfg.clone();
            let transfer = manifest.transfer;
            let chunk = tokio::task::spawn_blocking(move || {
                let backend =
                    CrrBackend::new(worker, cfg.group, cfg.node, cfg);
                backend.get(&murmur_tx::snapshot::chunk(
                    murmur_tx::snapshot::OUT,
                    transfer,
                    index as u32,
                ))
            })
            .await
            .map_err(|e| Error::Unavailable(format!("snapshot serve: {e}")))?
            .map_err(crate::wait::backend_err)?;
            let Some(data) = chunk else {
                return Err(Error::Protocol("snapshot chunk expired".into()));
            };
            protocol::write(
                send,
                &Message::Chunk {
                    index: index as u32,
                    data,
                },
            )
            .await?;
        }
        protocol::write(send, &Message::End).await?;
        self.sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    /// Queue a snapshot pull from a connected peer. Returns once queued;
    /// progress lands in `snapshot_status` (surfaced by status()).
    pub fn snapshot_request(live: &Arc<Live>, node: NodeId) -> Result<()> {
        {
            let mut status = live.snapshot_status.lock().unwrap();
            if *status == Some("running".to_string()) {
                return Err(Error::Unavailable("snapshot already running".into()));
            }
            *status = Some("running".to_string());
        }
        let this = live.clone();
        live.rt.spawn(async move {
            let outcome = this.fetch_snapshot(node).await;
            let mut status = this.snapshot_status.lock().unwrap();
            *status = Some(match outcome {
                Ok(transfer) => format!("ok {}", hex(&transfer)),
                Err(e) => {
                    this.record(&e);
                    format!("error: {e}")
                }
            });
        });
        Ok(())
    }
    /// Recipient side: request, stage, and install one snapshot, then
    /// resume normal pulls (the manifest boundary advances progress
    /// heads, so the next pull continues past the snapshot).
    async fn fetch_snapshot(self: &Arc<Self>, id: NodeId) -> Result<NodeId> {
        let c = self
            .peers
            .lock()
            .unwrap()
            .get(&id)
            .map(|p| p.connection.clone())
            .ok_or_else(|| Error::Unavailable("snapshot peer is not connected".into()))?;
        let schema = self.worker.run(|s| s.schema()).await?;
        let (mut send, mut recv) = c
            .open_bi()
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        protocol::write(
            &mut send,
            &Message::SnapshotRequest {
                group: self.cfg.group,
                node: self.cfg.node,
                site: self.cfg.site,
                schema,
            },
        )
        .await?;
        let bytes = match protocol::read(&mut recv).await? {
            Message::SnapshotManifest { manifest } => manifest,
            Message::Error(e) => return Err(Error::Protocol(e)),
            _ => return Err(Error::Protocol("expected snapshot manifest".into())),
        };
        let manifest: murmur_tx::snapshot::Manifest =
            murmur_tx::decode(&bytes).map_err(|e| Error::Protocol(e.to_string()))?;
        let transfer = manifest.transfer;
        let params = self.worker.run(|s| snapshot::params_from(&s.db)).await?;
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        let accepted = manifest.clone();
        let params_accept = params.clone();
        tokio::task::spawn_blocking(move || {
            let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
            snapshot::accept(
                &backend,
                &params_accept,
                id,
                accepted,
                murmur_tx::retention::now_millis(),
            )
        })
        .await
        .map_err(|e| Error::Unavailable(format!("snapshot accept: {e}")))?
        .map_err(crate::wait::backend_err)?;
        let mut index = 0u32;
        loop {
            match protocol::read(&mut recv).await? {
                Message::Chunk { index: i, data }
                    if i == index
                        && !data.is_empty()
                        && data.len() <= protocol::CHUNK_BYTES =>
                {
                    let worker = self.worker.clone();
                    let cfg = self.cfg.clone();
                    tokio::task::spawn_blocking(move || {
                        let backend =
                            CrrBackend::new(worker, cfg.group, cfg.node, cfg);
                        backend.commit(vec![murmur_tx::Record::put(
                            murmur_tx::snapshot::chunk(murmur_tx::snapshot::IN, transfer, i),
                            data,
                        )])
                    })
                    .await
                    .map_err(|e| Error::Unavailable(format!("snapshot stage: {e}")))?
                    .map_err(crate::wait::backend_err)?;
                    index += 1;
                }
                Message::End => break,
                Message::Error(e) => return Err(Error::Protocol(e)),
                _ => {
                    return Err(Error::Protocol(
                        "expected sequential snapshot chunks".into(),
                    ));
                }
            }
        }
        let _ = send.finish();
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
            for _ in 0..600 {
                let now = murmur_tx::retention::now_millis();
                let key = murmur_tx::snapshot::header(murmur_tx::snapshot::IN, transfer);
                let Some(b) = backend.get(&key).map_err(crate::wait::backend_err)? else {
                    return Ok(());
                };
                let incoming: murmur_tx::snapshot::Incoming =
                    murmur_tx::decode(&b).map_err(|e| Error::Protocol(e.to_string()))?;
                if !murmur_tx::snapshot::missing(&backend, &incoming)
                    .map_err(crate::wait::backend_err)?
                    .is_empty()
                {
                    return Err(Error::Protocol(
                        "snapshot donor short-sent chunks".into(),
                    ));
                }
                if snapshot::finish(&backend, &params, id, transfer, now)
                    .map_err(crate::wait::backend_err)?
                {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(Error::Unavailable(
                "snapshot install did not complete".into(),
            ))
        })
        .await
        .map_err(|e| Error::Unavailable(format!("snapshot install: {e}")))??;
        Self::touch_peer(&self.worker, &self.cfg, id, manifest.progress.clone()).await;
        self.applied.fetch_add(1, Ordering::Relaxed);
        Ok(transfer)
    }
    /// Serve our schema manifest to any peer. The requester decides
    /// adoption; non-master manifests are harmless (gated adopters
    /// record them without applying).
    /// Donor-side vet for a digest-mismatched puller: serve from a
    /// cached verdict, else spawn one background vet round trip and
    /// reject this pull (the puller's retry converges once the verdict
    /// lands). The fetch MUST stay out of the serve path: the session
    /// loop serves streams sequentially, so a blocking reverse
    /// SchemaRequest deadlocks when both peers mismatch at once. A
    /// true verdict means puller-local extra tables only (drop-never
    /// legacy) — without it, a follower that kept a table the donor
    /// dropped can never pull again, and no puller-side retry can fix
    /// a donor-side digest check. Verdicts cache per (peer, digest);
    /// any schema change flips the digest and re-vets. No
    /// digest-bind: the hello digest covers unfiltered bytes while
    /// manifests exclude opt-outs, so the envelope's self-consistency
    /// (signer/database) is the check — a lying peer only earns
    /// itself un-appliable rows.
    fn compat_or_spawn(&self, id: NodeId, c: &quinn::Connection, claimed: [u8; 32]) -> bool {
        if let Some(compat) = self.vetted.lock().unwrap().get(&(id, claimed)).copied() {
            return compat;
        }
        let worker = self.worker.clone();
        let vetted = self.vetted.clone();
        let group = self.cfg.group;
        let c = c.clone();
        let (ok, no, fail) = (self.vet_ok.clone(), self.vet_no.clone(), self.vet_fail.clone());
        tokio::spawn(async move {
            let verdict = Self::vet_fetch(&worker, group, id, &c).await;
            match verdict {
                Some(true) => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    vetted.lock().unwrap().insert((id, claimed), true);
                }
                Some(false) => {
                    no.fetch_add(1, Ordering::Relaxed);
                    vetted.lock().unwrap().insert((id, claimed), false);
                }
                None => {
                    fail.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        false
    }
    /// One vet round trip. None on transport failure (re-vet next
    /// pull); any received verdict is deterministic per digest and
    /// cacheable, including envelope and decode failures.
    async fn vet_fetch(
        worker: &Arc<Worker>,
        group: NodeId,
        id: NodeId,
        c: &quinn::Connection,
    ) -> Option<bool> {
        let (mut send, mut recv) = c.open_bi().await.ok()?;
        protocol::write(&mut send, &Message::SchemaRequest).await.ok()?;
        let bytes = match protocol::read(&mut recv).await.ok()? {
            Message::SchemaManifest { manifest } => manifest,
            _ => return None,
        };
        let _ = send.finish();
        let signed: murmur_tx::protocol::SignedSchema = murmur_tx::decode(&bytes).ok()?;
        if signed.signer != id || signed.database != group {
            return Some(false);
        }
        let local = worker.run(|s| s.manifest_schema_bytes()).await.ok()?;
        schema::puller_compatible(&local, &signed.schema).ok()
    }
    async fn serve_schema_request(
        &self,
        _id: NodeId,
        send: &mut quinn::SendStream,
    ) -> Result<()> {
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        let manifest = tokio::task::spawn_blocking(move || -> Result<murmur_tx::protocol::SignedSchema> {
            let backend = CrrBackend::new(worker.clone(), cfg.group, cfg.node, cfg.clone());
            let params = worker.call(|s| schema::params_from(&s.db))?;
            let bytes = worker.call(|s| s.manifest_schema_bytes())?;
            let (database, node) = backend.identity();
            schema::build_manifest(&backend, &params, node, database, bytes)
                .map_err(crate::wait::backend_err)
        })
        .await
        .map_err(|e| Error::Unavailable(format!("schema manifest: {e}")))??;
        let encoded = murmur_tx::encode(&manifest).map_err(crate::wait::backend_err)?;
        protocol::write(send, &Message::SchemaManifest { manifest: encoded }).await?;
        Ok(())
    }
    /// Vet one manifest from an authenticated peer (push or requested):
    /// check, adopt additive changes, record. Shared by both paths.
    async fn ingest_manifest(&self, id: NodeId, bytes: &[u8]) -> Result<murmur_tx::protocol::SignedSchema> {
        let signed: murmur_tx::protocol::SignedSchema =
            murmur_tx::decode(bytes).map_err(|e| Error::Protocol(e.to_string()))?;
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        let params = self.worker.run(|s| schema::params_from(&s.db)).await?;
        let local = self.worker.run(|s| s.manifest_schema_bytes()).await?;
        let vetted = signed.clone();
        let params_vet = params.clone();
        let decision = tokio::task::spawn_blocking(move || {
            let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
            let own = backend.identity().1;
            schema::check_manifest(&backend, &params_vet, own, &local, &vetted, id)
                .map_err(crate::wait::backend_err)
        })
        .await
        .map_err(|e| Error::Unavailable(format!("schema vet: {e}")))??;
        if decision == schema::SchemaDecision::Adopt {
            let worker = self.worker.clone();
            let cfg = self.cfg.clone();
            let schema_bytes = signed.schema.clone();
            let applied = tokio::task::spawn_blocking(move || {
                let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
                backend.accept_schema(schema_bytes)
            })
            .await
            .map_err(|e| Error::Unavailable(format!("schema adopt: {e}")))?
            .map_err(crate::wait::backend_err)?;
            if applied {
                let worker = self.worker.clone();
                let cfg = self.cfg.clone();
                let recorded = signed.clone();
                tokio::task::spawn_blocking(move || {
                    let backend = CrrBackend::new(worker, cfg.group, cfg.node, cfg);
                    schema::record_accepted(&backend, &recorded)
                })
                .await
                .map_err(|e| Error::Unavailable(format!("schema record: {e}")))?
                .map_err(crate::wait::backend_err)?;
                self.schema_adoptions.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(signed)
    }
    /// Handle an unsolicited master push: ingest, then ack everything
    /// vetted (Stale/Current/Record/Adopt all ack; only vet failures
    /// answer with Error via the session wrapper).
    async fn receive_schema_push(
        &self,
        id: NodeId,
        send: &mut quinn::SendStream,
        bytes: Vec<u8>,
    ) -> Result<()> {
        let signed = self.ingest_manifest(id, &bytes).await?;
        protocol::write(
            send,
            &Message::SchemaAck {
                digest: signed.digest(),
                sequence: signed.sequence,
            },
        )
        .await?;
        Ok(())
    }
    /// Fetch the peer's manifest, ingest it, ack. Called when a pull
    /// fails on schema mismatch (and only when adoption is enabled).
    async fn adopt_from_peer(&self, id: NodeId, c: &quinn::Connection) -> Result<()> {
        let (mut send, mut recv) = c
            .open_bi()
            .await
            .map_err(|e| Error::Unavailable(e.to_string()))?;
        protocol::write(&mut send, &Message::SchemaRequest).await?;
        let bytes = match protocol::read(&mut recv).await? {
            Message::SchemaManifest { manifest } => manifest,
            Message::Error(e) => return Err(Error::Protocol(e)),
            _ => return Err(Error::Protocol("expected schema manifest".into())),
        };
        let signed = self.ingest_manifest(id, &bytes).await?;
        protocol::write(
            &mut send,
            &Message::SchemaAck {
                digest: signed.digest(),
                sequence: signed.sequence,
            },
        )
        .await?;
        let _ = send.finish();
        Ok(())
    }
    /// Master push after each pull: send our manifest when the peer has
    /// not acked it yet. Best-effort; failures stay silent (the next
    /// pull re-discovers drift through its mismatch path).
    async fn push_schema(&self, id: NodeId, c: &quinn::Connection) {
        let params = match self.worker.run(|s| schema::params_from(&s.db)).await {
            Ok(p) if p.master => p,
            _ => return,
        };
        let worker = self.worker.clone();
        let cfg = self.cfg.clone();
        let manifest = match tokio::task::spawn_blocking(move || -> Result<murmur_tx::protocol::SignedSchema> {
            let backend = CrrBackend::new(worker.clone(), cfg.group, cfg.node, cfg.clone());
            let bytes = worker.call(|s| s.manifest_schema_bytes())?;
            let (database, node) = backend.identity();
            schema::build_manifest(&backend, &params, node, database, bytes)
                .map_err(crate::wait::backend_err)
        })
        .await
        {
            Ok(Ok(m)) => m,
            _ => return,
        };
        let key = (manifest.digest(), manifest.sequence);
        if self
            .peers
            .lock()
            .unwrap()
            .get(&id)
            .is_some_and(|p| p.schema_sent == Some(key))
        {
            return;
        }
        let encoded = match murmur_tx::encode(&manifest) {
            Ok(b) => b,
            Err(_) => return,
        };
        let (mut send, mut recv) = match c.open_bi().await {
            Ok(s) => s,
            Err(_) => return,
        };
        if protocol::write(&mut send, &Message::SchemaManifest { manifest: encoded })
            .await
            .is_err()
        {
            return;
        }
        if let Ok(Message::SchemaAck { digest, sequence }) = protocol::read(&mut recv).await
            && (digest, sequence) == key
        {
            if let Some(peer) = self.peers.lock().unwrap().get_mut(&id) {
                peer.schema_sent = Some(key);
            }
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        let _ = send.finish();
    }
    fn broadcast(&self, packet: Packet) {
        if let Ok(data) = encode(&packet)
            && data.len() <= 1000
        {
            use rand::seq::IteratorRandom;
            for peer in self
                .peers
                .lock()
                .unwrap()
                .values()
                .choose_multiple(&mut rand::thread_rng(), self.cfg.gossip_fanout)
            {
                let _ = peer.connection.send_datagram(data.clone().into());
            }
        }
    }
    fn gossip(&self) {
        let members = self.view.lock().unwrap().gossip();
        self.broadcast(Packet::Gossip {
            database: self.cfg.group,
            sender: self.cfg.node,
            members,
        });
    }
    fn probe(&self) {
        let lost = self.view.lock().unwrap().expire();
        use rand::seq::IteratorRandom;
        let target = self
            .peers
            .lock()
            .unwrap()
            .keys()
            .copied()
            .choose(&mut rand::thread_rng());
        if let Some(target) = target {
            let nonce = rand::random();
            let members = {
                let mut v = self.view.lock().unwrap();
                v.pending
                    .entry(target)
                    .or_insert((std::time::Instant::now(), nonce, false));
                v.gossip()
            };
            self.send_packet(
                target,
                Packet::Ping {
                    database: self.cfg.group,
                    sender: self.cfg.node,
                    nonce,
                    members,
                },
            );
        }
        for (target, nonce) in lost {
            let peers: Vec<_> = self
                .peers
                .lock()
                .unwrap()
                .keys()
                .filter(|id| **id != target)
                .take(3)
                .copied()
                .collect();
            for id in peers {
                self.send_packet(
                    id,
                    Packet::PingRequest {
                        database: self.cfg.group,
                        sender: self.cfg.node,
                        target,
                        nonce,
                    },
                );
            }
        }
    }
    fn send_packet(&self, id: NodeId, packet: Packet) {
        if let Ok(data) = encode(&packet)
            && data.len() <= 1000
            && let Some(p) = self.peers.lock().unwrap().get(&id)
        {
            let _ = p.connection.send_datagram(data.into());
        }
    }
    async fn packet(&self, id: NodeId, bytes: &[u8]) -> Result<()> {
        if bytes.len() > 1000 {
            return Err(Error::Protocol("membership datagram length".into()));
        }
        let packet: Packet = decode(bytes)?;
        let db = self.cfg.group;
        let local = self.cfg.node;
        match packet {
            Packet::Gossip {
                database,
                sender,
                members,
            }
            | Packet::Ping {
                database,
                sender,
                members,
                nonce: 0,
            } => {
                if database != db || sender != id || members.len() > 4 {
                    return Err(Error::Auth("membership envelope".into()));
                }
                let mut view = self.view.lock().unwrap();
                for m in members {
                    view.merge(m);
                }
            }
            Packet::Ping {
                database,
                sender,
                nonce,
                members,
            }
            | Packet::Ack {
                database,
                sender,
                nonce,
                members,
            } => {
                if database != db || sender != id || members.len() > 4 {
                    return Err(Error::Auth("membership envelope".into()));
                }
                let reply = {
                    let mut view = self.view.lock().unwrap();
                    for m in members {
                        view.merge(m);
                    }
                    view.acknowledged(id, nonce);
                    view.gossip()
                };
                let requester = self.relays.lock().unwrap().remove(&(id, nonce));
                if let Some((requester, _)) = requester {
                    self.send_packet(
                        requester,
                        Packet::IndirectAck {
                            database: db,
                            sender: local,
                            target: id,
                            nonce,
                        },
                    );
                }
                if matches!(decode::<Packet>(bytes)?, Packet::Ping { .. }) {
                    self.send_packet(
                        id,
                        Packet::Ack {
                            database: db,
                            sender: local,
                            nonce,
                            members: reply,
                        },
                    );
                }
            }
            Packet::IndirectAck {
                database,
                sender,
                target,
                nonce,
            } => {
                if database != db || sender != id {
                    return Err(Error::Auth("indirect ACK envelope".into()));
                }
                self.view.lock().unwrap().acknowledged(target, nonce);
            }
            Packet::PingRequest {
                database,
                sender,
                target,
                nonce,
            } => {
                if database != db || sender != id {
                    return Err(Error::Auth("probe envelope".into()));
                }
                {
                    let mut relays = self.relays.lock().unwrap();
                    relays.retain(|_, (_, at)| at.elapsed() < Duration::from_secs(5));
                    if relays.len() >= 4096 {
                        return Err(Error::Overloaded);
                    }
                    relays.insert((target, nonce), (id, std::time::Instant::now()));
                }
                let members = self.view.lock().unwrap().gossip();
                self.send_packet(
                    target,
                    Packet::Ping {
                        database: db,
                        sender: local,
                        nonce,
                        members,
                    },
                );
            }
        }
        let incarnation = self.view.lock().unwrap().local.incarnation;
        if incarnation > self.persisted_incarnation.load(Ordering::Relaxed) {
            self.worker
                .run_admitted(
                    murmur_sched::WriterClass::Remote,
                    ADMISSION_TIMEOUT,
                    move |s| {
                        s.db.execute(
                            "UPDATE murmur_identity SET incarnation=max(incarnation,?1) WHERE id=1",
                            [incarnation as i64],
                        )?;
                        s.flush()
                    },
                )
                .await?;
            self.persisted_incarnation
                .fetch_max(incarnation, Ordering::Relaxed);
        }
        Ok(())
    }
}
