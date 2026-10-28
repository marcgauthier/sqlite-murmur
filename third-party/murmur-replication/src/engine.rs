//! Connection/session orchestration adapted from Murmur, with CRR pull windows.
use crate::{
    membership::{Member, Packet, State, View},
    protocol::{self, Message},
    storage::Worker,
    *,
};
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
}
pub struct Live {
    pub cfg: Config,
    worker: Arc<Worker>,
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
    stopped: AtomicBool,
    persisted_incarnation: std::sync::atomic::AtomicU64,
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
                    let live = Arc::new(Live {
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
                        stopped: AtomicBool::new(false),
                        persisted_incarnation: cfg.incarnation.into(),
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
        json!({"running":!self.stopped.load(Ordering::Relaxed),"node_id":hex(&self.cfg.node),"site_id":hex(&self.cfg.site),"group_id":hex(&self.cfg.group),"bind":self.endpoint.local_addr().ok().map(|a|a.to_string()),"connected_peers":self.peers.lock().unwrap().len(),"applied_batches":self.applied.load(Ordering::Relaxed),"sent_batches":self.sent.load(Ordering::Relaxed),"last_error":self.error.lock().unwrap().clone()})
    }
    pub fn members(&self) -> Vec<serde_json::Value> {
        let peers = self.peers.lock().unwrap();
        let view = self.view.lock().unwrap();
        view.members.values().map(|m|json!({"node_id":hex(&m.node),"address":m.address.to_string(),"state":format!("{:?}",m.state).to_lowercase(),"connected":peers.contains_key(&m.node),"incarnation":m.incarnation})).collect()
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
            let result = tokio::time::timeout(
                Duration::from_secs(60),
                self.serve(id, &mut send, &mut recv),
            )
            .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    self.record(&e);
                    let _ = protocol::write(&mut send, &Message::Error(e.to_string())).await;
                }
                Err(_) => {
                    self.record("serve deadline");
                    let _ = send.reset(1u32.into());
                }
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
        if schema != remote_schema {
            return Err(Error::Protocol(
                "CRR schema mismatch; provision matching table definitions".into(),
            ));
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
    async fn pull(&self, id: NodeId, c: &quinn::Connection) -> Result<bool> {
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
                self.worker.run(move |s| s.reset(id, site)).await?;
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
                let _permit = self
                    .receiving
                    .clone()
                    .acquire_many_owned(bytes as u32)
                    .await
                    .map_err(|_| Error::Overloaded)?;
                let cfg = self.cfg.clone();
                self.worker.run(move |s| s.stage_begin(id, &cfg)).await?;
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
                                .run(move |s| s.stage(id, index, data, budget))
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
                    .run(move |s| s.apply(id, site, cursor, next, bytes as usize, digest))
                    .await?;
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
    async fn serve(
        &self,
        id: NodeId,
        send: &mut quinn::SendStream,
        recv: &mut quinn::RecvStream,
    ) -> Result<()> {
        let _session = self
            .sessions
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Overloaded)?;
        let Message::Pull {
            group,
            node,
            site,
            schema,
            expected_site,
            cursor,
        } = protocol::read(recv).await?
        else {
            return Err(Error::Protocol("expected pull request".into()));
        };
        if group != self.cfg.group || node != id || site == self.cfg.site {
            return Err(Error::Auth("replication group or identity mismatch".into()));
        }
        let local_schema = self.worker.run(|s| s.schema()).await?;
        if local_schema != schema {
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
        let _budget = self
            .serving
            .clone()
            .acquire_many_owned(self.cfg.max_batch_bytes as u32)
            .await
            .map_err(|_| Error::Overloaded)?;
        let cfg = self.cfg.clone();
        let previous = cursor.clone();
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
                self.worker
                    .run(move |s| {
                        s.db.execute(
                            "UPDATE murmur_progress SET sent=?1 WHERE node=?2",
                            rusqlite::params![encode(&next)?, id.as_slice()],
                        )?;
                        s.flush()
                    })
                    .await?;
                self.sent.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            _ => Err(Error::Protocol("batch acknowledgement mismatch".into())),
        }
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
                .choose_multiple(&mut rand::thread_rng(), 3)
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
                .run(move |s| {
                    s.db.execute(
                        "UPDATE murmur_identity SET incarnation=max(incarnation,?1) WHERE id=1",
                        [incarnation as i64],
                    )?;
                    s.flush()
                })
                .await?;
            self.persisted_incarnation
                .fetch_max(incarnation, Ordering::Relaxed);
        }
        Ok(())
    }
}
