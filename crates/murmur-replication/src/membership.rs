//! Bounded SWIM state. Relayed identities are discovery hints, never credentials.
use crate::*;
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum State {
    Alive,
    Suspect,
    Dead,
    Left,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Member {
    pub node: NodeId,
    pub address: SocketAddr,
    pub incarnation: u64,
    pub state: State,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Packet {
    Gossip {
        database: NodeId,
        sender: NodeId,
        members: Vec<Member>,
    },
    IndirectAck {
        database: NodeId,
        sender: NodeId,
        target: NodeId,
        nonce: u64,
    },
    Ping {
        database: NodeId,
        sender: NodeId,
        nonce: u64,
        members: Vec<Member>,
    },
    Ack {
        database: NodeId,
        sender: NodeId,
        nonce: u64,
        members: Vec<Member>,
    },
    PingRequest {
        database: NodeId,
        sender: NodeId,
        target: NodeId,
        nonce: u64,
    },
}
pub struct View {
    pub members: BTreeMap<NodeId, Member>,
    pub local: Member,
    pub excluded: BTreeSet<NodeId>,
    pub pending: BTreeMap<NodeId, (std::time::Instant, u64, bool)>,
    pub suspect: BTreeMap<NodeId, std::time::Instant>,
    pub awareness: u32,
}
impl View {
    pub fn new(local: Member) -> Self {
        Self {
            members: BTreeMap::new(),
            local,
            excluded: BTreeSet::new(),
            pending: BTreeMap::new(),
            suspect: BTreeMap::new(),
            awareness: 0,
        }
    }
    pub fn merge(&mut self, member: Member) {
        if member.node == self.local.node {
            if member.incarnation >= self.local.incarnation && member.state != State::Alive {
                self.local.incarnation = member.incarnation.saturating_add(1);
            }
            return;
        }
        if self.excluded.contains(&member.node)
            || self.members.len() >= 4096 && !self.members.contains_key(&member.node)
        {
            return;
        }
        let rank = |s: State| match s {
            State::Alive => 0,
            State::Suspect => 1,
            State::Dead => 2,
            State::Left => 3,
        };
        if self.members.get(&member.node).is_none_or(|old| {
            member.incarnation > old.incarnation
                || member.incarnation == old.incarnation && rank(member.state) > rank(old.state)
        }) {
            if member.state == State::Alive {
                self.suspect.remove(&member.node);
            }
            self.members.insert(member.node, member);
        }
    }
    pub fn gossip(&self) -> Vec<Member> {
        let mut out = vec![self.local.clone()];
        use rand::seq::IteratorRandom;
        out.extend(
            self.members
                .values()
                .choose_multiple(&mut rand::thread_rng(), 3)
                .into_iter()
                .cloned(),
        );
        out
    }
    pub fn acknowledged(&mut self, node: NodeId, nonce: u64) {
        if self.pending.get(&node).is_some_and(|(_, n, _)| *n == nonce) {
            self.pending.remove(&node);
            self.awareness = self.awareness.saturating_sub(1);
        }
    }
    pub fn expire(&mut self) -> Vec<(NodeId, u64)> {
        let now = std::time::Instant::now();
        let timeout = Duration::from_millis(500 * (self.awareness as u64 + 1));
        let lost: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, (at, _, _))| now.duration_since(*at) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        let mut indirect = Vec::new();
        for id in &lost {
            let pending = self.pending.get_mut(id).unwrap();
            if !pending.2 {
                pending.0 = now;
                pending.2 = true;
                indirect.push((*id, pending.1));
            } else {
                self.pending.remove(id);
                if let Some(member) = self.members.get_mut(id) {
                    member.state = State::Suspect;
                    self.suspect.entry(*id).or_insert(now);
                }
                self.awareness = (self.awareness + 1).min(8);
            }
        }
        let expiry = Duration::from_secs(5 * (self.awareness as u64 + 1));
        let dead: Vec<_> = self
            .suspect
            .iter()
            .filter(|(_, at)| now.duration_since(**at) >= expiry)
            .map(|(id, _)| *id)
            .collect();
        for id in dead {
            if let Some(m) = self.members.get_mut(&id) {
                m.state = State::Dead;
            }
            self.suspect.remove(&id);
        }
        indirect
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn member(node: u8, incarnation: u64, state: State) -> Member {
        Member {
            node: [node; 16],
            address: "127.0.0.1:7443".parse().unwrap(),
            incarnation,
            state,
        }
    }
    #[test]
    fn stale_gossip_cannot_revive_and_excluded_nodes_stay_excluded() {
        let mut view = View::new(member(1, 1, State::Alive));
        view.merge(member(2, 3, State::Dead));
        view.merge(member(2, 2, State::Alive));
        assert_eq!(view.members[&[2; 16]].state, State::Dead);
        view.merge(member(2, 4, State::Alive));
        assert_eq!(view.members[&[2; 16]].state, State::Alive);
        view.excluded.insert([3; 16]);
        view.merge(member(3, 9, State::Alive));
        assert!(!view.members.contains_key(&[3; 16]));
        view.merge(member(1, 10, State::Suspect));
        assert_eq!(view.local.incarnation, 11);
        assert_eq!(view.local.state, State::Alive);
    }
    #[test]
    fn acknowledgements_require_the_pending_nonce() {
        let mut view = View::new(member(1, 1, State::Alive));
        view.pending
            .insert([2; 16], (std::time::Instant::now(), 4, false));
        view.acknowledged([2; 16], 3);
        assert_eq!(view.pending.len(), 1);
        view.acknowledged([2; 16], 4);
        assert!(view.pending.is_empty());
    }
}

#[cfg(test)]
mod expiration_tests {
    use super::*;
    #[test]
    fn probes_use_indirect_repair_before_suspicion_and_death() {
        let mut view = View::new(Member {
            node: [1; 16],
            address: "127.0.0.1:1".parse().unwrap(),
            incarnation: 1,
            state: State::Alive,
        });
        view.merge(Member {
            node: [2; 16],
            address: "127.0.0.1:2".parse().unwrap(),
            incarnation: 1,
            state: State::Alive,
        });
        let past = std::time::Instant::now() - Duration::from_secs(10);
        view.pending.insert([2; 16], (past, 7, false));
        assert_eq!(view.expire(), vec![([2; 16], 7)]);
        assert_eq!(view.members[&[2; 16]].state, State::Alive);
        view.pending.get_mut(&[2; 16]).unwrap().0 = past;
        view.expire();
        assert_eq!(view.members[&[2; 16]].state, State::Suspect);
        view.suspect
            .insert([2; 16], std::time::Instant::now() - Duration::from_secs(30));
        view.expire();
        assert_eq!(view.members[&[2; 16]].state, State::Dead);
    }
}
