// Reduced from ipnet 2.12.2. Retained upstream notices: ../LICENSE-{MIT,APACHE}.
use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
};

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IpNet {
    V4(Ipv4Net),
    V6(Ipv6Net),
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Ipv4Net {
    addr: Ipv4Addr,
    prefix_len: u8,
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Ipv6Net {
    addr: Ipv6Addr,
    prefix_len: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrefixLenError;

impl fmt::Display for PrefixLenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid IP prefix length")
    }
}
impl std::error::Error for PrefixLenError {}

impl IpNet {
    pub fn new(addr: IpAddr, prefix_len: u8) -> Result<Self, PrefixLenError> {
        match addr {
            IpAddr::V4(addr) => Ipv4Net::new(addr, prefix_len).map(Self::V4),
            IpAddr::V6(addr) => Ipv6Net::new(addr, prefix_len).map(Self::V6),
        }
    }

    pub fn addr(&self) -> IpAddr {
        match self {
            Self::V4(net) => net.addr.into(),
            Self::V6(net) => net.addr.into(),
        }
    }

    pub fn prefix_len(&self) -> u8 {
        match self {
            Self::V4(net) => net.prefix_len,
            Self::V6(net) => net.prefix_len,
        }
    }

    pub fn contains(&self, addr: &IpAddr) -> bool {
        match (self, addr) {
            (Self::V4(net), IpAddr::V4(addr)) => net.contains(addr),
            (Self::V6(net), IpAddr::V6(addr)) => net.contains(addr),
            _ => false,
        }
    }
}

impl Ipv4Net {
    pub fn new(addr: Ipv4Addr, prefix_len: u8) -> Result<Self, PrefixLenError> {
        if prefix_len > 32 {
            return Err(PrefixLenError);
        }
        Ok(Self { addr, prefix_len })
    }

    pub fn addr(&self) -> Ipv4Addr {
        self.addr
    }
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    pub fn contains(&self, addr: &Ipv4Addr) -> bool {
        let mask = u32::MAX
            .checked_shl(32 - self.prefix_len as u32)
            .unwrap_or(0);
        u32::from(self.addr) & mask == u32::from(*addr) & mask
    }
}

impl Ipv6Net {
    pub fn new(addr: Ipv6Addr, prefix_len: u8) -> Result<Self, PrefixLenError> {
        if prefix_len > 128 {
            return Err(PrefixLenError);
        }
        Ok(Self { addr, prefix_len })
    }

    pub fn addr(&self) -> Ipv6Addr {
        self.addr
    }
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    pub fn contains(&self, addr: &Ipv6Addr) -> bool {
        let mask = u128::MAX
            .checked_shl(128 - self.prefix_len as u32)
            .unwrap_or(0);
        u128::from(self.addr) & mask == u128::from(*addr) & mask
    }
}

impl fmt::Display for IpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V4(net) => fmt::Display::fmt(net, f),
            Self::V6(net) => fmt::Display::fmt(net, f),
        }
    }
}

impl fmt::Display for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix_len)
    }
}

impl fmt::Display for Ipv6Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.prefix_len)
    }
}

impl fmt::Debug for IpNet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Debug for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Debug for Ipv6Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl From<Ipv4Net> for IpNet {
    fn from(net: Ipv4Net) -> Self {
        Self::V4(net)
    }
}
impl From<Ipv6Net> for IpNet {
    fn from(net: Ipv6Net) -> Self {
        Self::V6(net)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_validate_prefixes_and_keep_host_bits() {
        assert!(Ipv4Net::new(Ipv4Addr::LOCALHOST, 33).is_err());
        assert!(Ipv6Net::new(Ipv6Addr::LOCALHOST, 129).is_err());
        let net = IpNet::new("192.168.1.17".parse().unwrap(), 24).unwrap();
        assert_eq!(net.addr(), "192.168.1.17".parse::<IpAddr>().unwrap());
        assert_eq!(net.prefix_len(), 24);
        assert_eq!(net.to_string(), "192.168.1.17/24");
        assert!(net.contains(&"192.168.1.255".parse().unwrap()));
        assert!(!net.contains(&"::ffff:192.168.1.17".parse().unwrap()));
    }
}
