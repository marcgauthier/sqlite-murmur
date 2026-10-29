use ipnet::IpNet;
fn snapshot() -> String {
    let mut inputs: Vec<String> = ["", "1.2.3.4", "1.2.3.4/", "1.2.3.4/+24", "1.2.3.4/-1", "1.2.3.4/33", "1.2.3.4/024", "1.2.3.4/00", "01.002.003.004/24", " 1.2.3.4/24", "1.2.3.4/24 ", "::/129", "::/+1", "::/000", "::ffff:192.168.1.9/120", "2001:db8::abcd/064", "::1%lo/128", "::1/128/1", "fe80::1/128", "1.2.3.999/24", "1.2.3.4/2a", "192.168.1.17/24"].into_iter().map(str::to_owned).collect();
    for prefix in 0..=32 { inputs.push(format!("192.168.1.17/{prefix}")); }
    for prefix in 0..=128 { inputs.push(format!("2001:db8:42::abcd/{prefix}")); }
    let addresses: Vec<std::net::IpAddr> = ["0.0.0.0", "255.255.255.255", "192.168.1.0", "192.168.1.17", "192.168.1.255", "192.168.2.1", "::", "::1", "2001:db8:42::abcd", "2001:db8:42::abce", "2001:db9::1", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "::ffff:192.168.1.17"].iter().map(|x|x.parse().unwrap()).collect();
    let mut records = Vec::new();
    for text in inputs {
        records.push(match text.parse::<IpNet>() {
            Ok(net) => {
                let binary = postcard::to_allocvec(&net).unwrap();
                let decoded: IpNet = postcard::from_bytes(&binary).unwrap();
                assert_eq!(net, decoded);
                let json = serde_json::to_string(&net).unwrap();
                assert_eq!(net, serde_json::from_str(&json).unwrap());
                serde_json::json!({"input":text,"display":net.to_string(),"debug":format!("{net:?}"),"json":json,"binary":binary,"contains":addresses.iter().map(|ip|net.contains(ip)).collect::<Vec<_>>()})
            },
            Err(error) => serde_json::json!({"input":text,"error":error.to_string()}),
        });
    }
    serde_json::to_string_pretty(&records).unwrap()+"\n"
}
#[test]
fn ipnet_matches_original_parser_serialization_and_containment() {
    assert_eq!(snapshot(), include_str!("../../common/fixtures/ipnet.json"));
}
