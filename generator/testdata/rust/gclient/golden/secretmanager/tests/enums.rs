#[test]
pub fn test_enum() {
    use secretmanager_golden_gclient::model::secret::*;
    let e = Expiration::ExpireTime(gax_placeholder::Timestamp::default().set_seconds(123));
    match e {
        Expiration::ExpireTime(t) => {
            println!("{t:?}")
        },
        Expiration::Ttl(d) => {
            println!("{d:?}")
        },
        _ => { println!("unknown oneof branch")},
    }
}
