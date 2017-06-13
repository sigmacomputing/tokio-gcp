#![cfg_attr(feature = "strict", deny(warnings))]
extern crate env_logger;
extern crate tokio_gcp as gcp;

use gcp::svc::tokeninfo::Hub as TokenInfoHub;

fn main() {
    env_logger::init().unwrap();

    let client = gcp::GoogleCloudClient::new("").unwrap();

    let tokeninfo_hub = client.hub() as TokenInfoHub;
    println!("{:?}", tokeninfo_hub.tokeninfo(&[]));
}
