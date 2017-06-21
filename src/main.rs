#![cfg_attr(feature = "strict", deny(warnings))]
extern crate env_logger;
extern crate tokio_gcp as gcp;

use gcp::svc::tokeninfo::Hub as TokenInfoHub;

fn main() {
    env_logger::init().unwrap();

    let client = gcp::GoogleCloudClient::new("slate-fc6b5").unwrap();

    let tokeninfo_hub = client.hub() as TokenInfoHub;
    println!("{:?}", tokeninfo_hub.tokeninfo(&[]));

    let fb_hub = client.hub() as ::gcp::svc::firebase::Hub;
    println!("data: {:?}",
             fb_hub
                 .get_data::<::std::collections::HashMap<String, bool>>("wb/357d3307-24ea-4604-bbc4-b9ba09a982aa/queries/58f50b06-148f-499c-aadf-eea3170bb8e0", ::gcp::svc::firebase::GetOptions{shallow: true})
                 .expect("it to work"));
}
