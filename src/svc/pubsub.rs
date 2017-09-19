use std::collections::HashMap;
use std::str::FromStr;

use hyper::Uri;

use client::{self, ApiClient};

static PUBSUB_ROOT: &str = "https://pubsub.googleapis.com/v1";

pub struct PubsubService {}
pub type Hub<'a> = client::Hub<'a, PubsubService>;

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishRequest {
    pub messages: Vec<PubsubPublishMessage>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PublishResponse {
    pub message_ids: Vec<String>,
}

/// The message payload must not be empty; it must contain either a non-empty data field, or at
/// least one attribute.
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PubsubPublishMessage {
    /// data is a base64-encoded string
    pub data: String,
    pub attributes: HashMap<String, String>,
}

impl<'a> Hub<'a> {
    pub fn publish_messages(
        &self,
        ns: &str,
        topic: &str,
        messages: Vec<PubsubPublishMessage>,
    ) -> client::Result<PublishResponse> {
        let uri = Uri::from_str(&format!(
            "{}/projects/{}/topics/{}:publish",
            PUBSUB_ROOT,
            ns,
            topic,
        )).expect("uri to be valid");
        self.post(&uri, PublishRequest { messages }, &[])
    }
}
