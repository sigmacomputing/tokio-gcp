use std::str::FromStr;

use base64;
use hyper::Uri;

use client::{self, ApiClient};

static CLOUDKMS_ROOT: &str = "https://cloudkms.googleapis.com/v1";

pub struct CloudKeyMgmtService {}
pub type Hub<'a> = client::Hub<'a, CloudKeyMgmtService>;

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct CryptoKey {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(rename="nextRotationTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_rotation_time: Option<String>,

    #[serde(rename="rotationPeriod")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation_period: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<CryptoKeyVersion>,

    #[serde(rename="createTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct CryptoKeyVersion {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,

    #[serde(rename="destroyTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy_time: Option<String>,

    #[serde(rename="createTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<String>,

    #[serde(rename="destroyEventTime")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy_event_time: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Default, Debug)]
pub struct EncryptRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plaintext: Option<String>,

    #[serde(rename="additionalAuthenticatedData")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_authenticated_data: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
pub struct EncryptResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ciphertext: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DecryptRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ciphertext: Option<String>,

    #[serde(rename="additionalAuthenticatedData")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_authenticated_data: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DecryptResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plaintext: Option<String>,
}

impl<'a> Hub<'a> {
    pub fn create_cryptokey(&self, keyring: &str, keyid: &str) -> client::Result<String> {
        let path = format!("{}/projects/{}/locations/global/keyRings/{}/cryptoKeys?cryptoKeyId={}",
                           CLOUDKMS_ROOT,
                           self.project_id(),
                           keyring,
                           keyid);

        let req = CryptoKey {
            purpose: Some(String::from("ENCRYPT_DECRYPT")),
            ..Default::default()
        };

        let uri = Uri::from_str(&path).expect("uri to be valid");
        let res = self.post::<_, CryptoKey>(&uri, req, &[])?;
        Ok(res.name.expect("name to be set"))
    }

    pub fn encrypt(&self,
                   cryptokey: &str,
                   plaintext: &[u8],
                   nonce: Option<&str>)
                   -> client::Result<Vec<u8>> {
        let path = format!("{}/{}:encrypt", CLOUDKMS_ROOT, cryptokey);

        let req = EncryptRequest {
            plaintext: Some(base64::encode(plaintext)),
            additional_authenticated_data: nonce.map(|d| d.to_string()),
        };

        let uri = Uri::from_str(&path).expect("uri to be valid");
        let res = self.post::<_, EncryptResponse>(&uri, req, &[])?;

        let ciphertext = res.ciphertext.expect("ciphertext to be set");
        Ok(base64::decode(&ciphertext.as_bytes()).expect("ciphertext to be base64"))
    }

    pub fn decrypt(&self,
                   cryptokey: &str,
                   ciphertext: &[u8],
                   nonce: Option<&str>)
                   -> client::Result<Vec<u8>> {
        let path = format!("{}/{}:decrypt", CLOUDKMS_ROOT, cryptokey);

        let req = DecryptRequest {
            ciphertext: Some(base64::encode(ciphertext)),
            additional_authenticated_data: nonce.map(|d| d.to_string()),
        };

        let uri = Uri::from_str(&path).expect("uri to be valid");
        let res = self.post::<_, DecryptResponse>(&uri, req, &[])?;

        let plaintext = res.plaintext.expect("plaintext to be set");
        Ok(base64::decode(&plaintext.as_bytes()).expect("plaintext to be base64"))
    }
}
