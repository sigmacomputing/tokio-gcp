use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{self, DateTime, UTC};
use hyper::{self, Uri};
use openssl::x509::X509;

use client::{self, ApiClient};

// https://developers.google.com/identity/sign-in/web/backend-auth#verify-the-integrity-of-the-id-token
const GOOGLE_CERT_PEM_API: &str = "https://www.googleapis.com/oauth2/v1/certs";
// https://firebase.google.com/docs/auth/admin/verify-id-tokens#verify_id_tokens_using_a_third-party_jwt_library
const FIREBASE_CERT_PEM_API: &str = "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com";

#[derive(Debug, Clone)]
pub struct PubKey(Arc<Box<[u8]>>);

impl Deref for PubKey {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &**self.0
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum KeyRingType {
    GoogleAuth,
    Firebase,
}

#[derive(Clone, Debug)]
pub struct KeyRing {
    keys: HashMap<String, Arc<Box<[u8]>>>,
    expires_at: Option<DateTime<UTC>>,
}

impl KeyRing {
    pub fn is_expired(&self) -> bool {
        self.expires_at.map_or(true, |at| UTC::now() > at)
    }
    pub fn get(&self, kid: &str) -> client::Result<PubKey> {
        self.keys
            .get(kid)
            .map(|pk| PubKey(pk.clone()))
            .ok_or_else(|| client::Error::Unauthorized)
    }
}

pub fn fetch<C: ApiClient>(client: &C, ty: KeyRingType) -> client::Result<KeyRing> {
    let uri = match ty {
        KeyRingType::Firebase => Uri::from_str(FIREBASE_CERT_PEM_API).unwrap(),
        KeyRingType::GoogleAuth => Uri::from_str(GOOGLE_CERT_PEM_API).unwrap(),
    };

    let req = hyper::Request::new(hyper::Method::Get, uri);
    let (headers, json_pem_map) = client.request::<HashMap<String, String>>(req)?;
    let keys = json_pem_map
        .into_iter()
        .map(|(kid, pem)| {
            Ok((kid,
                Arc::new(X509::from_pem(pem.as_bytes())?
                             .public_key()?
                             .rsa()?
                             .public_key_to_der_pkcs1()?
                             .into_boxed_slice())))

        })
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(|e| client::Error::OpenSslError(e))?;

    Ok(KeyRing {
           keys: keys,
           expires_at: mk_expires_at(headers),
       })
}

fn mk_expires_at(headers: hyper::Headers) -> Option<DateTime<UTC>> {
    if let Some(&hyper::header::CacheControl(ref directives)) = headers.get() {
        for directive in directives {
            if let hyper::header::CacheDirective::MaxAge(age) = *directive {
                let expires_in = chrono::Duration::seconds(age as i64);
                return Some(UTC::now() + expires_in);
            }
        }
    }
    None
}
