use std::str::FromStr;

use hyper::Uri;

use client::{self, ApiClient};

pub struct FirebaseService {}
pub type Hub<'a> = client::Hub<'a, FirebaseService>;

lazy_static! {
    static ref DATABASE_SCOPES: Vec<String> = vec!["https://www.googleapis.com/auth/firebase.database".into(),
                       "https://www.googleapis.com/auth/userinfo.email".into()];
}

impl<'a> Hub<'a> {
    pub fn get_data(&self, path: &str) -> client::Result<String> {
        let uri =
            Uri::from_str(&format!("https://{}.firebaseio.com/{}.json", self.project_id(), path))
                .expect("uri is valid");
        self.get(&uri, &*DATABASE_SCOPES)
    }
}
