use std::str::FromStr;

use hyper::Uri;
use serde::Deserialize;

use client::{self, ApiClient};

pub struct FirebaseService {}
pub type Hub<'a> = client::Hub<'a, FirebaseService>;

pub struct GetOptions {
    pub shallow: bool,
}

lazy_static! {
    static ref DATABASE_SCOPES: Vec<String> = vec!["https://www.googleapis.com/auth/firebase.database".into(),
                       "https://www.googleapis.com/auth/userinfo.email".into()];
}

impl<'a> Hub<'a> {
    pub fn get_data<D>(
        &self,
        firebase_project_id: &str,
        path: &str,
        opts: GetOptions,
    ) -> client::Result<D>
    where
        for<'de> D: 'static + Send + Deserialize<'de>,
    {
        let uri = Uri::from_str(&format!(
            "https://{}.firebaseio.com/{}.json?shallow={}",
            firebase_project_id,
            path,
            opts.shallow
        )).expect("uri is valid");
        self.get(&uri, &*DATABASE_SCOPES)
    }
}
