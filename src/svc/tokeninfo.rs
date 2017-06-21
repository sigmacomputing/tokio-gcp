use std::str::FromStr;

use hyper::Uri;

use client::{self, ApiClient};

pub struct TokenInfoService {}
pub type Hub<'a> = client::Hub<'a, TokenInfoService>;

static TOKEN_INFO_URI: &str = "https://www.googleapis.com/oauth2/v3/tokeninfo";

#[derive(Deserialize, Debug)]
pub struct TokenInfo {
    pub iss: Option<String>,
    pub sub: Option<String>,
    pub azp: Option<String>,
    pub aud: Option<String>,
    pub iat: Option<String>,
    pub exp: Option<String>,

    pub email: Option<String>,
    pub email_verified: Option<String>,
    pub name: Option<String>,
    pub picture: Option<String>,
    pub given_name: Option<String>,
    pub family_name: Option<String>,
    pub locale: Option<String>,
}

impl<'a> Hub<'a> {
    pub fn tokeninfo(&self, scopes: &[String]) -> client::Result<TokenInfo> {
        let token = self.token(scopes)?;

        let mut uri = String::from(TOKEN_INFO_URI);
        uri.push_str(&format!("?access_token={}", token.access_token));

        self.get(&Uri::from_str(&uri).unwrap(), scopes)
    }
}
