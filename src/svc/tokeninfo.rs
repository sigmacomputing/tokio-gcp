use client::{self, ApiClient};

pub struct TokenInfoService {}
pub type Hub<'a> = client::Hub<'a, TokenInfoService>;

impl<'a> Hub<'a> {
    pub fn access_token(&self, scopes: &[String]) -> client::Result<::auth::Token> {
        self.token(scopes)
    }
}
