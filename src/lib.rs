extern crate base64;
extern crate chrono;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate jsonwebtoken as jwt;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate openssl;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;
extern crate url;

mod auth;
mod client;
pub mod svc;

pub use client::{GoogleCloudClient, Hub};
pub use client::{Error, ApiError, ErrorDetails, Result};
