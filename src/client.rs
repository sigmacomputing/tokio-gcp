use std::{io, fmt, thread, time};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};

use hyper;
use hyper_tls;
use futures::{future, Future, Stream};
use futures::sync::oneshot;
use serde::{Deserialize, Serialize};
use serde_json;
use tokio_core::reactor;

use auth;

// (max) NOTE please don't use this directly - prefer 'access_hyper_client'
type HyperClient = hyper::Client<hyper_tls::HttpsConnector<hyper::client::HttpConnector>>;
lazy_static!(static ref HYPER_CLIENT: AtomicPtr<HyperClient> = Default::default(););

// (max) NOTE since Handle is only available on the reactor thread, we can
// enforce that HyperClient is only used there by requiring Handle as an arg
fn access_hyper_client(_: &reactor::Handle) -> &HyperClient {
    unsafe { &*HYPER_CLIENT.load(Ordering::SeqCst) }
}

const DNS_WORKER_THREADS: usize = 4;
const KEEP_ALIVE_TIMEOUT: u64 = 600; // 10 minutes

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ApiError(ApiError),
    HyperError(hyper::Error),
    JsonError(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> ::std::result::Result<(), fmt::Error> {
        match *self {
            Error::ApiError(ref e) => write!(f, "ApiError {:?}", e),
            Error::HyperError(ref e) => write!(f, "HyperError {:?}", e),
            Error::JsonError(ref e) => write!(f, "JsonError {:?}", e),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct ApiError {
    #[serde(default)]
    pub error: Option<ErrorDetails>,
    pub error_description: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct ErrorDetails {
    code: Option<usize>,
    message: Option<String>,
    status: Option<String>,
}

#[derive(Clone)]
pub struct GoogleCloudClient {
    project_id: String,
    remote: reactor::Remote,
    auth: auth::GoogleCloudAuth,
    _core_thread: Arc<thread::JoinHandle<()>>,
}

impl GoogleCloudClient {
    pub fn new(project_id: &str) -> io::Result<Self> {
        let (tx, rx) = oneshot::channel();
        let core_thread = thread::Builder::new()
            .name("google-cloud-client".into())
            .spawn(move || {
                let mut core = reactor::Core::new().unwrap();

                let https = hyper_tls::HttpsConnector::new(DNS_WORKER_THREADS, &core.handle())
                    .unwrap();

                let mut client = hyper::Client::configure()
                    .connector(https)
                    .keep_alive_timeout(Some(time::Duration::from_secs(KEEP_ALIVE_TIMEOUT)))
                    .keep_alive(true)
                    .build(&core.handle());

                HYPER_CLIENT.store(&mut client, Ordering::SeqCst);

                let remote = core.remote();
                tx.send(remote).unwrap();

                core.run(future::empty::<(), ()>()).unwrap()
            })?;

        let remote = rx.wait().unwrap();
        Ok(GoogleCloudClient {
               project_id: project_id.to_string(),
               remote: remote,
               auth: auth::default_credentials(),
               _core_thread: Arc::new(core_thread),
           })
    }
    pub fn hub<S>(&self) -> Hub<S> {
        Hub {
            client: self,
            _service: PhantomData,
        }
    }
}

pub struct Hub<'a, S> {
    client: &'a GoogleCloudClient,
    _service: PhantomData<S>,
}

impl<'a, S> Hub<'a, S> {
    pub fn project_id(&self) -> &str {
        &self.client.project_id
    }
}

impl<'a, S> ApiClient for Hub<'a, S> {
    fn token(&self, scopes: &[String]) -> Result<auth::Token> {
        self.client.auth.token(self, scopes)
    }

    fn request<D: 'static + Send>(&self, r: hyper::Request<hyper::Body>) -> Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        trace!("send request: {:?}", r);
        let (tx, rx) = oneshot::channel();
        self.client
            .remote
            .spawn(|handle| {
                let work = access_hyper_client(handle).request(r);
                work.and_then(|res| {
                        trace!("recv response: {:?}", res);
                        let status = res.status();

                        res.body()
                            .collect()
                            .map(move |chunks| {
                                trace!("fold chunks: {:?}", chunks);

                                let body = chunks
                                    .into_iter()
                                    .fold(vec![], |mut acc, chunk| {
                                        acc.extend_from_slice(&*chunk);
                                        acc
                                    });

                                let res = if status.is_success() {
                                    serde_json::from_slice(&body).map_err(|e| Error::JsonError(e))
                                } else {
                                    match serde_json::from_slice::<ApiError>(&body) {
                                        Ok(e) => Err(Error::ApiError(e)),
                                        Err(e) => Err(Error::JsonError(e)),
                                    }
                                };

                                let as_str = unsafe { ::std::str::from_utf8_unchecked(&body) };
                                trace!("rslv oneshot: {}", as_str);

                                tx.send(res).unwrap_or(())
                            })
                    })
                    .map_err(|_| ())
            });
        rx.wait().unwrap()
    }
}

pub trait ApiClient {
    // submits a raw request using hyper
    fn request<D>(&self, hyper::Request<hyper::Body>) -> Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>;

    // fetches an access token for use in requests
    fn token(&self, scopes: &[String]) -> Result<auth::Token>;

    // helper method for making a GET request
    fn get<D>(&self, uri: &hyper::Uri, scopes: &[String]) -> Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        let mut req = hyper::Request::new(hyper::Method::Get, uri.clone());
        req.headers_mut().set(self.token(scopes)?.into_header());

        self.request(req)
    }

    // helper method for making a POST request with a JSON body
    fn post<B: Serialize, D>(&self, uri: &hyper::Uri, body: B, scopes: &[String]) -> Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        let mut req = hyper::Request::new(hyper::Method::Post, uri.clone());
        req.headers_mut().set(hyper::header::ContentType::json());
        req.headers_mut().set(self.token(scopes)?.into_header());

        let body = serde_json::to_string(&body).unwrap();
        req.set_body(body);

        self.request(req)
    }
}
