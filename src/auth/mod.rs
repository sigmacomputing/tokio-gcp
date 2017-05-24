use std::{env, fs, path};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

use client::{self, ApiClient};

use chrono::{self, DateTime, UTC};
use hyper::{self, Uri};
use hyper::header::ContentType;
use jwt;
use openssl::rsa::Rsa;
use serde_json;
use url::form_urlencoded;

static APP_DEFAULT_PATH: &'static str = ".config/gcloud/application_default_credentials.json";
static APP_DEFAULT_GRANT_TYPE: &'static str = "refresh_token";
static APP_DEFAULT_URI: &'static str = "https://www.googleapis.com/oauth2/v4/token";

static OAUTH_JWT_EXP_DELTA: i64 = 59 * 60; // 59 minutes (in seconds)
static OAUTH_JWT_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
static OAUTH_JWT_DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

pub type Authorization = hyper::header::Authorization<hyper::header::Bearer>;

#[derive(Deserialize, Clone, Debug, Default)]
pub struct Token {
    pub access_token: String,
    token_type: String,
    expires_in: i64,

    // set manually after deserialization
    #[serde(skip_deserializing)]
    expires_at: Option<DateTime<UTC>>,
}

impl Token {
    pub fn into_header(self) -> Authorization {
        let bearer = hyper::header::Bearer { token: self.access_token };
        hyper::header::Authorization(bearer)
    }
    fn is_expired(&self) -> bool {
        self.expires_at.map_or(true, |at| UTC::now() > at)
    }
}

#[derive(Clone, Debug)]
pub struct GoogleCloudAuth {
    token: Arc<RwLock<Token>>,
    adapter: AuthAdapter,
}

impl GoogleCloudAuth {
    pub fn token<C: ApiClient>(&self, client: &C, scopes: &[String]) -> client::Result<Token> {
        {
            let cached = self.token.read().expect("lock to not be poisoned");
            if !cached.is_expired() {
                trace!("reusing cached service account oauth token");
                return Ok((*cached).clone());
            }
        }

        let mut cached = self.token.write().expect("lock to not be poisoned");

        // check is we were blocked on another writer
        if !cached.is_expired() {
            return Ok(cached.clone());
        }

        // refresh the token and shrink the expiration window by 60s
        let mut up_to_date = self.adapter.refresh_token(client, scopes)?;
        let expires_in = chrono::Duration::seconds(up_to_date.expires_in - 60);
        up_to_date.expires_at = Some(UTC::now() + expires_in);

        *cached = up_to_date.clone();
        Ok(up_to_date)
    }
}

#[derive(Clone, Debug)]
enum AuthAdapter {
    ServiceAccount(ServiceAccountAuth),
    ApplicationDefault(ApplicationDefaultAuth),
}

impl AuthAdapter {
    fn refresh_token<C: ApiClient>(&self, client: &C, scopes: &[String]) -> client::Result<Token> {
        match *self {
            AuthAdapter::ServiceAccount(ref auth) => auth.refresh_token(client, scopes),
            AuthAdapter::ApplicationDefault(ref auth) => auth.refresh_token(client, scopes),
        }
    }
}

#[inline(never)] // for stack traces
pub fn default_credentials() -> GoogleCloudAuth {
    let adapter = if let Some(adapter) = credentials_from_env() {
        debug!("Using Google Cloud credentials from [env]");
        adapter
    } else if let Some(adapter) = credentials_from_app_default() {
        debug!("Using Google Cloud credentials from [application_default_credentials]");
        adapter
    } else {
        println!("
            Unable to obtain Google Cloud credentials. Please ensure you have either:
            A) Set GOOGLE_APPLICATION_CREDENTIALS to a valid service account key file
            B) Run the following command 'gcloud auth application-default login'
        ");
        ::std::process::exit(1)
    };

    GoogleCloudAuth {
        adapter: adapter,
        token: Arc::default(),
    }
}

#[inline(never)] // for stack traces
fn credentials_from_env() -> Option<AuthAdapter> {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct RawKey {
        #[serde(rename="type")]
        key_type: String,
        project_id: String,
        private_key_id: String,
        #[serde(rename="private_key")]
        private_key_pem: String,
        client_email: String,
        client_id: String,
        auth_uri: String,
        token_uri: String,
        client_x509_cert_url: String,
        auth_provier_x509_cert_url: Option<String>,
    };

    if let Ok(path) = env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        debug!("GOOGLE_APPLICATION_CREDENTIALS={}", path);

        let file = fs::File::open(path).unwrap();
        let raw = serde_json::from_reader::<_, RawKey>(file).unwrap();

        let private_key = Rsa::private_key_from_pem(raw.private_key_pem.as_bytes()).unwrap();
        let private_key_der = private_key.private_key_to_der().unwrap();

        let meta = ServiceAccountMeta {
            token_uri: Uri::from_str(&raw.token_uri).unwrap(),
            aud: raw.token_uri,
            client_email: raw.client_email,
            private_key_der: private_key_der,
        };

        return Some(AuthAdapter::ServiceAccount(ServiceAccountAuth { meta: meta }));
    }
    None
}

// see https://developers.google.com/identity/protocols/OAuth2WebServer#offline
#[inline(never)] // for stack traces
fn credentials_from_app_default() -> Option<AuthAdapter> {
    let homedir = env::home_dir().unwrap_or("./".into());
    let path = path::Path::new(&homedir).join(APP_DEFAULT_PATH);
    debug!("application-default-credentials={:?}", path);

    if path.exists() {
        let file = fs::File::open(path).unwrap();
        let app_default_auth = serde_json::from_reader::<_, ApplicationDefaultAuth>(file).unwrap();
        return Some(AuthAdapter::ApplicationDefault(app_default_auth));
    }
    None
}

#[derive(Deserialize, Clone, Debug)]
struct ApplicationDefaultAuth {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl ApplicationDefaultAuth {
    fn refresh_token<C: ApiClient>(&self, client: &C, _: &[String]) -> client::Result<Token> {
        trace!("refreshing application default token");

        let body = form_urlencoded::Serializer::new(String::new())
            .append_pair("client_id", &self.client_id)
            .append_pair("client_secret", &self.client_secret)
            .append_pair("refresh_token", &self.refresh_token)
            .append_pair("grant_type", APP_DEFAULT_GRANT_TYPE)
            .finish();

        let uri = Uri::from_str(APP_DEFAULT_URI).expect("app default uri to be valid");
        let mut request = hyper::Request::new(hyper::Method::Post, uri);
        request.set_body(body);
        request
            .headers_mut()
            .set(ContentType::form_url_encoded());

        client.request(request)
    }
}

#[derive(Clone, Debug)]
struct ServiceAccountAuth {
    meta: ServiceAccountMeta,
}

#[derive(Clone, Debug)]
struct ServiceAccountMeta {
    aud: String,
    token_uri: Uri,
    client_email: String,
    private_key_der: Vec<u8>,
}

impl ServiceAccountAuth {
    fn refresh_token<C: ApiClient>(&self, client: &C, scopes: &[String]) -> client::Result<Token> {
        trace!("refreshing service account oauth token");

        let scope = if scopes.is_empty() {
            OAUTH_JWT_DEFAULT_SCOPE.to_string()
        } else {
            scopes.join(" ")
        };

        #[derive(Serialize)]
        struct Claims<'a> {
            iss: &'a str, // email addr of the service account
            aud: &'a str, // always https://www.googleapis.com/oauth2/v4/token
            scope: String, // space delimited list of scopes
            iat: i64, // time issued in seconds since epoch
            exp: i64, // no more than 60 minutes after iat
            sub: Option<&'a str>, // User email address if this is a delegated token
        }

        let iat = UTC::now().timestamp();
        let exp = iat + OAUTH_JWT_EXP_DELTA;
        let claims = Claims {
            iss: self.meta.client_email.as_str(),
            aud: self.meta.aud.as_str(),
            scope: scope,
            iat: iat,
            exp: exp,
            sub: None,
        };

        let header = jwt::Header::new(jwt::Algorithm::RS256);
        let assertion = jwt::encode(&header, &claims, &self.meta.private_key_der)
            .expect("jwt signing not to fail");

        let body = form_urlencoded::Serializer::new(String::new())
            .append_pair("assertion", &assertion)
            .append_pair("grant_type", OAUTH_JWT_GRANT_TYPE)
            .finish();

        let mut request = hyper::Request::new(hyper::Method::Post, self.meta.token_uri.clone());
        request.set_body(body);
        request
            .headers_mut()
            .set(ContentType::form_url_encoded());

        client.request(request)
    }
}
