#![allow(unused_variables)]
use std::str::FromStr;

use hyper::{self, Uri};

use serde::{Serialize, Deserialize};
use serde_json;

use client::{self, ApiClient};

static BIGQUERY_ROOT: &str = "https://www.googleapis.com/bigquery/v2/projects";
static QUERY_RESOURCE_KIND: &str = "bigquery#queryResults";

pub struct BigQueryService {}
pub type Hub<'a> = client::Hub<'a, BigQueryService>;

#[derive(Serialize, Default, Debug)]
pub struct ListDatasetsRequest {
    pub all: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,

    #[serde(rename="maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,

    #[serde(rename="pageToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl ListDatasetsRequest {
    fn to_query(&self) -> String {
        let mut params = vec![("all", self.all.to_string())];
        if let Some(ref filter) = self.filter {
            params.push(("filter", filter.clone()));
        }
        if let Some(ref max_results) = self.max_results {
            params.push(("maxResults", max_results.to_string()));
        }
        if let Some(ref page_token) = self.page_token {
            params.push(("pageToken", page_token.clone()));
        }
        client::encode_query_params(params)
    }
}

#[derive(Deserialize, Default, Debug)]
pub struct ListDatasetsResponse {
    #[serde(rename="nextPageToken")]
    pub next_page_token: Option<String>,

    pub datasets: Vec<DatasetMeta>,
}

#[derive(Deserialize, Default, Debug)]
pub struct DatasetMeta {
    pub id: String,

    #[serde(rename="datasetReference")]
    pub dataset_reference: DatasetReference,

    #[serde(rename="friendlyName")]
    pub friendly_name: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct DatasetReference {
    #[serde(rename="projectId")]
    pub project_id: String,

    #[serde(rename="datasetId")]
    pub dataset_id: String,
}

#[derive(Serialize, Default, Debug)]
pub struct ListTablesRequest {
    #[serde(rename="maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,

    #[serde(rename="pageToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl ListTablesRequest {
    fn to_query(&self) -> String {
        let mut params = vec![];
        if let Some(ref max_results) = self.max_results {
            params.push(("maxResults", max_results.to_string()));
        }
        if let Some(ref page_token) = self.page_token {
            params.push(("pageToken", page_token.clone()));
        }
        client::encode_query_params(params)
    }
}

#[derive(Deserialize, Default, Debug)]
pub struct ListTablesResponse {
    #[serde(rename="nextPageToken")]
    pub next_page_token: Option<String>,

    pub tables: Vec<TableMeta>,

    #[serde(rename="totalItems")]
    pub total_items: usize,
}

#[derive(Deserialize, Default, Debug)]
pub struct TableMeta {
    pub id: String,

    #[serde(rename="tableReference")]
    pub table_reference: TableReference,

    #[serde(rename="friendlyName")]
    pub friendly_name: Option<String>,

    #[serde(rename="type")]
    pub type0: String,
}

#[derive(Deserialize, Default, Debug)]
pub struct TableReference {
    #[serde(rename="projectId")]
    pub project_id: String,

    #[serde(rename="datasetId")]
    pub dataset_id: String,

    #[serde(rename="tableId")]
    pub table_id: String,
}

#[derive(Deserialize, Default, Debug)]
pub struct DescribeTableResponse {
    pub id: String,
    pub schema: TableFieldSchema,

    #[serde(rename="type")]
    pub type0: String,

    #[serde(rename="tableReference")]
    pub table_reference: TableReference,

    #[serde(rename="friendlyName")]
    pub friendly_name: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
pub struct TableFieldSchema {
    pub fields: Vec<TableField>,
}

#[derive(Deserialize, Default, Debug)]
pub struct TableField {
    pub name: String,
    pub mode: String,

    #[serde(rename="type")]
    pub type0: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<TableField>>,
}

#[derive(Serialize, Debug)]
pub struct SubmitQueryRequest {
    pub kind: String,
    pub query: String,

    #[serde(rename="timeoutMs")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<usize>,

    #[serde(rename="useLegacySql")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_legacy_sql: Option<bool>,

    #[serde(rename="useQueryCache")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_query_cache: Option<bool>,

    #[serde(rename="dryRun")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,

    #[serde(rename="maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,
}

impl Default for SubmitQueryRequest {
    fn default() -> Self {
        SubmitQueryRequest {
            kind: QUERY_RESOURCE_KIND.to_string(),
            query: Default::default(),
            timeout_ms: None,
            use_legacy_sql: None,
            use_query_cache: None,
            dry_run: None,
            max_results: None,
        }
    }
}

#[derive(Deserialize, Default, Debug)]
pub struct SubmitQueryResponse {
    pub schema: Option<TableFieldSchema>,

    #[serde(rename="jobReference")]
    pub job_reference: JobReference,

    #[serde(rename="totalRows")]
    pub total_rows: Option<String>,

    pub rows: Option<Vec<TableRow>>,

    #[serde(rename="jobComplete")]
    pub job_complete: bool,

    #[serde(rename="pageToken")]
    pub page_token: Option<String>,

    pub errors: Option<Vec<QueryError>>,
}

#[derive(Deserialize, Default, Debug)]
pub struct JobReference {
    #[serde(rename="projectId")]
    pub project_id: String,

    #[serde(rename="jobId")]
    pub job_id: String,
}

#[derive(Deserialize, Default, Debug)]
pub struct QueryError {
    pub reason: Option<String>,
    pub location: Option<String>,
    pub message: Option<String>,

    #[serde(rename="debugInfo")]
    pub debug_info: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
pub struct CancelQueryResponse {}

#[derive(Serialize, Default, Debug)]
pub struct AwaitQueryRequest {
    #[serde(rename="timeoutMs")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<usize>,

    #[serde(rename="maxResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,

    #[serde(rename="startIndex")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_index: Option<usize>,

    #[serde(rename="pageToken")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl AwaitQueryRequest {
    fn to_query(&self) -> String {
        let mut params = Vec::new();
        if let Some(ref timeout_ms) = self.timeout_ms {
            params.push(("timeoutMs", timeout_ms.to_string()));
        }
        if let Some(ref max_results) = self.max_results {
            params.push(("maxResults", max_results.to_string()));
        }
        if let Some(ref page_token) = self.page_token {
            params.push(("pageToken", page_token.clone()));
        }
        if let Some(ref start_index) = self.start_index {
            params.push(("startIndex", start_index.to_string()));
        }
        client::encode_query_params(params)
    }
}

#[derive(Deserialize, Default, Debug)]
pub struct AwaitQueryResponse {
    pub schema: Option<TableFieldSchema>,

    #[serde(rename="jobReference")]
    pub job_reference: Option<JobReference>,

    #[serde(rename="totalRows")]
    pub total_rows: Option<String>,

    pub rows: Option<Vec<TableRow>>,

    #[serde(rename="jobComplete")]
    pub job_complete: bool,

    #[serde(rename="pageToken")]
    pub page_token: Option<String>,

    pub errors: Option<Vec<QueryError>>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct TableRow {
    pub f: Vec<TableCell>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct TableCell {
    pub v: Cell,
}

#[serde(untagged)]
#[derive(Serialize, Deserialize, Debug)]
pub enum Cell {
    Value(Option<String>),
    Repeat(Vec<TableCell>),
    Row(TableRow),
}

impl Default for Cell {
    fn default() -> Self {
        Cell::Value(None)
    }
}

impl<'a> Hub<'a> {
    pub fn list_datasets(&self,
                         token: &str,
                         project_id: &str,
                         req: &ListDatasetsRequest)
                         -> client::Result<ListDatasetsResponse> {
        let path = format!("{}/{}/datasets?{}",
                           BIGQUERY_ROOT,
                           project_id,
                           req.to_query());

        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<ListDatasetsResponse>(&uri, token.to_string())
    }

    pub fn list_tables(&self,
                       token: &str,
                       project_id: &str,
                       dataset_id: &str,
                       req: &ListTablesRequest)
                       -> client::Result<ListTablesResponse> {
        let path = format!("{}/{}/datasets/{}/tables?{}",
                           BIGQUERY_ROOT,
                           project_id,
                           dataset_id,
                           req.to_query());
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<ListTablesResponse>(&uri, token.to_string())
    }

    pub fn describe_table(&self,
                          token: &str,
                          project_id: &str,
                          dataset_id: &str,
                          table_id: &str)
                          -> client::Result<DescribeTableResponse> {
        let path = format!("{}/{}/datasets/{}/tables/{}",
                           BIGQUERY_ROOT,
                           project_id,
                           dataset_id,
                           table_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<DescribeTableResponse>(&uri, token.to_string())
    }

    pub fn submit_query(&self,
                        token: &str,
                        project_id: &str,
                        req: &SubmitQueryRequest)
                        -> client::Result<SubmitQueryResponse> {
        let path = format!("{}/{}/queries", BIGQUERY_ROOT, project_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.post_bq::<_, SubmitQueryResponse>(&uri, req, token.to_string())
    }

    pub fn await_query(&self,
                       token: &str,
                       project_id: &str,
                       job_id: &str,
                       req: &AwaitQueryRequest)
                       -> client::Result<AwaitQueryResponse> {
        let path = format!("{}/{}/queries/{}?{}",
                           BIGQUERY_ROOT,
                           project_id,
                           job_id,
                           req.to_query());
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<AwaitQueryResponse>(&uri, token.to_string())
    }

    pub fn cancel_query(&self,
                        token: &str,
                        project_id: &str,
                        job_id: &str)
                        -> client::Result<CancelQueryResponse> {
        let path = format!("{}/{}/jobs/{}/cancel", BIGQUERY_ROOT, project_id, job_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");

        let mut req = hyper::Request::new(hyper::Method::Post, uri);
        req.headers_mut().set(hyper::header::ContentType::json());

        let auth = hyper::header::Authorization(hyper::header::Bearer { token: token.to_string() });
        req.headers_mut().set(auth);

        self.request::<CancelQueryResponse>(req)
    }

    // helper method for making a GET request
    fn get_bq<D>(&self, uri: &hyper::Uri, token: String) -> client::Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        let mut req = hyper::Request::new(hyper::Method::Get, uri.clone());
        let auth = hyper::header::Authorization(hyper::header::Bearer { token });
        req.headers_mut().set(auth);
        self.request(req)
    }

    // helper method for making a POST request with a JSON body
    fn post_bq<B: Serialize, D>(&self,
                                uri: &hyper::Uri,
                                body: B,
                                token: String)
                                -> client::Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        let mut req = hyper::Request::new(hyper::Method::Post, uri.clone());
        req.headers_mut().set(hyper::header::ContentType::json());

        let auth = hyper::header::Authorization(hyper::header::Bearer { token });
        req.headers_mut().set(auth);

        let body = serde_json::to_string(&body).unwrap();
        req.set_body(body);

        self.request(req)
    }

    /*
     */
}
