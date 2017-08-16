#![allow(unused_variables)]
use std::str::FromStr;

use hyper::{self, Uri};

use serde::{Serialize, Deserialize};
use serde_json;

use client::{self, ApiClient};
use svc::common;

static BIGQUERY_ROOT: &str = "https://www.googleapis.com/bigquery/v2/projects";

pub struct BigQueryService {}
pub type Hub<'a> = client::Hub<'a, BigQueryService>;

#[derive(Default, Debug)]
pub struct ListDatasetsRequest {
    pub all: bool,
    pub filter: Option<String>,
    pub max_results: Option<usize>,
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

#[derive(Default, Debug)]
pub struct ListTablesRequest {
    pub max_results: Option<usize>,
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

#[derive(Serialize, Deserialize, Default, Debug)]
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

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobResource {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub dry_run: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub job_reference: Option<JobReference>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,

    pub configuration: JobConfiguration,
}

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<QueryResource>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResource {
    pub query: String,
    pub use_legacy_sql: bool,
    pub use_query_cache: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination_table: Option<TableReference>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobStatus {
    pub state: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_result: Option<QueryError>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub errors: Option<Vec<QueryError>>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct JobReference {
    pub project_id: String,

    pub job_id: String,
}

#[derive(Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub reason: Option<String>,
    pub location: Option<String>,
    pub message: Option<String>,
    pub debug_info: Option<String>,
}

#[derive(Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct GetQueryResultsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_results: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_index: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

impl GetQueryResultsRequest {
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
pub struct GetQueryResultsResponse {
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

    pub fn create_job(&self,
                      token: &str,
                      project_id: &str,
                      req: &JobResource)
                      -> client::Result<JobResource> {
        let path = format!("{}/{}/jobs", BIGQUERY_ROOT, project_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.post_bq::<_, _>(&uri, req, token.to_string())
    }

    pub fn cancel_job(&self,
                      token: &str,
                      project_id: &str,
                      job_id: &str)
                      -> client::Result<JobResource> {
        let path = format!("{}/{}/jobs/{}/cancel", BIGQUERY_ROOT, project_id, job_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");

        #[derive(Deserialize, Debug)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            pub job: JobResource,
        }

        self.post_bq::<_, Response>(&uri, common::Empty {}, token.to_string())
            .map(|r| r.job)
    }

    pub fn get_job(&self,
                   token: &str,
                   project_id: &str,
                   job_id: &str)
                   -> client::Result<JobResource> {
        let path = format!("{}/{}/jobs/{}", BIGQUERY_ROOT, project_id, job_id);
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<_>(&uri, token.to_string())
    }

    pub fn get_query_results(&self,
                             token: &str,
                             project_id: &str,
                             job_id: &str,
                             req: &GetQueryResultsRequest)
                             -> client::Result<GetQueryResultsResponse> {
        let path = format!("{}/{}/queries/{}?{}",
                           BIGQUERY_ROOT,
                           project_id,
                           job_id,
                           req.to_query());
        let uri = Uri::from_str(&path).expect("uri to be valid");
        self.get_bq::<_>(&uri, token.to_string())
    }

    // helper method for making a GET request
    fn get_bq<D>(&self, uri: &hyper::Uri, token: String) -> client::Result<D>
        where for<'de> D: 'static + Send + Deserialize<'de>
    {
        let mut req = hyper::Request::new(hyper::Method::Get, uri.clone());
        let auth = hyper::header::Authorization(hyper::header::Bearer { token });
        req.headers_mut().set(auth);

        self.request(req).map(|(_, res)| res)
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

        self.request(req).map(|(_, res)| res)
    }
}
