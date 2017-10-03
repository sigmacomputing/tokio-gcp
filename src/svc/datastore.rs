use std::collections::HashMap;
use std::str::FromStr;

use hyper::Uri;

use client::{self, ApiClient};

static DATASTORE_ROOT: &str = "https://datastore.googleapis.com/v1";

pub struct DatastoreService {}
pub type Hub<'a> = client::Hub<'a, DatastoreService>;

pub type ValueMap = HashMap<String, Value>;

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BeginTransactionRequest {}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct BeginTransactionResponse {
    pub transaction: String,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RollbackTransactionRequest {
    pub transaction: String,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RollbackTransactionResponse {}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AllocateIdsRequest {
    keys: Vec<Key>,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AllocateIdsResponse {
    keys: Vec<Key>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CommitRequest {
    pub transaction: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutations: Option<Vec<Mutation>>,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct CommitResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_results: Option<Vec<MutationResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_updates: Option<i32>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
struct RunQueryRequest {
    partition_id: PartitionId,
    read_options: ReadOptions,

    #[serde(skip_serializing_if = "Option::is_none")]
    gql_query: Option<GqlQuery>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
struct GqlQuery {
    query_string: String,
    allow_literals: bool,
    named_bindings: HashMap<String, GqlQueryParameter>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
struct GqlQueryParameter {
    value: Value,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct RunQueryResponse {
    pub batch: QueryResultBatch,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResultBatch {
    pub entity_results: Option<Vec<EntityResult>>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Mutation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<Key>,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct MutationResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_detected: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Key>,
}

#[derive(Clone, Serialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LookupRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Key>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_options: Option<ReadOptions>,
}

#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct LookupResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<Vec<EntityResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<EntityResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferred: Option<Vec<Key>>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ReadOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_consistency: Option<String>,
}


#[derive(Clone, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct EntityResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<Entity>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Entity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Key>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Key {
    pub path: Vec<PathElement>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<PartitionId>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PathElement {
    pub kind: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct PartitionId {
    pub project_id: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Value {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_value: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_value: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub double_value: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub meaning: Option<i32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_from_indexes: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_value: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_value: Option<Key>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub boolean_value: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub integer_value: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value: Option<()>,

    //#[serde(rename="geoPointValue")]
    //#[serde(skip_serializing_if = "Option::is_none")]
    //pub geo_point_value: Option<LatLng>,

    //#[serde(rename="arrayValue")]
    //#[serde(skip_serializing_if = "Option::is_none")]
    //pub array_value: Option<ArrayValue>,
}


impl<'a> Hub<'a> {
    //
    // api-level operations

    pub fn begin_transaction(&self) -> client::Result<String> {
        let uri = self.mk_uri("beginTransaction");
        let req = BeginTransactionRequest::default();
        self.post::<_, BeginTransactionResponse>(&uri, req, &[])
            .map(|r| r.transaction)
    }

    pub fn rollback(&self, txn: &str) -> client::Result<()> {
        let uri = self.mk_uri("rollback");
        let req = RollbackTransactionRequest { transaction: txn.to_string() };
        self.post::<_, RollbackTransactionResponse>(&uri, req, &[])
            .map(|_| ())
    }

    pub fn commit(&self, req: CommitRequest) -> client::Result<CommitResponse> {
        debug_assert!(!req.transaction.is_empty());
        let uri = self.mk_uri("commit");
        self.post(&uri, req, &[])
    }

    pub fn allocate_ids(&self, keys: Vec<Key>) -> client::Result<Vec<Key>> {
        let uri = self.mk_uri("allocateIds");
        let req = AllocateIdsRequest { keys: keys };
        self.post::<_, AllocateIdsResponse>(&uri, req, &[]).map(
            |r| {
                r.keys
            },
        )
    }


    //
    // high-level operations

    pub fn insert_entity_auto_id(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        props: ValueMap,
    ) -> client::Result<String> {
        let key = self.mk_key(kind, Some(ns), ancestors, None, None);
        let res = self.insert(key, props)?;
        let mut results = res.mutation_results.expect("mutations to be valid");

        let mut key = results.remove(0).key.expect("key to be valid");
        Ok(key.path.pop().expect("path to be non-empty").id.expect(
            "id to be valid",
        ))
    }

    pub fn insert_entity_by_name(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        name: &str,
        props: ValueMap,
    ) -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), ancestors, Some(name), None);
        self.insert(key, props)?;
        Ok(())
    }

    fn insert(&self, key: Key, props: ValueMap) -> client::Result<CommitResponse> {
        let entity = Entity {
            key: Some(key),
            properties: Some(props),
        };

        let insert = Mutation {
            insert: Some(entity),
            ..Default::default()
        };

        let req = CommitRequest {
            transaction: self.begin_transaction()?,
            mutations: Some(vec![insert]),
            ..Default::default()
        };

        self.commit(req)
    }

    pub fn lookup_by_id(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        id: &str,
        txn: Option<&str>,
    ) -> client::Result<Option<ValueMap>> {
        let key = self.mk_key(kind, Some(ns), ancestors, None, Some(id));
        self.lookup_one(key, txn)
    }

    pub fn lookup_by_name(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        name: &str,
        txn: Option<&str>,
    ) -> client::Result<Option<ValueMap>> {
        let key = self.mk_key(kind, Some(ns), ancestors, Some(name), None);
        self.lookup_one(key, txn)
    }

    pub fn gql<B>(
        &self,
        ns: &str,
        q: &str,
        txn: Option<&str>,
        bindings: B,
    ) -> client::Result<RunQueryResponse>
    where
        B: IntoIterator<Item = (String, Value)>,
    {
        let query = GqlQuery {
            query_string: q.to_string(),
            allow_literals: false,
            named_bindings: bindings
                .into_iter()
                .map(|(k, v)| (k, GqlQueryParameter { value: v }))
                .collect(),
        };

        let req = RunQueryRequest {
            partition_id: PartitionId {
                project_id: self.project_id().to_string(),
                namespace_id: Some(ns.to_string()),
            },
            read_options: ReadOptions {
                transaction: txn.map(|t| t.to_string()),
                ..Default::default()
            },
            gql_query: Some(query),
        };

        let uri = self.mk_uri("runQuery");
        self.post::<_, RunQueryResponse>(&uri, req, &[])
    }

    // Lookup a key using default read options:
    // https://cloud.google.com/datastore/docs/reference/rest/v1/ReadOptions
    pub fn lookup_one(&self, key: Key, txn: Option<&str>) -> client::Result<Option<ValueMap>> {
        let req = LookupRequest {
            keys: Some(vec![key]),
            read_options: Some(ReadOptions {
                transaction: txn.map(|t| t.to_string()),
                ..Default::default()
            }),
        };

        let uri = self.mk_uri("lookup");
        let res = self.post::<_, LookupResponse>(&uri, req, &[])?;

        Ok(
            res.found
                .and_then(|mut f| if f.len() != 1 {
                    None
                } else {
                    f.remove(0).entity
                })
                .and_then(|e| e.properties),
        )
    }

    pub fn update_by_id(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        id: &str,
        props: ValueMap,
    ) -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), ancestors, None, Some(id));

        let entity = Entity {
            key: Some(key),
            properties: Some(props),
        };

        let update = Mutation {
            update: Some(entity),
            ..Default::default()
        };

        let req = CommitRequest {
            transaction: self.begin_transaction()?,
            mutations: Some(vec![update]),
            ..Default::default()
        };

        self.commit(req).map(|_| ())
    }

    pub fn delete_by_id(
        &self,
        kind: &str,
        ns: &str,
        ancestors: Vec<PathElement>,
        id: &str,
    ) -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), ancestors, None, Some(id));

        let delete = Mutation {
            delete: Some(key),
            ..Default::default()
        };

        let req = CommitRequest {
            transaction: self.begin_transaction()?,
            mutations: Some(vec![delete]),
            ..Default::default()
        };

        self.commit(req).map(|_| ())
    }

    fn mk_uri(&self, action: &str) -> Uri {
        let path = format!(
            "{}/projects/{}:{}",
            DATASTORE_ROOT,
            self.project_id(),
            action
        );
        Uri::from_str(&path).expect("uri to be valid")
    }

    fn mk_key(
        &self,
        kind: &str,
        ns: Option<&str>,
        mut path: Vec<PathElement>,
        name: Option<&str>,
        id: Option<&str>,
    ) -> Key {
        let partition_id = PartitionId {
            project_id: self.project_id().to_string(),
            namespace_id: ns.map(|ns| ns.to_string()),
        };

        path.push(PathElement {
            kind: kind.to_string(),
            name: name.map(|name| name.to_string()),
            id: id.map(|id| id.to_string()),
        });

        Key {
            path: path,
            partition_id: Some(partition_id),
        }
    }
}
