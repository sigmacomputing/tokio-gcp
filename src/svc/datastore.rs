use std::collections::HashMap;
use std::str::FromStr;

use hyper::Uri;

use client::{self, ApiClient};

static DATASTORE_ROOT: &str = "https://datastore.googleapis.com/v1";

pub struct DatastoreService {}
pub type Hub<'a> = client::Hub<'a, DatastoreService>;

pub type ValueMap = HashMap<String, Value>;

#[derive(Serialize, Default, Debug)]
pub struct BeginTransactionRequest {}

#[derive(Deserialize, Default, Debug)]
pub struct BeginTransactionResponse {
    pub transaction: String,
}

#[derive(Serialize, Default, Debug)]
pub struct CommitRequest {
    pub transaction: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutations: Option<Vec<Mutation>>,
}

#[derive(Deserialize, Default, Debug)]
pub struct CommitResponse {
    #[serde(rename="mutationResults")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutation_results: Option<Vec<MutationResult>>,

    #[serde(rename="indexUpdates")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index_updates: Option<i32>,
}

#[derive(Serialize, Default, Debug)]
pub struct Mutation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub upsert: Option<Entity>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub update: Option<Entity>,

    #[serde(rename="baseVersion")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<Key>,
}

#[derive(Deserialize, Default, Debug)]
pub struct MutationResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(rename="conflictDetected")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub conflict_detected: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Key>,
}

#[derive(Serialize, Default, Debug)]
pub struct LookupRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keys: Option<Vec<Key>>,

    #[serde(rename="readOptions")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_options: Option<ReadOptions>,
}

#[derive(Deserialize, Default, Debug)]
pub struct LookupResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub found: Option<Vec<EntityResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub missing: Option<Vec<EntityResult>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferred: Option<Vec<Key>>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct ReadOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<String>,

    #[serde(rename="readConsistency")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_consistency: Option<String>,
}


#[derive(Deserialize, Default, Debug)]
pub struct EntityResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity: Option<Entity>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Entity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, Value>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Key>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Key {
    pub path: Vec<PathElement>,

    #[serde(rename="partitionId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_id: Option<PartitionId>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct PathElement {
    pub kind: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct PartitionId {
    #[serde(rename="projectId")]
    pub project_id: String,

    #[serde(rename="namespaceId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace_id: Option<String>,
}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct Value {
    #[serde(rename="entityValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entity_value: Option<Entity>,

    #[serde(rename="timestampValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp_value: Option<String>,

    #[serde(rename="stringValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_value: Option<String>,

    #[serde(rename="doubleValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub double_value: Option<f64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub meaning: Option<i32>,

    #[serde(rename="excludeFromIndexes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclude_from_indexes: Option<bool>,

    #[serde(rename="blobValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_value: Option<String>,

    #[serde(rename="keyValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_value: Option<Key>,

    #[serde(rename="booleanValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boolean_value: Option<bool>,

    #[serde(rename="integerValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub integer_value: Option<String>,

    #[serde(rename="nullValue")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub null_value: Option<String>,

    //#[serde(rename="geoPointValue")]
    //#[serde(skip_serializing_if = "Option::is_none")]
    //pub geo_point_value: Option<LatLng>,

    //#[serde(rename="arrayValue")]
    //#[serde(skip_serializing_if = "Option::is_none")]
    //pub array_value: Option<ArrayValue>,
}


impl<'a> Hub<'a> {
    pub fn insert_entity_auto_id(&self,
                                 kind: &str,
                                 ns: &str,
                                 props: ValueMap)
                                 -> client::Result<String> {
        let key = self.mk_key(kind, Some(ns), None, None);
        let res = self.insert(key, props)?;
        let mut results = res.mutation_results.expect("mutations to be valid");

        let mut key = results.remove(0).key.expect("key to be valid");
        Ok(key.path.remove(0).id.expect("id to be valid"))
    }

    pub fn insert_entity_by_name(&self,
                                 kind: &str,
                                 ns: &str,
                                 name: &str,
                                 props: ValueMap)
                                 -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), Some(name), None);
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
            mutations: Some(vec![insert]),
            ..Default::default()
        };

        let txn_id = self.begin_transaction()?;
        self.commit(&txn_id, req)
    }

    pub fn lookup_by_id(&self, kind: &str, ns: &str, id: &str) -> client::Result<Option<ValueMap>> {
        let key = self.mk_key(kind, Some(ns), None, Some(id));
        self.lookup_one(key)
    }

    pub fn lookup_by_name(&self,
                          kind: &str,
                          ns: &str,
                          name: &str)
                          -> client::Result<Option<ValueMap>> {
        let key = self.mk_key(kind, Some(ns), Some(name), None);
        self.lookup_one(key)
    }

    fn lookup_one(&self, key: Key) -> client::Result<Option<ValueMap>> {
        let req = LookupRequest {
            keys: Some(vec![key]),
            read_options: None,
        };

        let uri = self.mk_uri("lookup");
        let res = self.post::<_, LookupResponse>(&uri, req, &[])?;

        Ok(res.found
               .and_then(|mut f| if f.len() != 1 {
                             None
                         } else {
                             f.remove(0).entity
                         })
               .and_then(|e| e.properties))
    }

    pub fn update_by_id(&self,
                        kind: &str,
                        ns: &str,
                        id: &str,
                        props: ValueMap)
                        -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), None, Some(id));

        let entity = Entity {
            key: Some(key),
            properties: Some(props),
        };

        let update = Mutation {
            update: Some(entity),
            ..Default::default()
        };

        let req = CommitRequest {
            mutations: Some(vec![update]),
            ..Default::default()
        };

        let txn_id = self.begin_transaction()?;
        self.commit(&txn_id, req)?;

        Ok(())
    }

    pub fn delete_by_id(&self, kind: &str, ns: &str, id: &str) -> client::Result<()> {
        let key = self.mk_key(kind, Some(ns), None, Some(id));

        let delete = Mutation {
            delete: Some(key),
            ..Default::default()
        };

        let req = CommitRequest {
            mutations: Some(vec![delete]),
            ..Default::default()
        };

        let txn_id = self.begin_transaction()?;
        self.commit(&txn_id, req)?;

        Ok(())
    }

    fn begin_transaction(&self) -> client::Result<String> {
        let uri = self.mk_uri("beginTransaction");
        let req = BeginTransactionRequest::default();
        self.post::<_, BeginTransactionResponse>(&uri, req, &[])
            .map(|r| r.transaction)
    }

    fn commit(&self, txn_id: &str, mut req: CommitRequest) -> client::Result<CommitResponse> {
        req.transaction = txn_id.to_string();
        let uri = self.mk_uri("commit");
        self.post(&uri, req, &[])
    }

    fn mk_uri(&self, action: &str) -> Uri {
        let path = format!("{}/projects/{}:{}",
                           DATASTORE_ROOT,
                           self.project_id(),
                           action);
        Uri::from_str(&path).expect("uri to be valid")
    }

    fn mk_key(&self, kind: &str, ns: Option<&str>, name: Option<&str>, id: Option<&str>) -> Key {
        let partition_id = PartitionId {
            project_id: self.project_id().to_string(),
            namespace_id: ns.map(|ns| ns.to_string()),
        };

        let path = PathElement {
            kind: kind.to_string(),
            name: name.map(|name| name.to_string()),
            id: id.map(|id| id.to_string()),
        };

        Key {
            path: vec![path],
            partition_id: Some(partition_id),
        }
    }
}
