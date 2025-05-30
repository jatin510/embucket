use core_metastore::RwObject;
use core_metastore::models::Database as MetastoreDatabase;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct DatabasePayload {
    pub name: String,
    pub volume: String,
}

impl From<MetastoreDatabase> for DatabasePayload {
    fn from(db: MetastoreDatabase) -> Self {
        Self {
            name: db.ident,
            volume: db.volume,
        }
    }
}

impl From<Database> for DatabasePayload {
    fn from(db: Database) -> Self {
        Self {
            name: db.name.clone(),
            volume: db.volume,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Eq, PartialEq)]
pub struct Database {
    pub name: String,
    pub volume: String,
    pub created_at: String,
    pub updated_at: String,
}

impl From<RwObject<MetastoreDatabase>> for Database {
    fn from(db: RwObject<MetastoreDatabase>) -> Self {
        Self {
            name: db.data.ident,
            volume: db.data.volume,
            created_at: db.created_at.to_string(),
            updated_at: db.updated_at.to_string(),
        }
    }
}

// TODO: Remove it when found why it can't locate .into() if only From trait implemeted
#[allow(clippy::from_over_into)]
impl Into<MetastoreDatabase> for DatabasePayload {
    fn into(self) -> MetastoreDatabase {
        MetastoreDatabase {
            ident: self.name,
            volume: self.volume,
            properties: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreatePayload {
    #[serde(flatten)]
    pub data: DatabasePayload,
}

// TODO: make Database fields optional in update payload, not used currently
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdatePayload {
    #[serde(flatten)]
    pub data: DatabasePayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseCreateResponse {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseUpdateResponse {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabaseResponse {
    #[serde(flatten)]
    pub data: Database,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatabasesResponse {
    pub items: Vec<Database>,
}
