//use super::datafusion::functions::geospatial::register_udfs as register_geo_udfs;
use super::datafusion::functions::register_udfs;
use super::datafusion::type_planner::CustomTypePlanner;
use super::dedicated_executor::DedicatedExecutor;
use super::error::{
    self as ex_error, ExecutionError, ExecutionResult, RefreshCatalogListSnafu,
    RegisterCatalogSnafu,
};
use super::query::{QueryContext, UserQuery};
use crate::datafusion::analyzer::IcebergTypesAnalyzer;
// TODO: We need to fix this after geodatafusion is updated to datafusion 47
//use geodatafusion::udf::native::register_native as register_geo_native;
use crate::datafusion::physical_optimizer::physical_optimizer_rules;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;
use aws_credential_types::provider::SharedCredentialsProvider;
use chrono::{DateTime, Utc};
use core_metastore::error::MetastoreError;
use core_metastore::{AwsCredentials, Metastore, VolumeType as MetastoreVolumeType};
use core_utils::scan_iterator::ScanIterator;
use datafusion::catalog::CatalogProvider;
use datafusion::common::error::Result as DFResult;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::sql::planner::IdentNormalizer;
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_functions_json::register_all as register_json_udfs;
use datafusion_iceberg::catalog::catalog::IcebergCatalog as DataFusionIcebergCatalog;
use datafusion_iceberg::planner::IcebergQueryPlanner;
use derive_more::Debug;
use df_builtins::register_udafs;
use df_catalog::catalog_list::{DEFAULT_CATALOG, EmbucketCatalogList};
use iceberg_rust::object_store::ObjectStoreBuilder;
use iceberg_s3tables_catalog::S3TablesCatalog;
use snafu::ResultExt;
use sqlparser::ast::Value;
use sqlparser::ast::helpers::key_value_options::{KeyValueOption, KeyValueOptionType};
use std::any::Any;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

#[derive(Debug)]
pub struct UserSession {
    pub metastore: Arc<dyn Metastore>,
    #[debug(skip)]
    pub ctx: SessionContext,
    pub ident_normalizer: IdentNormalizer,
    pub executor: DedicatedExecutor,
}

impl UserSession {
    pub async fn new(metastore: Arc<dyn Metastore>) -> ExecutionResult<Self> {
        let sql_parser_dialect =
            env::var("SQL_PARSER_DIALECT").unwrap_or_else(|_| "snowflake".to_string());

        let catalog_list_impl = Arc::new(EmbucketCatalogList::new(metastore.clone()));

        let runtime_config = RuntimeEnvBuilder::new()
            .with_object_store_registry(catalog_list_impl.clone())
            .build()
            .context(ex_error::DataFusionSnafu)?;

        let state = SessionStateBuilder::new()
            .with_config(
                SessionConfig::new()
                    .with_option_extension(SessionParams::default())
                    .with_information_schema(true)
                    // Cannot create catalog (database) automatic since it requires default volume
                    .with_create_default_catalog_and_schema(false)
                    .set_str("datafusion.sql_parser.dialect", &sql_parser_dialect)
                    .set_str("datafusion.catalog.default_catalog", DEFAULT_CATALOG),
            )
            .with_default_features()
            .with_runtime_env(Arc::new(runtime_config))
            .with_catalog_list(catalog_list_impl.clone())
            .with_query_planner(Arc::new(IcebergQueryPlanner::new()))
            .with_type_planner(Arc::new(CustomTypePlanner {}))
            .with_analyzer_rule(Arc::new(IcebergTypesAnalyzer {}))
            .with_physical_optimizer_rules(physical_optimizer_rules())
            .build();
        let mut ctx = SessionContext::new_with_state(state);
        register_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        register_udafs(&mut ctx).context(ex_error::RegisterUDAFSnafu)?;
        register_json_udfs(&mut ctx).context(ex_error::RegisterUDFSnafu)?;
        //register_geo_native(&ctx);
        //register_geo_udfs(&ctx);

        catalog_list_impl
            .register_catalogs()
            .await
            .context(RegisterCatalogSnafu)?;
        catalog_list_impl
            .refresh()
            .await
            .context(RefreshCatalogListSnafu)?;
        catalog_list_impl.start_refresh_internal_catalogs_task(10);
        let enable_ident_normalization = ctx.enable_ident_normalization();
        let session = Self {
            metastore,
            ctx,
            ident_normalizer: IdentNormalizer::new(enable_ident_normalization),
            executor: DedicatedExecutor::builder().build(),
        };
        session.register_external_catalogs().await?;
        Ok(session)
    }

    #[allow(clippy::as_conversions)]
    pub async fn register_external_catalogs(&self) -> ExecutionResult<()> {
        let volumes = self
            .metastore
            .iter_volumes()
            .collect()
            .await
            .map_err(|e| ExecutionError::Metastore {
                source: MetastoreError::UtilSlateDB { source: e },
            })?
            .into_iter()
            .filter_map(|volume| {
                if let MetastoreVolumeType::S3Tables(s3_volume) = volume.volume.clone() {
                    Some(s3_volume)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if volumes.is_empty() {
            return Ok(());
        }
        for volume in volumes {
            let (ak, sk, token) = match volume.credentials {
                AwsCredentials::AccessKey(ref creds) => (
                    Some(creds.aws_access_key_id.clone()),
                    Some(creds.aws_secret_access_key.clone()),
                    None,
                ),
                AwsCredentials::Token(ref token) => (None, None, Some(token.clone())),
            };
            let creds =
                Credentials::from_keys(ak.unwrap_or_default(), sk.unwrap_or_default(), token);
            let config = SdkConfig::builder()
                .behavior_version(BehaviorVersion::latest())
                .credentials_provider(SharedCredentialsProvider::new(creds))
                .region(Region::new(volume.region.clone()))
                .build();
            let catalog = S3TablesCatalog::new(
                &config,
                volume.arn.as_str(),
                ObjectStoreBuilder::S3(volume.s3_builder()),
            )
            .context(ex_error::S3TablesSnafu)?;
            println!("s3 catalog {:?}", catalog);

            let catalog = DataFusionIcebergCatalog::new(Arc::new(catalog), None)
                .await
                .context(ex_error::DataFusionSnafu)?;
            let catalog_provider = Arc::new(catalog) as Arc<dyn CatalogProvider>;

            self.ctx.register_catalog(volume.name, catalog_provider);
        }
        Ok(())
    }

    pub fn query<S>(self: &Arc<Self>, query: S, query_context: QueryContext) -> UserQuery
    where
        S: Into<String>,
    {
        UserQuery::new(self.clone(), query.into(), query_context)
    }

    pub fn set_session_variable(
        &self,
        set: bool,
        params: HashMap<String, SessionProperty>,
    ) -> ExecutionResult<()> {
        let state = self.ctx.state_ref();
        let mut write = state.write();

        let mut datafusion_params = Vec::new();
        let mut session_params = HashMap::new();

        for (key, prop) in params {
            if key.to_lowercase().starts_with("datafusion.") {
                datafusion_params.push((key, prop.value));
            } else {
                session_params.insert(key, prop);
            }
        }
        let options = write.config_mut().options_mut();
        for (key, value) in datafusion_params {
            options
                .set(&key, &value)
                .context(ex_error::DataFusionSnafu)?;
        }

        let config = options.extensions.get_mut::<SessionParams>();
        if let Some(cfg) = config {
            if set {
                cfg.set_properties(session_params)
                    .context(ex_error::DataFusionSnafu)?;
            } else {
                cfg.remove_properties(session_params)
                    .context(ex_error::DataFusionSnafu)?;
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn get_session_variable(&self, variable: &str) -> Option<String> {
        let state = self.ctx.state();
        let config = state.config().options().extensions.get::<SessionParams>();
        if let Some(cfg) = config {
            return cfg.properties.get(variable).map(|v| v.value.clone());
        }
        None
    }
}

#[derive(Default, Debug, Clone)]
pub struct SessionParams {
    pub properties: HashMap<String, SessionProperty>,
}

#[derive(Default, Debug, Clone)]
pub struct SessionProperty {
    pub session_id: Option<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub value: String,
    pub property_type: String,
    pub comment: Option<String>,
}

impl SessionProperty {
    pub fn from_key_value(option: &KeyValueOption) -> Self {
        let now = Utc::now();
        Self {
            session_id: None,
            created_on: now,
            updated_on: now,
            value: option.value.clone(),
            property_type: match option.option_type {
                KeyValueOptionType::STRING | KeyValueOptionType::ENUM => "text".to_string(),
                KeyValueOptionType::BOOLEAN => "boolean".to_string(),
                KeyValueOptionType::NUMBER => "fixed".to_string(),
            },
            comment: None,
        }
    }

    pub fn from_value(option: Value) -> Self {
        let now = Utc::now();
        Self {
            session_id: None,
            created_on: now,
            updated_on: now,
            value: match option {
                Value::Number(_, _) | Value::Boolean(_) => option.to_string(),
                _ => option.clone().into_string().unwrap_or_default(),
            },
            property_type: match option {
                Value::Number(_, _) => "fixed".to_string(),
                Value::Boolean(_) => "boolean".to_string(),
                _ => "text".to_string(),
            },
            comment: None,
        }
    }

    pub fn from_str(value: String) -> Self {
        let now = Utc::now();
        Self {
            session_id: None,
            created_on: now,
            updated_on: now,
            value,
            property_type: "text".to_string(),
            comment: None,
        }
    }
}

impl SessionParams {
    pub fn set_properties(&mut self, properties: HashMap<String, SessionProperty>) -> DFResult<()> {
        for (key, value) in properties {
            self.properties.insert(key, value);
        }
        Ok(())
    }

    pub fn remove_properties(
        &mut self,
        properties: HashMap<String, SessionProperty>,
    ) -> DFResult<()> {
        for (key, ..) in properties {
            self.properties.remove(&key);
        }
        Ok(())
    }
}

impl ConfigExtension for SessionParams {
    const PREFIX: &'static str = "session_params";
}

impl ExtensionOptions for SessionParams {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        self.properties
            .insert(key.to_owned(), SessionProperty::from_str(value.to_owned()));
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        self.properties
            .iter()
            .map(|(key, prop)| ConfigEntry {
                key: format!("session_params.{key}"),
                value: Some(prop.value.clone()),
                description: "session variable",
            })
            .collect()
    }
}
