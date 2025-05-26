use crate::query::{QueryContext, UserQuery};
use crate::session::UserSession;

use core_metastore::Metastore;
use core_metastore::SlateDBMetastore;
use core_metastore::{
    Database as MetastoreDatabase, Schema as MetastoreSchema, SchemaIdent as MetastoreSchemaIdent,
    Volume as MetastoreVolume,
};
use datafusion::sql::parser::DFParser;
use std::sync::Arc;

#[allow(clippy::unwrap_used)]
#[test]
fn test_postprocess_query_statement_functions_expressions() {
    let args: [(&str, &str); 21] = [
        ("select year(ts)", "SELECT date_part('year', ts)"),
        ("select dayofyear(ts)", "SELECT date_part('doy', ts)"),
        ("select day(ts)", "SELECT date_part('day', ts)"),
        ("select dayofmonth(ts)", "SELECT date_part('day', ts)"),
        ("select dayofweek(ts)", "SELECT date_part('dow', ts)"),
        ("select month(ts)", "SELECT date_part('month', ts)"),
        ("select weekofyear(ts)", "SELECT date_part('week', ts)"),
        ("select week(ts)", "SELECT date_part('week', ts)"),
        ("select hour(ts)", "SELECT date_part('hour', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        ("select second(ts)", "SELECT date_part('second', ts)"),
        ("select minute(ts)", "SELECT date_part('minute', ts)"),
        ("select yearofweek(ts)", "SELECT yearofweek(ts)"),
        ("select yearofweekiso(ts)", "SELECT yearofweekiso(ts)"),
        // timestamp keywords postprocess
        (
            "SELECT dateadd(year, 5, '2025-06-01')",
            "SELECT dateadd('year', 5, '2025-06-01')",
        ),
        (
            "SELECT dateadd(\"year\", 5, '2025-06-01')",
            "SELECT dateadd('year', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(day, 5, '2025-06-01')",
            "SELECT datediff('day', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(week, 5, '2025-06-01')",
            "SELECT datediff('week', 5, '2025-06-01')",
        ),
        (
            "SELECT datediff(nsecond, 10000000, '2025-06-01')",
            "SELECT datediff('nsecond', 10000000, '2025-06-01')",
        ),
        (
            "SELECT date_diff(hour, 5, '2025-06-01')",
            "SELECT date_diff('hour', 5, '2025-06-01')",
        ),
        (
            "SELECT date_add(us, 100000, '2025-06-01')",
            "SELECT date_add('us', 100000, '2025-06-01')",
        ),
    ];

    for (init, exp) in args {
        let statement = DFParser::parse_sql(init).unwrap().pop_front();
        if let Some(mut s) = statement {
            UserQuery::postprocess_query_statement(&mut s);
            assert_eq!(s.to_string(), exp);
        }
    }
}
static TABLE_SETUP: &str = include_str!(r"./table_setup.sql");

#[allow(clippy::unwrap_used, clippy::expect_used)]
pub async fn create_df_session() -> Arc<UserSession> {
    let metastore = SlateDBMetastore::new_in_memory().await;
    metastore
        .create_volume(
            &"test_volume".to_string(),
            MetastoreVolume::new(
                "test_volume".to_string(),
                core_metastore::VolumeType::Memory,
            ),
        )
        .await
        .expect("Failed to create volume");
    metastore
        .create_database(
            &"embucket".to_string(),
            MetastoreDatabase {
                ident: "embucket".to_string(),
                properties: None,
                volume: "test_volume".to_string(),
            },
        )
        .await
        .expect("Failed to create database");
    let schema_ident = MetastoreSchemaIdent {
        database: "embucket".to_string(),
        schema: "public".to_string(),
    };
    metastore
        .create_schema(
            &schema_ident.clone(),
            MetastoreSchema {
                ident: schema_ident,
                properties: None,
            },
        )
        .await
        .expect("Failed to create schema");

    let user_session = Arc::new(
        UserSession::new(metastore)
            .await
            .expect("Failed to create user session"),
    );

    for query in TABLE_SETUP.split(';') {
        if !query.is_empty() {
            let mut query = user_session.query(query, QueryContext::default());
            query.execute().await.unwrap();
        }
    }
    user_session
}

#[macro_export]
macro_rules! test_query {
    (
        $test_fn_name:ident,
        $query:expr
        $(, setup_queries =[$($setup_queries:expr),* $(,)?])?
        $(, sort_all = $sort_all:expr)?
        $(, snapshot_path = $user_snapshot_path:expr)?
    ) => {
        paste::paste! {
            #[tokio::test]
            async fn [< query_ $test_fn_name >]() {
                let ctx = crate::tests::query::create_df_session().await;

                // Execute all setup queries (if provided) to set up the session context
                $(
                    $(
                        {
                            let mut q = ctx.query($setup_queries, crate::query::QueryContext::default());
                            q.execute().await.unwrap();
                        }
                    )*
                )?

                let mut query = ctx.query($query, crate::query::QueryContext::default());
                let res = query.execute().await;
                let sort_all = false $(|| $sort_all)?;

                let mut settings = insta::Settings::new();
                settings.set_description(stringify!($query));
                settings.set_omit_expression(true);
                settings.set_prepend_module_to_snapshot(false);
                settings.set_snapshot_path(concat!("snapshots", "/") $(.to_owned() + $user_snapshot_path)?);

                let setup: Vec<&str> = vec![$($($setup_queries),*)?];
                if !setup.is_empty() {
                    settings.set_info(&format!("Setup queries: {}", setup.join("; ")));
                }
                settings.bind(|| {
                    let df = match res {
                        Ok(record_batches) => {
                            let mut batches: Vec<datafusion::arrow::array::RecordBatch> = record_batches.records;
                            if sort_all {
                                for batch in &mut batches {
                                    *batch = df_catalog::test_utils::sort_record_batch_by_sortable_columns(batch);
                                }
                            }
                            Ok(datafusion::arrow::util::pretty::pretty_format_batches(&batches).unwrap().to_string())
                        },
                        Err(e) => Err(format!("Error: {e}"))
                    };

                    let df = df.map(|df| df.split('\n').map(|s| s.to_string()).collect::<Vec<String>>());
                    insta::assert_debug_snapshot!((df));
                });
            }
        }
    };
}

// CREATE SCHEMA
test_query!(
    create_schema,
    "SHOW SCHEMAS IN embucket STARTS WITH 'new_schema'",
    setup_queries = ["CREATE SCHEMA embucket.new_schema"]
);

// CREATE TABLE
test_query!(
    create_table_with_timestamp_nanosecond,
    "CREATE TABLE embucket.public.ts_table (ts TIMESTAMP_NTZ(9)) as VALUES ('2025-04-09T21:11:23');"
);

test_query!(
    create_table_and_insert,
    "SELECT * FROM embucket.public.test",
    setup_queries = [
        "CREATE TABLE embucket.public.test (id INT)",
        "INSERT INTO embucket.public.test VALUES (1), (2)",
    ]
);

// DROP TABLE
test_query!(
    drop_table,
    "SHOW TABLES IN public STARTS WITH 'test'",
    setup_queries = [
        "CREATE TABLE embucket.public.test (id INT) as VALUES (1), (2)",
        "DROP TABLE embucket.public.test"
    ]
);

// context name injection
test_query!(
    context_name_injection,
    "SHOW TABLES IN new_schema",
    setup_queries = [
        "CREATE SCHEMA embucket.new_schema",
        "SET schema = 'new_schema'",
        "CREATE table new_table (id INT)",
    ]
);

// SELECT
test_query!(select_date_add_diff, "SELECT dateadd(day, 5, '2025-06-01')");
test_query!(func_date_add, "SELECT date_add(day, 30, '2025-01-06')");
test_query!(select_star, "SELECT * FROM employee_table");
// FIXME: ILIKE is not supported yet
// test_query!(select_ilike, "SELECT * ILIKE '%id%' FROM employee_table;");
test_query!(
    select_exclude,
    "SELECT * EXCLUDE department_id FROM employee_table;"
);
test_query!(
    select_exclude_multiple,
    "SELECT * EXCLUDE (department_id, employee_id) FROM employee_table;"
);

test_query!(
    qualify,
    "SELECT product_id, retail_price, quantity, city
    FROM sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY retail_price) = 1;"
);

// SHOW DATABASES
test_query!(
    show_databases,
    "SHOW DATABASES",
    sort_all = true,
    snapshot_path = "session"
);

// SHOW SCHEMAS
test_query!(
    show_schemas,
    "SHOW SCHEMAS",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_schemas_starts_with,
    "SHOW SCHEMAS STARTS WITH 'publ'",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_schemas_in_db,
    "SHOW SCHEMAS IN embucket",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_schemas_in_db_and_prefix,
    "SHOW SCHEMAS IN embucket STARTS WITH 'pub'",
    sort_all = true,
    snapshot_path = "session"
);

// SHOW TABLES
test_query!(
    show_tables,
    "SHOW TABLES",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_tables_starts_with,
    "SHOW TABLES STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_tables_in_schema,
    "SHOW TABLES IN public",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_tables_in_schema_full,
    "SHOW TABLES IN embucket.public",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_tables_in_schema_and_prefix,
    "SHOW TABLES IN public STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "session"
);

// SHOW VIEWS
test_query!(
    show_views,
    "SHOW VIEWS",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_views_starts_with,
    "SHOW VIEWS STARTS WITH 'schem'",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_views_in_schema,
    "SHOW VIEWS IN information_schema",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_views_in_schema_full,
    "SHOW VIEWS IN embucket.information_schema",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_views_in_schema_and_prefix,
    "SHOW VIEWS IN information_schema STARTS WITH 'schem'",
    sort_all = true,
    snapshot_path = "session"
);

// SHOW COLUMNS
test_query!(
    show_columns,
    "SHOW COLUMNS",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_columns_in_table,
    "SHOW COLUMNS IN employee_table",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_columns_in_table_full,
    "SHOW COLUMNS IN embucket.public.employee_table",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_columns_starts_with,
    "SHOW COLUMNS IN employee_table STARTS WITH 'last_'",
    sort_all = true,
    snapshot_path = "session"
);

// SHOW OBJECTS
test_query!(
    show_objects,
    "SHOW OBJECTS",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_objects_starts_with,
    "SHOW OBJECTS STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_objects_in_schema,
    "SHOW OBJECTS IN public",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_objects_in_schema_full,
    "SHOW OBJECTS IN embucket.public",
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    show_objects_in_schema_and_prefix,
    "SHOW OBJECTS IN public STARTS WITH 'dep'",
    sort_all = true,
    snapshot_path = "session"
);

// SESSION RELATED https://docs.snowflake.com/en/sql-reference/commands-session
test_query!(
    alter_session_set,
    "SHOW VARIABLES",
    setup_queries = ["ALTER SESSION SET v1 = 'test'"],
    snapshot_path = "session"
);
test_query!(
    alter_session_unset,
    "SHOW VARIABLES",
    setup_queries = [
        "ALTER SESSION SET v1 = 'test' v2 = 1",
        "ALTER SESSION UNSET v1"
    ],
    snapshot_path = "session"
);

// TODO SHOW PARAMETERS is not supported yet
test_query!(
    show_parameters,
    "SHOW PARAMETERS",
    sort_all = true,
    snapshot_path = "session"
);

test_query!(
    use_role,
    "SHOW VARIABLES",
    setup_queries = ["USE ROLE test_role"],
    snapshot_path = "session"
);

test_query!(
    use_secondary_roles,
    "SHOW VARIABLES",
    setup_queries = ["USE SECONDARY ROLES test_role"],
    snapshot_path = "session"
);
test_query!(
    use_warehouse,
    "SHOW VARIABLES",
    setup_queries = ["USE WAREHOUSE test_warehouse"],
    snapshot_path = "session"
);
test_query!(
    use_database,
    "SHOW VARIABLES",
    setup_queries = ["USE DATABASE test_db"],
    snapshot_path = "session"
);
test_query!(
    use_schema,
    "SHOW VARIABLES",
    setup_queries = ["USE SCHEMA test_schema"],
    snapshot_path = "session"
);

test_query!(
    show_variables_multiple,
    "SHOW VARIABLES",
    setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    sort_all = true,
    snapshot_path = "session"
);
test_query!(
    set_variable,
    "SHOW VARIABLES",
    setup_queries = ["SET v1 = 'test'"],
    snapshot_path = "session"
);
test_query!(
    set_variable_system,
    "SELECT name, value FROM snowplow.information_schema.df_settings
     WHERE name = 'datafusion.execution.time_zone'",
    setup_queries = ["SET datafusion.execution.time_zone = 'TEST_TIMEZONE'"],
    snapshot_path = "session"
);
// TODO Currently UNSET is not supported
test_query!(
    unset_variable,
    "UNSET v3",
    setup_queries = ["SET v1 = 'test'", "SET v2 = 1", "SET v3 = true"],
    snapshot_path = "session"
);

// https://docs.snowflake.com/en/sql-reference/sql/explain
// https://datafusion.apache.org/user-guide/sql/explain.html
// Datafusion has different output format.
// Check session config ExplainOptions for the full list of options
// logical_only_plan flag is used to only print logical plans
// since physical plan contains dynamic files names
test_query!(
    explain_select,
    "EXPLAIN SELECT * FROM embucket.public.employee_table",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_limit,
    "EXPLAIN SELECT * FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_column,
    "EXPLAIN SELECT last_name FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
test_query!(
    explain_select_missing_column,
    "EXPLAIN SELECT missing FROM embucket.public.employee_table limit 1",
    setup_queries = ["SET datafusion.explain.logical_plan_only = true"],
    snapshot_path = "session"
);
