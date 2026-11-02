//! Browsing data for the desktop app (section 9.4): table pages with filter and sort done by the
//! database (VIEW-5) and read-only queries in the database's own language (VIEW-6). Results are
//! streamed as the same column batches the migrations use, through a bounded channel, so the
//! database cursor only moves when the reader asks for the next batch (VIEW-2).

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use tokio::sync::mpsc;
use tokio_postgres::{types::Type, IsolationLevel, NoTls};

use crate::{
    db::{
        database::DatabaseConnectionType,
        mongodb::{mongodb::Mongodb, mongodb_data_types::{push_bson, SchemaAnalyzer}},
        mysql::{
            mysql_connection::{connect as mysql_connect, quote_ident as mysql_quote, Mysql},
            mysql_data_types::logical_type_for_result as mysql_logical,
            mysql_provider::{select_query as mysql_select, stream_rows as mysql_stream},
        },
        oracle::{
            oracle_connection::{quote_ident as oracle_quote, Oracle},
            oracle_data_types::logical_type_for_result as oracle_logical,
            oracle_provider::{select_query as oracle_select, stream_read_only as oracle_stream},
        },
        postgresql::{
            postgresql::{quote_ident, Postgresql},
            postgresql_data_types::{logical_type_for as pg_logical, select_expression},
            postgresql_provider::{copy_out, select_query},
        },
        scylladb::{
            scylladb::{sc, Scylla},
            scylladb_data_types::{logical_type_for as cql_logical, push_cql, quote as cql_quote},
        },
        sqlite::{
            sqlite::{quote_ident as sqlite_quote, Sqlite},
            sqlite_data_types::{logical_type_for as sqlite_logical, push_value},
        },
    },
    dbc::batch::ColumnBatch,
    migrator::{batching::BatchSizer, migrator_steps::DataProvider},
    models::{
        data_definitions::{DataDefinition, DataPropertyInfo},
        value::LogicalType,
    },
    DMError, DMResult,
};

/// Sort by one column.
#[derive(Debug, Clone, PartialEq)]
pub struct Sort {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    /// All records of a container; `filter` is a SQL / CQL `WHERE` condition or a MongoDB filter
    /// document (JSON).
    Table { def: DataDefinition, filter: Option<String>, sort: Option<Sort> },
    /// SQL / CQL `SELECT`, or `db.<collection>.find(..)` / `db.<collection>.aggregate(..)`.
    Text(String),
}

/// Stops a running query on the server.
pub enum Canceller {
    /// the TLS settings of the connection, so the cancel request is accepted by TLS-only servers
    Postgres(tokio_postgres::CancelToken, Option<crate::db::tls::TlsOptions>),
    Sqlite(Arc<rusqlite::InterruptHandle>),
    /// MySQL: `KILL QUERY <connection id>` from a second connection
    Mysql(crate::models::connection::MigrationConnection, u32),
    /// Oracle: OCIBreak on the connection that runs the query
    Oracle(Arc<oracle::Connection>),
    /// MongoDB and ScyllaDB: dropping the cursor / paging stream ends the server work
    Drop,
}

impl Canceller {
    pub async fn cancel(&self) {
        match self {
            Canceller::Postgres(t, tls) => {
                let r = match tls.as_ref().map(|t| t.client_config()) {
                    Some(Ok(cfg)) => t.cancel_query(tokio_postgres_rustls::MakeRustlsConnect::new(cfg)).await,
                    Some(Err(e)) => return log::warn!("cancel request failed: {e}"),
                    None => t.cancel_query(NoTls).await,
                };
                if let Err(e) = r {
                    log::warn!("cancel request failed: {e}");
                }
            }
            Canceller::Sqlite(h) => h.interrupt(),
            Canceller::Mysql(c, id) => {
                use mysql_async::prelude::Queryable;
                let r = match mysql_connect(c).await {
                    Ok(mut conn) => conn.query_drop(format!("KILL QUERY {id}")).await.map_err(crate::DMError::from),
                    Err(e) => Err(e),
                };
                if let Err(e) = r {
                    log::warn!("cancel request failed: {e}");
                }
            }
            Canceller::Oracle(c) => {
                let c = c.clone();
                if let Ok(Err(e)) = tokio::task::spawn_blocking(move || c.break_execution()).await {
                    log::warn!("cancel request failed: {e}");
                }
            }
            Canceller::Drop => {}
        }
    }
}

/// Read-only queries for browsing (VIEW-9: nothing here can change data).
#[async_trait]
pub trait QueryRunner: Send {
    /// Names and types of the result columns, without reading rows.
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition>;

    /// Streams the result in batches of `sizer.next()` rows; returns the number of rows.
    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64>;

    fn canceller(&self) -> Canceller;
}

fn not_empty(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

fn query_text(q: &str) -> &str {
    q.trim().trim_end_matches(';').trim_end()
}

fn result_column(name: String, data_type: LogicalType, source_type: Option<String>) -> DataPropertyInfo {
    DataPropertyInfo {
        property_name: name,
        data_type,
        source_type,
        is_nullable: true,
        is_identifier: false,
        reference_container_name: None,
        reference_property_name: None,
        shape: None,
    }
}

fn query_def(columns: Vec<DataPropertyInfo>) -> DataDefinition {
    DataDefinition::new("query".to_string(), columns)
}

fn sql_table_query(base: String, filter: &Option<String>, sort: &Option<Sort>, quote: fn(&str) -> String) -> String {
    let mut q = base;
    if let Some(f) = not_empty(filter) {
        q.push_str(&format!(" WHERE ({f})"));
    }
    if let Some(s) = sort {
        q.push_str(&format!(" ORDER BY {} {}", quote(&s.column), if s.descending { "DESC" } else { "ASC" }));
    }
    q
}

/// `information_schema.columns.data_type` name of a result column type, so result columns are
/// read the same way as table columns.
fn pg_data_type(ty: &Type) -> &'static str {
    match *ty {
        Type::INT2 => "smallint",
        Type::INT4 => "integer",
        Type::INT8 => "bigint",
        Type::FLOAT4 => "real",
        Type::FLOAT8 => "double precision",
        Type::NUMERIC => "numeric",
        Type::MONEY => "money",
        Type::BOOL => "boolean",
        Type::DATE => "date",
        Type::TIME => "time without time zone",
        Type::TIMESTAMP => "timestamp without time zone",
        Type::TIMESTAMPTZ => "timestamp with time zone",
        Type::UUID => "uuid",
        Type::JSON => "json",
        Type::JSONB => "jsonb",
        Type::BYTEA => "bytea",
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => "text",
        _ if matches!(ty.kind(), tokio_postgres::types::Kind::Array(_)) => "ARRAY",
        _ => "USER-DEFINED",
    }
}

#[async_trait]
impl QueryRunner for Postgresql {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match query {
            Query::Table { def, .. } => Ok(def.clone()),
            Query::Text(q) => {
                let stmt = self.client.prepare(query_text(q)).await?;
                Ok(query_def(
                    stmt.columns()
                        .iter()
                        .map(|c| {
                            let dt = pg_data_type(c.type_());
                            result_column(c.name().to_string(), pg_logical(dt), Some(dt.to_string()))
                        })
                        .collect(),
                ))
            }
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let sql = match query {
            Query::Table { filter, sort, .. } if not_empty(filter).is_none() && sort.is_none() => return self.read_data(def, sizer, tx).await,
            Query::Table { filter, sort, .. } => sql_table_query(select_query(def)?, filter, sort, quote_ident),
            // positional aliases keep duplicate result names (joins) apart
            Query::Text(q) => {
                let aliases: Vec<String> = (0..def.properties_info.len()).map(|i| format!("c{i}")).collect();
                let columns: Vec<String> = def
                    .properties_info
                    .iter()
                    .zip(&aliases)
                    .map(|(p, a)| select_expression(a, p.source_type.as_deref().unwrap_or("text")))
                    .collect();
                format!("SELECT {} FROM ({}) AS dm_q({})", columns.join(", "), query_text(q), aliases.join(", "))
            }
        };
        let t = self.client.build_transaction().isolation_level(IsolationLevel::RepeatableRead).read_only(true).start().await?;
        let n = copy_out(&t, def, &sql, &sizer, &tx).await?;
        t.commit().await?;
        Ok(n)
    }

    fn canceller(&self) -> Canceller {
        Canceller::Postgres(self.client.cancel_token(), self.connection.tls.clone())
    }
}

/// Streams the rows of `sql` (columns aligned with `def`) on the blocking pool.
pub(crate) fn sqlite_stream(conn: &rusqlite::Connection, sql: &str, def: &DataDefinition, sizer: &BatchSizer, tx: &mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
    let mut stmt = conn.prepare(sql)?;
    if !stmt.readonly() {
        return Err(DMError::Error("only read-only statements can be run here".to_string()));
    }
    let mut rows = stmt.query([])?;
    let mut total = 0u64;
    let mut size = sizer.next();
    let mut batch = ColumnBatch::for_definition(def, size);
    while let Some(r) = rows.next()? {
        for (i, col) in batch.columns.iter_mut().enumerate() {
            push_value(col, r.get_ref(i)?)?;
        }
        batch.end_row();
        if batch.rows >= size {
            total += batch.rows as u64;
            size = sizer.next();
            tx.blocking_send(std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size)))
                .map_err(|_| DMError::Error("receiver stopped".to_string()))?;
        }
    }
    if !batch.is_empty() {
        total += batch.rows as u64;
        tx.blocking_send(batch).map_err(|_| DMError::Error("receiver stopped".to_string()))?;
    }
    Ok(total)
}

#[async_trait]
impl QueryRunner for Sqlite {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match query {
            Query::Table { def, .. } => Ok(def.clone()),
            Query::Text(q) => {
                let q = query_text(q).to_string();
                self.run(move |conn| {
                    let stmt = conn.prepare(&q)?;
                    if !stmt.readonly() {
                        return Err(DMError::Error("only read-only statements can be run here".to_string()));
                    }
                    // expressions have no declared type; their values are shown as text
                    Ok(query_def(
                        stmt.columns()
                            .iter()
                            .map(|c| {
                                let decl = c.decl_type().unwrap_or("TEXT");
                                result_column(c.name().to_string(), sqlite_logical(decl), Some(decl.to_string()))
                            })
                            .collect(),
                    ))
                })
                .await
            }
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let sql = match query {
            Query::Table { def, filter, sort } => {
                let cols: Vec<String> = def.properties_info.iter().map(|p| sqlite_quote(&p.property_name)).collect();
                sql_table_query(format!("SELECT {} FROM {}", cols.join(", "), sqlite_quote(&def.container_name)), filter, sort, sqlite_quote)
            }
            Query::Text(q) => query_text(q).to_string(),
        };
        let def = def.clone();
        self.run(move |conn| sqlite_stream(conn, &sql, &def, &sizer, &tx)).await
    }

    fn canceller(&self) -> Canceller {
        Canceller::Sqlite(self.interrupt.clone())
    }
}

/// Parsed `db.<collection>.find(filter, projection).sort(..).skip(n).limit(n)` or
/// `db.<collection>.aggregate([..])`. Arguments are JSON (MongoDB extended JSON for `$oid`,
/// `$date`, ...).
#[derive(Debug, PartialEq)]
pub struct MongoFind {
    pub collection: String,
    pub filter: Document,
    pub projection: Option<Document>,
    pub sort: Option<Document>,
    pub skip: Option<u64>,
    pub limit: Option<i64>,
}

#[derive(Debug, PartialEq)]
pub enum MongoQuery {
    Find(Box<MongoFind>),
    Aggregate { collection: String, pipeline: Vec<Document> },
}

/// Text between the parenthesis at `open` and its matching `)`, and the rest after it.
fn call_args(s: &str, open: usize) -> DMResult<(&str, &str)> {
    let (mut depth, mut in_str, mut escaped) = (0usize, false, false);
    for (i, ch) in s[open..].char_indices() {
        if in_str {
            match (escaped, ch) {
                (true, _) => escaped = false,
                (false, '\\') => escaped = true,
                (false, '"') => in_str = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_str = true,
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&s[open + 1..open + i], &s[open + i + 1..]));
                }
            }
            _ => {}
        }
    }
    Err(DMError::Error("unbalanced parentheses in the query".to_string()))
}

fn json_args(args: &str) -> DMResult<Vec<Bson>> {
    let values: Vec<serde_json::Value> = serde_json::from_str(&format!("[{args}]")).map_err(|e| DMError::Error(format!("query arguments must be JSON: {e}")))?;
    values.into_iter().map(|v| Bson::try_from(v).map_err(|e| DMError::Error(e.to_string()))).collect()
}

fn as_document(b: Bson) -> DMResult<Document> {
    match b {
        Bson::Document(d) => Ok(d),
        other => Err(DMError::Error(format!("expected a document, got {other}"))),
    }
}

fn as_number(b: Bson) -> DMResult<i64> {
    match b {
        Bson::Int32(i) => Ok(i as i64),
        Bson::Int64(i) => Ok(i),
        Bson::Double(f) if f.fract() == 0.0 => Ok(f as i64),
        other => Err(DMError::Error(format!("expected a number, got {other}"))),
    }
}

pub fn parse_mongo_query(text: &str) -> DMResult<MongoQuery> {
    let text = query_text(text);
    let rest = text.strip_prefix("db.").ok_or_else(|| DMError::Error("MongoDB queries start with db.<collection>".to_string()))?;
    let (pos, method) = [".find(", ".aggregate("]
        .iter()
        .filter_map(|m| rest.find(m).map(|p| (p, *m)))
        .min()
        .ok_or_else(|| DMError::Error("supported: db.<collection>.find(..) and db.<collection>.aggregate([..])".to_string()))?;
    let collection = rest[..pos].to_string();
    let (args, mut tail) = call_args(rest, pos + method.len() - 1)?;
    let mut args = json_args(args)?.into_iter();
    if method == ".aggregate(" {
        if !tail.trim().is_empty() {
            return Err(DMError::Error(format!("unexpected text after aggregate(..): {}", tail.trim())));
        }
        let pipeline: Vec<Document> = match args.next() {
            Some(Bson::Array(stages)) => stages.into_iter().map(as_document).collect::<DMResult<_>>()?,
            _ => return Err(DMError::Error("aggregate expects an array of stages".to_string())),
        };
        // VIEW-9 / FIX-2: $out and $merge write; MongoDB allows them only as top-level stages
        if pipeline.iter().any(|s| s.contains_key("$out") || s.contains_key("$merge")) {
            return Err(DMError::Error("only read-only queries can be run here ($out and $merge write data)".to_string()));
        }
        return Ok(MongoQuery::Aggregate { collection, pipeline });
    }
    let filter = args.next().map(as_document).transpose()?.unwrap_or_default();
    let projection = args.next().map(as_document).transpose()?;
    let (mut sort, mut skip, mut limit) = (None, None, None);
    loop {
        let t = tail.trim_start();
        if t.is_empty() {
            break;
        }
        let name_end = t.find('(').filter(|_| t.starts_with('.')).ok_or_else(|| DMError::Error(format!("unexpected text: {t}")))?;
        let (a, next) = call_args(t, name_end)?;
        let arg = json_args(a)?.into_iter().next().ok_or_else(|| DMError::Error(format!("{} needs an argument", &t[1..name_end])))?;
        match &t[1..name_end] {
            "sort" => sort = Some(as_document(arg)?),
            "skip" => skip = Some(as_number(arg)?.max(0) as u64),
            "limit" => limit = Some(as_number(arg)?),
            other => return Err(DMError::Error(format!("unsupported cursor method .{other}(..)"))),
        }
        tail = next;
    }
    Ok(MongoQuery::Find(Box::new(MongoFind { collection, filter, projection, sort, skip, limit })))
}

impl Mongodb {
    async fn query_cursor(&self, query: &Query, batch: u32, max: Option<i64>) -> DMResult<mongodb::Cursor<Document>> {
        let options = |projection, sort, skip, limit: Option<i64>| {
            let limit = match (limit, max) {
                (Some(l), Some(m)) => Some(l.min(m)),
                (l, m) => l.or(m),
            };
            mongodb::options::FindOptions::builder().projection(projection).sort(sort).skip(skip).limit(limit).batch_size(batch).build()
        };
        Ok(match query {
            Query::Table { def, filter, sort } => {
                let filter = not_empty(filter).map(|f| json_args(f).and_then(|mut a| as_document(a.pop().unwrap_or_default()))).transpose()?;
                let sort = sort.as_ref().map(|s| doc! { s.column.clone(): if s.descending { -1 } else { 1 } });
                self.db.collection::<Document>(&def.container_name).find(filter, options(None, sort, None, None)).await?
            }
            Query::Text(q) => match parse_mongo_query(q)? {
                MongoQuery::Find(f) => {
                    let MongoFind { collection, filter, projection, sort, skip, limit } = *f;
                    self.db.collection::<Document>(&collection).find(filter, options(projection, sort, skip, limit)).await?
                }
                MongoQuery::Aggregate { collection, mut pipeline } => {
                    if let Some(m) = max {
                        pipeline.push(doc! {"$limit": m});
                    }
                    self.db
                        .collection::<Document>(&collection)
                        .aggregate(pipeline, mongodb::options::AggregateOptions::builder().batch_size(batch).build())
                        .await?
                }
            },
        })
    }
}

#[async_trait]
impl QueryRunner for Mongodb {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        if let Query::Table { def, .. } = query {
            return Ok(def.clone());
        }
        // result fields come from analyzing a sample of the result, like a collection (DB-4)
        let sample = self.exec_settings.schema_sample.max(1) as i64;
        let mut cursor = self.query_cursor(query, sample.min(10_000) as u32, Some(sample)).await?;
        let mut analyzer = SchemaAnalyzer::new();
        while let Some(d) = cursor.try_next().await? {
            analyzer.add(&d);
        }
        Ok(query_def(analyzer.finish().into_iter().map(|(name, t, _, shape)| DataPropertyInfo { shape, ..result_column(name, t, None) }).collect()))
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let mut size = sizer.next();
        let mut cursor = self.query_cursor(query, size.min(u32::MAX as usize) as u32, None).await?;
        let mut total = 0u64;
        let mut batch = ColumnBatch::for_definition(def, size);
        while let Some(d) = cursor.try_next().await? {
            for (p, col) in def.properties_info.iter().zip(batch.columns.iter_mut()) {
                push_bson(col, d.get(&p.property_name))?;
            }
            batch.end_row();
            if batch.rows >= size {
                total += batch.rows as u64;
                size = sizer.next();
                tx.send(std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size)))
                    .await
                    .map_err(|_| DMError::Error("receiver stopped".to_string()))?;
            }
        }
        if !batch.is_empty() {
            total += batch.rows as u64;
            tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
        }
        Ok(total)
    }

    fn canceller(&self) -> Canceller {
        Canceller::Drop
    }
}

/// CQL type name of a result column, in the form `system_schema.columns.type` uses.
fn cql_type_name(t: &scylla::frame::response::result::ColumnType<'_>) -> String {
    use scylla::frame::response::result::{CollectionType, ColumnType};
    match t {
        ColumnType::Native(n) => format!("{n:?}").to_ascii_lowercase(),
        ColumnType::Collection { typ: CollectionType::List(e), .. } => format!("list<{}>", cql_type_name(e)),
        ColumnType::Collection { typ: CollectionType::Set(e), .. } => format!("set<{}>", cql_type_name(e)),
        ColumnType::Collection { typ: CollectionType::Map(k, v), .. } => format!("map<{}, {}>", cql_type_name(k), cql_type_name(v)),
        ColumnType::Tuple(ts) => format!("tuple<{}>", ts.iter().map(cql_type_name).collect::<Vec<_>>().join(", ")),
        ColumnType::UserDefinedType { definition, .. } => definition.name.to_string(),
        ColumnType::Vector { typ, dimensions } => format!("vector<{}, {dimensions}>", cql_type_name(typ)),
        _ => "blob".to_string(),
    }
}

#[async_trait]
impl QueryRunner for Scylla {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match query {
            Query::Table { def, .. } => Ok(def.clone()),
            Query::Text(q) => {
                let q = query_text(q);
                if !q.get(..6).is_some_and(|s| s.eq_ignore_ascii_case("select")) {
                    return Err(DMError::Error("only SELECT statements can be run here".to_string()));
                }
                let prepared = self.session.prepare(q).await.map_err(sc)?;
                Ok(query_def(
                    prepared
                        .get_current_result_set_col_specs()
                        .get()
                        .iter()
                        .map(|c| {
                            let ty = cql_type_name(c.typ());
                            result_column(c.name().to_string(), cql_logical(&ty), Some(ty))
                        })
                        .collect(),
                ))
            }
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let cql = match query {
            Query::Table { filter, sort, .. } if not_empty(filter).is_none() && sort.is_none() => return self.read_data(def, sizer, tx).await,
            Query::Table { def, filter, sort } => {
                let cols: Vec<String> = def.properties_info.iter().map(|p| cql_quote(&p.property_name)).collect();
                let mut q = format!("SELECT {} FROM {}", cols.join(", "), self.table(&def.container_name));
                if let Some(f) = not_empty(filter) {
                    q.push_str(&format!(" WHERE {f}"));
                }
                // CQL sorts only inside a partition: the filter has to fix the partition key
                if let Some(s) = sort {
                    q.push_str(&format!(" ORDER BY {} {}", cql_quote(&s.column), if s.descending { "DESC" } else { "ASC" }));
                }
                q
            }
            Query::Text(q) => query_text(q).to_string(),
        };
        let mut prepared = self.session.prepare(cql).await.map_err(sc)?;
        prepared.set_page_size(sizer.next().clamp(100, 10_000) as i32);
        let mut rows = self.session.execute_iter(prepared, ()).await.map_err(sc)?.rows_stream::<scylla::value::Row>().map_err(sc)?;
        let mut size = sizer.next();
        let mut batch = ColumnBatch::for_definition(def, size);
        let mut total = 0u64;
        while let Some(row) = rows.try_next().await.map_err(sc)? {
            for (col, v) in batch.columns.iter_mut().zip(row.columns.iter()) {
                push_cql(col, v.as_ref())?;
            }
            batch.end_row();
            if batch.rows >= size {
                total += batch.rows as u64;
                size = sizer.next();
                tx.send(std::mem::replace(&mut batch, ColumnBatch::for_definition(def, size)))
                    .await
                    .map_err(|_| DMError::Error("receiver stopped".to_string()))?;
            }
        }
        if !batch.is_empty() {
            total += batch.rows as u64;
            tx.send(batch).await.map_err(|_| DMError::Error("receiver stopped".to_string()))?;
        }
        Ok(total)
    }

    fn canceller(&self) -> Canceller {
        Canceller::Drop
    }
}

/// MySQL statements that only read (VIEW-9). Queries also run in a READ ONLY transaction, but
/// DDL commits implicitly even there and `INTO OUTFILE` writes files on the server, so both are
/// refused before anything is sent.
pub(crate) fn mysql_read_only(sql: &str) -> DMResult<()> {
    let lower = sql.trim_start().trim_start_matches('(').to_ascii_lowercase();
    let first = lower.split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or("");
    let reads = matches!(first, "select" | "with" | "table" | "values" | "show" | "explain" | "desc" | "describe");
    if !reads || lower.contains("outfile") || lower.contains("dumpfile") {
        return Err(DMError::Error("only read-only statements can be run here".to_string()));
    }
    Ok(())
}

#[async_trait]
impl QueryRunner for Mysql {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match query {
            Query::Table { def, .. } => Ok(def.clone()),
            Query::Text(q) => {
                let q = query_text(q);
                mysql_read_only(q)?;
                use mysql_async::prelude::Queryable;
                let stmt = self.conn.prep(q).await?;
                Ok(query_def(
                    stmt.columns()
                        .iter()
                        .map(|c| result_column(c.name_str().into_owned(), mysql_logical(c.column_type(), c.flags(), c.column_length()), Some(format!("{:?}", c.column_type()))))
                        .collect(),
                ))
            }
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let sql = match query {
            Query::Table { def, filter, sort } => sql_table_query(mysql_select(self.database(), def), filter, sort, mysql_quote),
            Query::Text(q) => query_text(q).to_string(),
        };
        // the filter is user text too
        mysql_read_only(&sql)?;
        let mut opts = mysql_async::TxOpts::default();
        opts.with_readonly(true);
        let mut t = self.conn.start_transaction(opts).await?;
        let n = mysql_stream(&mut t, &sql, def, &sizer, &tx).await?;
        t.commit().await?;
        Ok(n)
    }

    fn canceller(&self) -> Canceller {
        Canceller::Mysql(self.connection.clone(), self.conn.id())
    }
}

/// Oracle statements that only read (ORA-9). Queries also run in a READ ONLY transaction, but
/// DDL commits implicitly, `SELECT .. FOR UPDATE` locks rows and PL/SQL blocks (also inline
/// `WITH FUNCTION`) can do anything, so they are refused before anything is sent.
pub(crate) fn oracle_read_only(sql: &str) -> DMResult<()> {
    let lower = sql.trim_start().trim_start_matches('(').to_ascii_lowercase();
    let words: Vec<&str> = lower.split(|c: char| !c.is_ascii_alphanumeric() && c != '_').filter(|w| !w.is_empty()).collect();
    let reads = matches!(words.first(), Some(&"select") | Some(&"with"));
    let plsql = words.first() == Some(&"with") && matches!(words.get(1), Some(&"function") | Some(&"procedure"));
    let locks = words.windows(2).any(|w| w == ["for", "update"]);
    if !reads || plsql || locks {
        return Err(DMError::Error("only read-only statements can be run here".to_string()));
    }
    Ok(())
}

#[async_trait]
impl QueryRunner for Oracle {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match query {
            Query::Table { def, .. } => Ok(def.clone()),
            Query::Text(q) => {
                let q = query_text(q).to_string();
                oracle_read_only(&q)?;
                // column types without reading a row; no prefetch, or the execute itself would fetch
                // and compute an aggregate over the whole query
                let probe = format!("SELECT * FROM ({q}) WHERE 1 = 0");
                self.blocking(move |c| {
                    let mut stmt = c.statement(&probe).prefetch_rows(0).build()?;
                    let rows = stmt.query(&[])?;
                    Ok(query_def(
                        rows.column_info()
                            .iter()
                            .map(|ci| result_column(ci.name().to_string(), oracle_logical(ci.oracle_type()), Some(ci.oracle_type().to_string())))
                            .collect(),
                    ))
                })
                .await
            }
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        let sql = match query {
            Query::Table { def, filter, sort } => sql_table_query(oracle_select(self.schema(), def), filter, sort, oracle_quote),
            Query::Text(q) => query_text(q).to_string(),
        };
        // the filter is user text too
        oracle_read_only(&sql)?;
        self.end_read_tx().await?;
        let def = def.clone();
        self.blocking(move |c| {
            c.execute("SET TRANSACTION READ ONLY", &[])?;
            let n = oracle_stream(c, &sql, &def, &sizer, &tx);
            c.commit()?;
            n
        })
        .await
    }

    fn canceller(&self) -> Canceller {
        Canceller::Oracle(self.conn.clone())
    }
}

#[async_trait]
impl QueryRunner for DatabaseConnectionType {
    async fn describe(&mut self, query: &Query) -> DMResult<DataDefinition> {
        match self {
            DatabaseConnectionType::Postgresql(d) => d.describe(query).await,
            DatabaseConnectionType::Mongodb(d) => d.describe(query).await,
            DatabaseConnectionType::Sqlite(d) => d.describe(query).await,
            DatabaseConnectionType::Scylla(d) => d.describe(query).await,
            DatabaseConnectionType::Mysql(d) => d.describe(query).await,
            DatabaseConnectionType::Oracle(d) => d.describe(query).await,
        }
    }

    async fn fetch(&mut self, query: &Query, def: &DataDefinition, sizer: Arc<BatchSizer>, tx: mpsc::Sender<ColumnBatch>) -> DMResult<u64> {
        match self {
            DatabaseConnectionType::Postgresql(d) => d.fetch(query, def, sizer, tx).await,
            DatabaseConnectionType::Mongodb(d) => d.fetch(query, def, sizer, tx).await,
            DatabaseConnectionType::Sqlite(d) => d.fetch(query, def, sizer, tx).await,
            DatabaseConnectionType::Scylla(d) => d.fetch(query, def, sizer, tx).await,
            DatabaseConnectionType::Mysql(d) => d.fetch(query, def, sizer, tx).await,
            DatabaseConnectionType::Oracle(d) => d.fetch(query, def, sizer, tx).await,
        }
    }

    fn canceller(&self) -> Canceller {
        match self {
            DatabaseConnectionType::Postgresql(d) => d.canceller(),
            DatabaseConnectionType::Mongodb(d) => d.canceller(),
            DatabaseConnectionType::Sqlite(d) => d.canceller(),
            DatabaseConnectionType::Scylla(d) => d.canceller(),
            DatabaseConnectionType::Mysql(d) => d.canceller(),
            DatabaseConnectionType::Oracle(d) => d.canceller(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_find_with_cursor_methods() {
        let q = parse_mongo_query(r#"db.my.users.find({"age": {"$gt": 30}, "n": "a)b"}, {"name": 1}).sort({"name": -1}).skip(5).limit(10);"#).unwrap();
        assert_eq!(
            q,
            MongoQuery::Find(Box::new(MongoFind {
                collection: "my.users".into(),
                filter: doc! {"age": {"$gt": 30}, "n": "a)b"},
                projection: Some(doc! {"name": 1}),
                sort: Some(doc! {"name": -1}),
                skip: Some(5),
                limit: Some(10),
            }))
        );
        assert_eq!(
            parse_mongo_query("db.users.find()").unwrap(),
            MongoQuery::Find(Box::new(MongoFind { collection: "users".into(), filter: Document::new(), projection: None, sort: None, skip: None, limit: None }))
        );
    }

    #[test]
    fn view9_mysql_statements_that_write_are_refused() {
        for ok in ["SELECT * FROM t", "  (select 1)", "WITH x AS (SELECT 1) SELECT * FROM x", "show tables", "EXPLAIN SELECT 1"] {
            assert!(mysql_read_only(ok).is_ok(), "{ok}");
        }
        for bad in ["DROP TABLE t", "insert into t values (1)", "SELECT * FROM t INTO OUTFILE '/tmp/x'", "select 1 into dumpfile '/x'", "CALL p()", "SET @a = 1"] {
            assert!(mysql_read_only(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ora9_oracle_statements_that_write_are_refused() {
        for ok in ["SELECT * FROM t", " (select 1 from dual)", "WITH x AS (SELECT 1 a FROM dual) SELECT * FROM x", "select updated_for from t"] {
            assert!(oracle_read_only(ok).is_ok(), "{ok}");
        }
        for bad in [
            "DELETE FROM t",
            "DROP TABLE t",
            "SELECT * FROM t FOR UPDATE",
            "select * from t for  update nowait",
            "BEGIN DELETE FROM t; END;",
            "WITH FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END; SELECT f FROM dual",
            "MERGE INTO t USING dual ON (1=1) WHEN MATCHED THEN UPDATE SET a = 1",
            "CALL p()",
        ] {
            assert!(oracle_read_only(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn parses_aggregate_and_extended_json() {
        let q = parse_mongo_query(r#"db.orders.aggregate([{"$match": {"_id": {"$oid": "65f0c0ffee0000000000abcd"}}}, {"$count": "n"}])"#).unwrap();
        let MongoQuery::Aggregate { collection, pipeline } = q else { panic!("not an aggregate") };
        assert_eq!(collection, "orders");
        assert_eq!(pipeline.len(), 2);
        assert!(matches!(pipeline[0].get_document("$match").unwrap().get("_id"), Some(Bson::ObjectId(_))));
    }

    #[test]
    fn rejects_other_mongo_commands() {
        for q in ["db.users.deleteMany({})", "users.find()", "db.users.find({}).forEach(1)", "db.users.find({)"] {
            assert!(parse_mongo_query(q).is_err(), "{q}");
        }
    }

    #[test]
    fn fix2_view9_mongo_pipelines_that_write_are_rejected() {
        for q in [r#"db.users.aggregate([{"$match": {"vip": true}}, {"$out": "users"}])"#, r#"db.users.aggregate([{"$merge": {"into": "x"}}])"#] {
            assert!(parse_mongo_query(q).unwrap_err().to_string().contains("read-only"), "{q}");
        }
    }

    #[test]
    fn table_query_adds_filter_and_sort() {
        let s = Some(Sort { column: "na\"me".into(), descending: true });
        assert_eq!(
            sql_table_query("SELECT a FROM t".into(), &Some(" a > 1 ".into()), &s, quote_ident),
            "SELECT a FROM t WHERE (a > 1) ORDER BY \"na\"\"me\" DESC"
        );
        assert_eq!(sql_table_query("SELECT a FROM t".into(), &Some("  ".into()), &None, quote_ident), "SELECT a FROM t");
    }
}
