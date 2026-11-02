use serde::{Deserialize, Deserializer, Serialize};

use super::value::LogicalType;

/// Description of one container (table / collection) written by the provider and read by the
/// receiver (the "json data definition" from the process overview).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataDefinition {
    /// Source name; `schema.table` for PostgreSQL, `table` for SQLite, `collection` for MongoDB.
    pub container_name: String,
    pub properties_info: Vec<DataPropertyInfo>,
    #[serde(default)]
    pub foreign_keys: Vec<ForeignKeyInfo>,
    #[serde(default)]
    pub indexes: Vec<IndexInfo>,
    /// Partition key columns (ScyllaDB source, or a per-table override for a ScyllaDB target);
    /// empty = the first identifier column (SCY-8).
    #[serde(default)]
    pub partition_key: Vec<String>,
}

/// Stable shape of a json column (SCY-10): lets a ScyllaDB receiver use a collection or a UDT
/// instead of json text.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum JsonShape {
    /// Array whose elements all have this scalar type.
    List(LogicalType),
    /// Object whose fields always have these scalar types (missing fields are null).
    Object(Vec<(String, LogicalType)>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataPropertyInfo {
    pub property_name: String,
    /// Logical type shared by all databases.
    pub data_type: LogicalType,
    /// Native type in the source database (informational, used by the provider to read it).
    #[serde(default)]
    pub source_type: Option<String>,
    pub is_nullable: bool,
    pub is_identifier: bool,
    #[serde(default, deserialize_with = "deserialize_empty_string_as_none")]
    pub reference_container_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_empty_string_as_none")]
    pub reference_property_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shape: Option<JsonShape>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub reference_container_name: String,
    pub reference_columns: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndexInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

fn deserialize_empty_string_as_none<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    Ok(s.filter(|x| !x.is_empty()))
}

/// Last segment of a container name (`public.users` -> `users`), used as the target name.
pub fn short_name(container: &str) -> &str {
    container.rsplit('.').next().unwrap_or(container)
}

impl DataDefinition {
    pub fn new(container_name: String, properties_info: Vec<DataPropertyInfo>) -> Self {
        DataDefinition {
            container_name,
            properties_info,
            foreign_keys: Vec::new(),
            indexes: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    /// Name of the container in the target database.
    pub fn target_name(&self) -> &str {
        short_name(&self.container_name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.properties_info.iter().position(|p| p.property_name == name)
    }

    /// Indexes of the identifier (primary key) columns, in definition order.
    pub fn identifier_indexes(&self) -> Vec<usize> {
        self.properties_info
            .iter()
            .enumerate()
            .filter(|(_, p)| p.is_identifier)
            .map(|(i, _)| i)
            .collect()
    }

    pub fn identifier_names(&self) -> Vec<&str> {
        self.properties_info
            .iter()
            .filter(|p| p.is_identifier)
            .map(|p| p.property_name.as_str())
            .collect()
    }

    /// Partition key columns: the explicit list, else the first identifier column.
    pub fn partition_key_names(&self) -> Vec<&str> {
        if !self.partition_key.is_empty() {
            return self.partition_key.iter().map(|s| s.as_str()).collect();
        }
        self.identifier_names().into_iter().take(1).collect()
    }

    /// Containers this one depends on (foreign keys), without self references.
    pub fn dependencies(&self) -> Vec<String> {
        let mut deps: Vec<String> = Vec::new();
        for fk in &self.foreign_keys {
            if fk.reference_container_name != self.container_name
                && !deps.contains(&fk.reference_container_name)
            {
                deps.push(fk.reference_container_name.clone());
            }
        }
        for p in &self.properties_info {
            if let Some(r) = &p.reference_container_name {
                if r != &self.container_name && !deps.contains(r) {
                    deps.push(r.clone());
                }
            }
        }
        deps
    }

    /// Fills the per-property reference fields from the foreign key list.
    pub fn sync_property_references(&mut self) {
        for fk in &self.foreign_keys {
            for (col, ref_col) in fk.columns.iter().zip(fk.reference_columns.iter()) {
                if let Some(p) = self.properties_info.iter_mut().find(|p| &p.property_name == col) {
                    if p.reference_container_name.is_none() {
                        p.reference_container_name = Some(fk.reference_container_name.clone());
                        p.reference_property_name = Some(ref_col.clone());
                    }
                }
            }
        }
    }
}
