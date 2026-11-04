use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, DB};

use crate::error::{Error, Result};
use crate::key::{decode_primary_key, meta_key, row_key, row_prefix, META_PREFIX};
use crate::schema::{DataType, TableSchema};
use crate::value::Value;

#[derive(Debug, Clone)]
pub(crate) struct StoredRow {
    pub primary_key: Value,
    pub values: Vec<Value>,
}

/// Thin RocksDB storage layer for tables.
///
/// A table has one catalog entry (`meta/table/<name>`) and one key per row
/// (`row/<name>/<encoded-primary-key>`). Mutations take an in-process mutation
/// lock so uniqueness checks and the following batch write are atomic within a
/// single engine instance.
pub(crate) struct Database {
    db: DB,
    write_lock: Mutex<()>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = Options::default();
        options.create_if_missing(true);
        // The crate is built without optional codec dependencies, so avoid
        // requesting a codec that may not be compiled in.
        options.set_compression_type(DBCompressionType::None);
        Self::open_with_options(path, options)
    }

    pub fn open_with_options(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let db = DB::open(&options, path.as_ref())?;
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }

    pub fn create_table(&self, schema: &TableSchema) -> Result<bool> {
        schema.validate()?;
        let _guard = self.lock();
        let key = meta_key(&schema.name);
        if self.db.get(&key)?.is_some() {
            return Ok(false);
        }
        let payload = serde_json::to_vec(schema)?;
        self.db.put(key, payload)?;
        Ok(true)
    }

    pub fn drop_table(&self, table: &str) -> Result<bool> {
        let _guard = self.lock();
        let key = meta_key(table);
        if self.db.get(&key)?.is_none() {
            return Ok(false);
        }

        let prefix = row_prefix(table);
        let mut batch = WriteBatch::default();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete(key);
        }
        batch.delete(key);
        self.db.write(batch)?;
        Ok(true)
    }

    pub fn get_schema(&self, table: &str) -> Result<TableSchema> {
        let Some(bytes) = self.db.get(meta_key(table))? else {
            return Err(Error::TableNotFound(table.to_string()));
        };
        let schema: TableSchema = serde_json::from_slice(&bytes)?;
        if schema.name != table {
            return Err(Error::CorruptData(format!(
                "catalog entry `{table}` contains schema for `{}`",
                schema.name
            )));
        }
        schema.validate()?;
        Ok(schema)
    }

    pub fn list_schemas(&self) -> Result<Vec<TableSchema>> {
        let mut schemas = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(META_PREFIX, Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(META_PREFIX) {
                break;
            }
            let table = String::from_utf8(key[META_PREFIX.len()..].to_vec())
                .map_err(|_| Error::CorruptData("non-UTF-8 table name in catalog".into()))?;
            let mut schema: TableSchema = serde_json::from_slice(&value)?;
            schema.name = table;
            schema.validate()?;
            schemas.push(schema);
        }
        schemas.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(schemas)
    }

    pub fn scan(&self, schema: &TableSchema) -> Result<Vec<StoredRow>> {
        self.scan_rows(schema)
    }

    pub fn get_row(&self, schema: &TableSchema, primary_key: &Value) -> Result<Option<StoredRow>> {
        let key = row_key(&schema.name, primary_key)?;
        let Some(bytes) = self.db.get(key)? else {
            return Ok(None);
        };
        let values = self.decode_row(schema, &bytes)?;
        Ok(Some(StoredRow {
            primary_key: primary_key.clone(),
            values,
        }))
    }

    pub fn insert_rows(&self, schema: &TableSchema, rows: Vec<Vec<Value>>) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }
        let _guard = self.lock();
        let mut batch = WriteBatch::default();
        let mut keys_in_batch = HashSet::new();
        let primary_key_column = schema.primary_key_column();

        for values in &rows {
            self.validate_row(schema, values)?;
            let primary_key = &values[schema.primary_key];
            if primary_key.is_null() {
                return Err(Error::NullPrimaryKey(primary_key_column.name.clone()));
            }
            let key = row_key(&schema.name, primary_key)?;
            if !keys_in_batch.insert(key.clone()) {
                return Err(Error::DuplicatePrimaryKey(primary_key.to_string()));
            }
            if self.db.get(&key)?.is_some() {
                return Err(Error::DuplicatePrimaryKey(primary_key.to_string()));
            }
            batch.put(key, serde_json::to_vec(values)?);
        }

        self.db.write(batch)?;
        Ok(rows.len())
    }

    /// Apply an update function to every row. The function returns `None` to
    /// leave a row unchanged, or a full replacement row. All changes are
    /// written in one batch, after primary-key uniqueness has been verified.
    pub fn update_rows<F>(&self, schema: &TableSchema, mut update: F) -> Result<usize>
    where
        F: FnMut(&[Value]) -> Result<Option<Vec<Value>>>,
    {
        let _guard = self.lock();
        let rows = self.scan_rows(schema)?;
        let primary_key_column = schema.primary_key_column();

        let existing_keys: HashSet<Vec<u8>> = rows
            .iter()
            .map(|row| row_key(&schema.name, &row.primary_key))
            .collect::<Result<_>>()?;

        struct Pending {
            old_key: Vec<u8>,
            new_key: Vec<u8>,
            payload: Vec<u8>,
        }

        let mut pending = Vec::new();
        let mut seen_new_keys = HashSet::new();
        for row in &rows {
            let Some(new_values) = update(&row.values)? else {
                continue;
            };
            self.validate_row(schema, &new_values)?;
            let primary_key = &new_values[schema.primary_key];
            if primary_key.is_null() {
                return Err(Error::NullPrimaryKey(primary_key_column.name.clone()));
            }
            let old_key = row_key(&schema.name, &row.primary_key)?;
            let new_key = row_key(&schema.name, primary_key)?;
            if !seen_new_keys.insert(new_key.clone()) {
                return Err(Error::DuplicatePrimaryKey(primary_key.to_string()));
            }
            pending.push(Pending {
                old_key,
                new_key,
                payload: serde_json::to_vec(&new_values)?,
            });
        }

        if pending.is_empty() {
            return Ok(0);
        }

        // Rows being updated may release their old keys, so only keys held by
        // untouched rows are conflicts. All deletes run before all puts so that
        // primary-key swaps remain correct.
        let updated_old_keys: HashSet<&[u8]> = pending
            .iter()
            .map(|change| change.old_key.as_slice())
            .collect();
        for change in &pending {
            if change.new_key != change.old_key
                && !updated_old_keys.contains(change.new_key.as_slice())
                && existing_keys.contains(&change.new_key)
            {
                return Err(Error::DuplicatePrimaryKey(
                    self.primary_key_for_error(schema, &change.new_key),
                ));
            }
        }

        let mut batch = WriteBatch::default();
        for change in &pending {
            if change.new_key != change.old_key {
                batch.delete(&change.old_key);
            }
        }
        for change in &pending {
            batch.put(&change.new_key, &change.payload);
        }
        self.db.write(batch)?;
        Ok(pending.len())
    }

    pub fn delete_rows<F>(&self, schema: &TableSchema, mut predicate: F) -> Result<usize>
    where
        F: FnMut(&[Value]) -> Result<bool>,
    {
        let _guard = self.lock();
        let rows = self.scan_rows(schema)?;
        let mut batch = WriteBatch::default();
        let mut affected = 0;
        for row in rows {
            if predicate(&row.values)? {
                batch.delete(row_key(&schema.name, &row.primary_key)?);
                affected += 1;
            }
        }
        if affected > 0 {
            self.db.write(batch)?;
        }
        Ok(affected)
    }

    fn scan_rows(&self, schema: &TableSchema) -> Result<Vec<StoredRow>> {
        let prefix = row_prefix(&schema.name);
        let primary_key_type = schema.primary_key_column().data_type;
        let mut rows = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let primary_key = decode_primary_key(&key[prefix.len()..], primary_key_type)?;
            let values = self.decode_row(schema, &value)?;
            rows.push(StoredRow {
                primary_key,
                values,
            });
        }
        Ok(rows)
    }

    fn decode_row(&self, schema: &TableSchema, bytes: &[u8]) -> Result<Vec<Value>> {
        let values: Vec<Value> = serde_json::from_slice(bytes)
            .map_err(|error| Error::CorruptData(format!("invalid row JSON: {error}")))?;
        if values.len() != schema.columns.len() {
            return Err(Error::CorruptData(format!(
                "table `{}` row has {} values, expected {}",
                schema.name,
                values.len(),
                schema.columns.len()
            )));
        }
        Ok(values)
    }

    fn validate_row(&self, schema: &TableSchema, values: &[Value]) -> Result<()> {
        if values.len() != schema.columns.len() {
            return Err(Error::InvalidRow {
                expected: schema.columns.len(),
                actual: values.len(),
            });
        }
        for (column, value) in schema.columns.iter().zip(values) {
            if value.is_null() {
                if !column.nullable {
                    return Err(Error::NotNullConstraint(column.name.clone()));
                }
                continue;
            }
            let matches = matches!(
                (column.data_type, value),
                (DataType::Integer, Value::Integer(_))
                    | (DataType::Real, Value::Real(_))
                    | (DataType::Text, Value::Text(_))
                    | (DataType::Boolean, Value::Boolean(_))
            );
            if !matches {
                return Err(Error::TypeMismatch {
                    column: column.name.clone(),
                    expected: column.data_type.sql_name().to_string(),
                    actual: value.type_name().to_string(),
                });
            }
        }
        Ok(())
    }

    fn primary_key_for_error(&self, schema: &TableSchema, key: &[u8]) -> String {
        let prefix = row_prefix(&schema.name);
        key.strip_prefix(prefix.as_slice())
            .and_then(|bytes| decode_primary_key(bytes, schema.primary_key_column().data_type).ok())
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<unknown>".to_string())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
