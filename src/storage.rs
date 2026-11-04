use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;

use rocksdb::{DBCompressionType, Direction, IteratorMode, Options, WriteBatch, DB};

use crate::error::{Error, Result};
use crate::key::{
    decode_primary_key, encode_index_value, encode_primary_key, index_meta_key, index_meta_prefix,
    index_prefix, index_table_prefix, meta_key, row_key, row_prefix, INDEX_META_PREFIX,
    META_PREFIX,
};
use crate::schema::{DataType, IndexSchema, TableSchema};
use crate::value::Value;

#[derive(Debug, Clone)]
pub(crate) struct StoredRow {
    pub primary_key: Value,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    index: String,
    key: Vec<u8>,
    value: Vec<u8>,
    enforce_unique: bool,
    display: String,
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
        for index in self.list_indexes_unlocked(table)? {
            batch.delete(index_meta_key(table, &index.name));
        }
        let index_prefix = index_table_prefix(table);
        let iter = self.db.iterator(IteratorMode::From(
            index_prefix.as_slice(),
            Direction::Forward,
        ));
        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&index_prefix) {
                break;
            }
            batch.delete(key);
        }
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

    pub fn list_indexes(&self, table: &str) -> Result<Vec<IndexSchema>> {
        self.get_schema(table)?;
        self.list_indexes_unlocked(table)
    }

    pub fn create_index(&self, table: &str, index: &IndexSchema) -> Result<bool> {
        let _guard = self.lock();
        let schema = self.get_schema(table)?;
        index.validate(&schema)?;

        if let Some((existing_table, existing)) = self.find_index_unlocked(&index.name)? {
            if existing_table != table {
                return Err(Error::IndexAlreadyExists(index.name.clone()));
            }
            if existing.ready {
                return Ok(false);
            }
            // A previous build did not finish. Remove its partial entries and
            // rebuild the index from the current rows.
            self.delete_index_data_unlocked(table, &existing.name)?;
            self.db.delete(index_meta_key(table, &existing.name))?;
        }

        let mut building = index.clone();
        building.ready = false;
        self.db.put(
            index_meta_key(table, &index.name),
            serde_json::to_vec(&building)?,
        )?;

        let result = (|| -> Result<()> {
            let rows = self.scan_rows(&schema)?;
            let mut batch = WriteBatch::default();
            let mut unique_keys = HashSet::new();
            for row in &rows {
                for entry in
                    self.index_entries(&schema, std::slice::from_ref(index), &row.values)?
                {
                    if entry.enforce_unique && !unique_keys.insert(entry.key.clone()) {
                        return Err(Error::DuplicateIndexValue {
                            index: entry.index,
                            value: entry.display,
                        });
                    }
                    batch.put(entry.key, entry.value);
                }
            }
            self.db.write(batch)?;
            let mut ready = index.clone();
            ready.ready = true;
            self.db.put(
                index_meta_key(table, &index.name),
                serde_json::to_vec(&ready)?,
            )?;
            Ok(())
        })();

        if result.is_err() {
            let _ = self.delete_index_data_unlocked(table, &index.name);
            let _ = self.db.delete(index_meta_key(table, &index.name));
        }
        result?;
        Ok(true)
    }

    pub fn drop_index(&self, name: &str) -> Result<Option<String>> {
        let _guard = self.lock();
        let Some((table, index)) = self.find_index_unlocked(name)? else {
            return Ok(None);
        };
        self.delete_index_data_unlocked(&table, &index.name)?;
        self.db.delete(index_meta_key(&table, &index.name))?;
        Ok(Some(table))
    }

    fn list_indexes_unlocked(&self, table: &str) -> Result<Vec<IndexSchema>> {
        let prefix = index_meta_prefix(table);
        let mut indexes = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(prefix.as_slice(), Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            let index_name = String::from_utf8(key[prefix.len()..].to_vec())
                .map_err(|_| Error::CorruptData("non-UTF-8 index name in catalog".into()))?;
            let schema: IndexSchema = serde_json::from_slice(&value)?;
            if schema.name != index_name {
                return Err(Error::CorruptData(format!(
                    "index catalog entry `{index_name}` contains index `{}`",
                    schema.name
                )));
            }
            indexes.push(schema);
        }
        indexes.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(indexes)
    }

    fn ready_indexes_unlocked(&self, table: &str) -> Result<Vec<IndexSchema>> {
        Ok(self
            .list_indexes_unlocked(table)?
            .into_iter()
            .filter(|index| index.ready)
            .collect())
    }

    fn find_index_unlocked(&self, name: &str) -> Result<Option<(String, IndexSchema)>> {
        let iter = self
            .db
            .iterator(IteratorMode::From(INDEX_META_PREFIX, Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(INDEX_META_PREFIX) {
                break;
            }
            let remainder = &key[INDEX_META_PREFIX.len()..];
            let Some(separator) = remainder.iter().position(|byte| *byte == b'/') else {
                return Err(Error::CorruptData(
                    "index catalog key is missing its table separator".into(),
                ));
            };
            let table = String::from_utf8(remainder[..separator].to_vec())
                .map_err(|_| Error::CorruptData("non-UTF-8 table name in index catalog".into()))?;
            let index_name = String::from_utf8(remainder[separator + 1..].to_vec())
                .map_err(|_| Error::CorruptData("non-UTF-8 index name in catalog".into()))?;
            let schema: IndexSchema = serde_json::from_slice(&value)?;
            if schema.name != index_name {
                return Err(Error::CorruptData(format!(
                    "index catalog entry `{index_name}` contains index `{}`",
                    schema.name
                )));
            }
            if schema.name == name {
                return Ok(Some((table, schema)));
            }
        }
        Ok(None)
    }

    fn delete_index_data_unlocked(&self, table: &str, index: &str) -> Result<()> {
        let mut batch = WriteBatch::default();
        self.delete_index_batch_unlocked(&mut batch, table, index)?;
        self.db.write(batch)?;
        Ok(())
    }

    fn delete_index_batch_unlocked(
        &self,
        batch: &mut WriteBatch,
        table: &str,
        index: &str,
    ) -> Result<()> {
        let prefix = index_prefix(table, index);
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
        batch.delete(index_meta_key(table, index));
        Ok(())
    }

    fn index_entries(
        &self,
        schema: &TableSchema,
        indexes: &[IndexSchema],
        values: &[Value],
    ) -> Result<Vec<IndexEntry>> {
        let primary_key = &values[schema.primary_key];
        let encoded_primary_key = encode_primary_key(primary_key)?;
        let mut entries = Vec::with_capacity(indexes.len());
        for index in indexes {
            let mut key = index_prefix(&schema.name, &index.name);
            let mut has_null = false;
            let mut display = Vec::with_capacity(index.columns.len());
            for name in &index.columns {
                let column_index = schema.column_index_or_error(name)?;
                let value = &values[column_index];
                has_null |= value.is_null();
                key.extend(encode_index_value(&schema.columns[column_index], value)?);
                display.push(format!("{name}={value}"));
            }
            let enforce_unique = index.unique && !has_null;
            if !index.unique || has_null {
                key.extend_from_slice(&encoded_primary_key);
            }
            entries.push(IndexEntry {
                index: index.name.clone(),
                key,
                value: encoded_primary_key.clone(),
                enforce_unique,
                display: display.join(", "),
            });
        }
        Ok(entries)
    }

    pub fn scan_index_rows(
        &self,
        schema: &TableSchema,
        prefix: &[u8],
        lower: &[u8],
        upper: Option<&[u8]>,
    ) -> Result<Vec<StoredRow>> {
        let primary_key_type = schema.primary_key_column().data_type;
        let mut rows = Vec::new();
        let iter = self
            .db
            .iterator(IteratorMode::From(lower, Direction::Forward));
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(prefix) {
                break;
            }
            if let Some(upper) = upper {
                if key.as_ref() >= upper {
                    break;
                }
            }
            let primary_key = decode_primary_key(&value, primary_key_type)?;
            let Some(row) = self.get_row(schema, &primary_key)? else {
                return Err(Error::CorruptData(format!(
                    "index entry points to missing row {primary_key}"
                )));
            };
            rows.push(row);
        }
        Ok(rows)
    }

    pub fn scan(&self, schema: &TableSchema) -> Result<Vec<StoredRow>> {
        self.scan_rows(schema)
    }

    /// Stream rows through a callback without materializing the whole table.
    /// Used by grouped/aggregate queries to keep memory bounded.
    pub fn for_each_row<F>(&self, schema: &TableSchema, mut callback: F) -> Result<()>
    where
        F: FnMut(&StoredRow) -> Result<()>,
    {
        let prefix = row_prefix(&schema.name);
        let primary_key_type = schema.primary_key_column().data_type;
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
            callback(&StoredRow {
                primary_key,
                values,
            })?;
        }
        Ok(())
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
        let indexes = self.ready_indexes_unlocked(&schema.name)?;
        let mut batch = WriteBatch::default();
        let mut keys_in_batch = HashSet::new();
        let mut unique_index_keys = HashSet::new();
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
            for entry in self.index_entries(schema, &indexes, values)? {
                if entry.enforce_unique {
                    let existing = unique_index_keys.contains(&entry.key)
                        || self.db.get(&entry.key)?.is_some();
                    if existing {
                        return Err(Error::DuplicateIndexValue {
                            index: entry.index,
                            value: entry.display,
                        });
                    }
                    unique_index_keys.insert(entry.key.clone());
                }
                batch.put(entry.key, entry.value);
            }
            batch.put(key, serde_json::to_vec(values)?);
        }

        self.db.write(batch)?;
        Ok(rows.len())
    }

    /// Apply an update function to every row. The function returns `None` to
    /// leave a row unchanged, or a full replacement row. All changes are
    /// written in one batch, after primary-key and unique-index constraints
    /// have been verified.
    pub fn update_rows<F>(&self, schema: &TableSchema, mut update: F) -> Result<usize>
    where
        F: FnMut(&[Value]) -> Result<Option<Vec<Value>>>,
    {
        let _guard = self.lock();
        let rows = self.scan_rows(schema)?;
        let indexes = self.ready_indexes_unlocked(&schema.name)?;
        let all_entries: Vec<Vec<IndexEntry>> = rows
            .iter()
            .map(|row| self.index_entries(schema, &indexes, &row.values))
            .collect::<Result<_>>()?;
        let primary_key_column = schema.primary_key_column();

        let existing_keys: HashSet<Vec<u8>> = rows
            .iter()
            .map(|row| row_key(&schema.name, &row.primary_key))
            .collect::<Result<_>>()?;

        struct Pending {
            old_key: Vec<u8>,
            new_key: Vec<u8>,
            payload: Vec<u8>,
            old_entries: Vec<IndexEntry>,
            new_entries: Vec<IndexEntry>,
        }

        let mut pending = Vec::new();
        let mut seen_new_keys = HashSet::new();
        for (row, old_entries) in rows.iter().zip(&all_entries) {
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
            let new_entries = self.index_entries(schema, &indexes, &new_values)?;
            pending.push(Pending {
                old_key,
                new_key,
                payload: serde_json::to_vec(&new_values)?,
                old_entries: old_entries.clone(),
                new_entries,
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

        // Unique indexes need the same release-before-acquire treatment as
        // primary keys, otherwise swapping two unique values would fail.
        for index in &indexes {
            if !index.unique {
                continue;
            }
            let mut occupied: HashSet<Vec<u8>> = all_entries
                .iter()
                .flatten()
                .filter(|entry| entry.index == index.name && entry.enforce_unique)
                .map(|entry| entry.key.clone())
                .collect();
            for change in &pending {
                for entry in &change.old_entries {
                    if entry.index == index.name && entry.enforce_unique {
                        occupied.remove(&entry.key);
                    }
                }
            }
            for change in &pending {
                for entry in &change.new_entries {
                    if entry.index == index.name
                        && entry.enforce_unique
                        && !occupied.insert(entry.key.clone())
                    {
                        return Err(Error::DuplicateIndexValue {
                            index: entry.index.clone(),
                            value: entry.display.clone(),
                        });
                    }
                }
            }
        }

        let mut batch = WriteBatch::default();
        for change in &pending {
            if change.new_key != change.old_key {
                batch.delete(&change.old_key);
            }
            for entry in &change.old_entries {
                batch.delete(&entry.key);
            }
        }
        for change in &pending {
            batch.put(&change.new_key, &change.payload);
            for entry in &change.new_entries {
                batch.put(&entry.key, &entry.value);
            }
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
        let indexes = self.ready_indexes_unlocked(&schema.name)?;
        let mut batch = WriteBatch::default();
        let mut affected = 0;
        for row in rows {
            if predicate(&row.values)? {
                batch.delete(row_key(&schema.name, &row.primary_key)?);
                for entry in self.index_entries(schema, &indexes, &row.values)? {
                    batch.delete(entry.key);
                }
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
