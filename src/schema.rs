use serde::{Deserialize, Serialize};
use sqlparser::ast::DataType as SqlDataType;

use crate::error::{Error, Result};

/// Supported column types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Real,
    Text,
    Boolean,
}

impl DataType {
    pub fn sql_name(&self) -> &'static str {
        match self {
            DataType::Integer => "INT",
            DataType::Real => "REAL",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
        }
    }

    pub fn from_ast(data_type: &SqlDataType) -> Result<DataType> {
        match data_type {
            SqlDataType::Int(_)
            | SqlDataType::Integer(_)
            | SqlDataType::BigInt(_)
            | SqlDataType::SmallInt(_)
            | SqlDataType::Int2(_)
            | SqlDataType::Int4(_)
            | SqlDataType::Int8(_)
            | SqlDataType::Int16
            | SqlDataType::Int32
            | SqlDataType::Int64 => Ok(DataType::Integer),
            SqlDataType::Real
            | SqlDataType::Float4
            | SqlDataType::Float8
            | SqlDataType::Float32
            | SqlDataType::Float64
            | SqlDataType::Float(_)
            | SqlDataType::Double(_)
            | SqlDataType::DoublePrecision => Ok(DataType::Real),
            SqlDataType::Text
            | SqlDataType::TinyText
            | SqlDataType::MediumText
            | SqlDataType::LongText
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_)
            | SqlDataType::Character(_)
            | SqlDataType::CharacterVarying(_)
            | SqlDataType::CharVarying(_)
            | SqlDataType::Nvarchar(_)
            | SqlDataType::String(_) => Ok(DataType::Text),
            SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),
            other => Err(Error::Unsupported(format!(
                "column type `{other}` (supported types: INT, REAL, TEXT, BOOLEAN)"
            ))),
        }
    }

    pub fn supports_primary_key(&self) -> bool {
        matches!(self, DataType::Integer | DataType::Text)
    }
}

/// A column in a table definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// A table definition persisted in the RocksDB catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    /// Index into `columns`.
    pub primary_key: usize,
}

impl TableSchema {
    pub fn primary_key_column(&self) -> &Column {
        &self.columns[self.primary_key]
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    pub fn column_index_or_error(&self, name: &str) -> Result<usize> {
        self.column_index(name)
            .ok_or_else(|| Error::ColumnNotFound {
                table: self.name.clone(),
                column: name.to_string(),
            })
    }

    pub fn validate(&self) -> Result<()> {
        validate_identifier(&self.name)?;
        if self.columns.is_empty() {
            return Err(Error::Unsupported(format!(
                "table `{}` must have at least one column",
                self.name
            )));
        }
        for (index, column) in self.columns.iter().enumerate() {
            validate_identifier(&column.name)?;
            if self.columns[..index]
                .iter()
                .any(|other| other.name == column.name)
            {
                return Err(Error::DuplicateColumn(column.name.clone()));
            }
        }
        if self.primary_key >= self.columns.len() {
            return Err(Error::Internal(format!(
                "table `{}` has an out-of-range primary key index",
                self.name
            )));
        }
        let primary_key = self.primary_key_column();
        if primary_key.nullable {
            return Err(Error::NullPrimaryKey(primary_key.name.clone()));
        }
        if !primary_key.data_type.supports_primary_key() {
            return Err(Error::InvalidPrimaryKeyType(primary_key.name.clone()));
        }
        Ok(())
    }

    pub fn describe(&self) -> String {
        let columns = self
            .columns
            .iter()
            .map(|column| {
                if column.name == self.primary_key_column().name {
                    format!(
                        "{} {} PRIMARY KEY",
                        column.name,
                        column.data_type.sql_name()
                    )
                } else if column.nullable {
                    format!("{} {}", column.name, column.data_type.sql_name())
                } else {
                    format!("{} {} NOT NULL", column.name, column.data_type.sql_name())
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("CREATE TABLE {} ({columns})", self.name)
    }
}

/// A secondary index definition persisted in the RocksDB catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    /// Indexed column names, in index order.
    pub columns: Vec<String>,
    pub unique: bool,
    /// An index is only visible to readers and maintained by writers once its
    /// initial build has completed.
    #[serde(default = "default_true")]
    pub ready: bool,
}

fn default_true() -> bool {
    true
}

impl IndexSchema {
    pub fn validate(&self, table: &TableSchema) -> Result<()> {
        validate_identifier(&self.name)?;
        if self.columns.is_empty() {
            return Err(Error::Unsupported(format!(
                "index `{}` must include at least one column",
                self.name
            )));
        }
        for (position, name) in self.columns.iter().enumerate() {
            table.column_index_or_error(name)?;
            if self.columns[..position].iter().any(|other| other == name) {
                return Err(Error::DuplicateColumn(name.clone()));
            }
        }
        Ok(())
    }

    pub fn describe(&self, table: &str) -> String {
        format!(
            "CREATE {}INDEX {} ON {} ({})",
            if self.unique { "UNIQUE " } else { "" },
            self.name,
            table,
            self.columns.join(", ")
        )
    }
}

/// Identifiers are intentionally restricted to simple ASCII names. This keeps
/// key layout (`row/<table>/<pk>`) unambiguous and avoids quoting edge cases.
pub fn validate_identifier(name: &str) -> Result<()> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.bytes().enumerate().all(|(index, byte)| match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => true,
            b'0'..=b'9' => index > 0,
            _ => false,
        });
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidIdentifier(name.to_string()))
    }
}
