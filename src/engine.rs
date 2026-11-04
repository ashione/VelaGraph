use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, ColumnOption, CreateIndex, CreateTable,
    DataType as SqlDataType, DateTimeField, Delete, Distinct, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, Ident, Insert, Interval, LimitClause,
    ObjectName, ObjectType, OrderBy, OrderByKind, OrderBySort, Query, Select, SelectItem, SetExpr,
    Statement, TableConstraint, TableFactor, TableObject, TableWithJoins, TypedString,
    UnaryOperator, Update,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::{Error, Result};
use crate::key::{encode_index_value, index_prefix, key_upper_bound};
use crate::schema::{Column, DataType, IndexSchema, TableSchema};
use crate::storage::Database;
use crate::value::Value;

/// The result of a single statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    Rows(QueryResult),
    Modified {
        affected: usize,
    },
    Created {
        table: String,
        existed: bool,
    },
    Dropped {
        table: String,
        existed: bool,
    },
    CreatedIndex {
        table: String,
        index: String,
        existed: bool,
    },
    DroppedIndex {
        index: String,
        existed: bool,
    },
}

/// Rows and column headers produced by a SELECT statement.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

/// A RocksDB-backed SQL table engine.
///
/// ```no_run
/// use rocksdb_sql_table::TableEngine;
///
/// # fn main() -> rocksdb_sql_table::Result<()> {
/// let engine = TableEngine::open("./data")?;
/// engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")?;
/// engine.execute("INSERT INTO users (id, name) VALUES (1, 'Ada')")?;
/// let results = engine.execute("SELECT name FROM users WHERE id = 1")?;
/// println!("{results:?}");
/// # Ok(())
/// # }
/// ```
pub struct TableEngine {
    db: Database,
}

impl TableEngine {
    /// Open (or create) a database directory.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Ok(Self {
            db: Database::open(path)?,
        })
    }

    /// Open a database with custom RocksDB options.
    pub fn open_with_options(
        path: impl AsRef<std::path::Path>,
        options: rocksdb::Options,
    ) -> Result<Self> {
        Ok(Self {
            db: Database::open_with_options(path, options)?,
        })
    }

    /// Parse and execute one or more `;`-separated SQL statements.
    ///
    /// Statements are validated by the parser before execution, then executed
    /// in order. Each statement writes one RocksDB batch, but a multi-statement
    /// script is not one transaction: an error aborts the remaining statements
    /// and leaves earlier statements committed.
    pub fn execute(&self, sql: &str) -> Result<Vec<Outcome>> {
        if sql.trim().is_empty() {
            return Ok(Vec::new());
        }
        let statements = Parser::parse_sql(&GenericDialect {}, sql)?;
        statements
            .iter()
            .map(|statement| self.execute_statement(statement))
            .collect()
    }

    fn execute_statement(&self, statement: &Statement) -> Result<Outcome> {
        match statement {
            Statement::Query(query) => self.execute_query(query),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update(update) => self.execute_update(update),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::CreateIndex(create) => self.execute_create_index(create),
            Statement::Drop {
                object_type,
                if_exists,
                names,
                ..
            } => self.execute_drop(object_type, *if_exists, names),
            other => Err(Error::Unsupported(format!("statement `{other}`"))),
        }
    }

    /// List all table definitions, sorted by name.
    pub fn list_tables(&self) -> Result<Vec<TableSchema>> {
        self.db.list_schemas()
    }

    /// Return one table definition.
    pub fn table_schema(&self, table: &str) -> Result<TableSchema> {
        self.db.get_schema(table)
    }

    /// List the secondary indexes defined on a table, sorted by index name.
    pub fn list_indexes(&self, table: &str) -> Result<Vec<IndexSchema>> {
        self.db.list_indexes(table)
    }

    fn execute_create_table(&self, create: &CreateTable) -> Result<Outcome> {
        if create.or_replace {
            return Err(Error::Unsupported("CREATE OR REPLACE TABLE".into()));
        }
        if create.temporary || create.external {
            return Err(Error::Unsupported(
                "temporary and external tables are not supported".into(),
            ));
        }
        if create.query.is_some() {
            return Err(Error::Unsupported("CREATE TABLE ... AS SELECT".into()));
        }
        if create.like.is_some() || create.clone.is_some() {
            return Err(Error::Unsupported(
                "CREATE TABLE ... LIKE/CLONE is not supported".into(),
            ));
        }
        if create.primary_key.is_some() {
            return Err(Error::Unsupported(
                "the ClickHouse table-level PRIMARY KEY syntax is not supported; use a column constraint".into(),
            ));
        }

        let schema = schema_from_create(create)?;
        let created = self.db.create_table(&schema)?;
        if !created && !create.if_not_exists {
            return Err(Error::TableAlreadyExists(schema.name));
        }
        Ok(Outcome::Created {
            table: schema.name,
            existed: !created,
        })
    }

    fn execute_create_index(&self, create: &CreateIndex) -> Result<Outcome> {
        if create.concurrently || create.r#async {
            return Err(Error::Unsupported("CREATE INDEX CONCURRENTLY/ASYNC".into()));
        }
        if create.using.is_some() {
            return Err(Error::Unsupported("CREATE INDEX ... USING".into()));
        }
        if !create.include.is_empty() {
            return Err(Error::Unsupported("CREATE INDEX ... INCLUDE".into()));
        }
        if create.nulls_distinct == Some(false) {
            return Err(Error::Unsupported(
                "CREATE INDEX ... NULLS NOT DISTINCT".into(),
            ));
        }
        if !create.with.is_empty()
            || create.predicate.is_some()
            || !create.index_options.is_empty()
            || !create.alter_options.is_empty()
        {
            return Err(Error::Unsupported(
                "CREATE INDEX options/predicates are not supported".into(),
            ));
        }

        let table = single_name(&create.table_name)?;
        let name = create
            .name
            .as_ref()
            .ok_or_else(|| Error::Unsupported("CREATE INDEX requires an index name".into()))?;
        let name = single_name(name)?;
        let schema = self.db.get_schema(&table)?;

        let mut columns = Vec::with_capacity(create.columns.len());
        for index_column in &create.columns {
            if index_column.operator_class.is_some() {
                return Err(Error::Unsupported("index operator classes".into()));
            }
            if index_column.column.options.sort.is_some()
                || index_column.column.options.nulls_first.is_some()
                || index_column.column.with_fill.is_some()
            {
                return Err(Error::Unsupported("index column ordering options".into()));
            }
            let column = &schema.columns[resolve_column(&index_column.column.expr, &schema, None)?];
            if columns.contains(&column.name) {
                return Err(Error::DuplicateColumn(column.name.clone()));
            }
            columns.push(column.name.clone());
        }

        let index = IndexSchema {
            name,
            columns,
            unique: create.unique,
            ready: true,
        };
        index.validate(&schema)?;
        let created = self.db.create_index(&table, &index)?;
        if !created && !create.if_not_exists {
            return Err(Error::IndexAlreadyExists(index.name));
        }
        Ok(Outcome::CreatedIndex {
            table,
            index: index.name,
            existed: !created,
        })
    }

    fn execute_drop(
        &self,
        object_type: &ObjectType,
        if_exists: bool,
        names: &[ObjectName],
    ) -> Result<Outcome> {
        match object_type {
            ObjectType::Table => {
                if names.len() != 1 {
                    return Err(Error::Unsupported(
                        "dropping more than one table per statement is not supported".into(),
                    ));
                }
                let table = single_name(&names[0])?;
                let existed = self.db.drop_table(&table)?;
                if !existed && !if_exists {
                    return Err(Error::TableNotFound(table));
                }
                Ok(Outcome::Dropped { table, existed })
            }
            ObjectType::Index => {
                if names.len() != 1 {
                    return Err(Error::Unsupported(
                        "dropping more than one index per statement is not supported".into(),
                    ));
                }
                let index = single_name(&names[0])?;
                let existed = self.db.drop_index(&index)?.is_some();
                if !existed && !if_exists {
                    return Err(Error::IndexNotFound(index));
                }
                Ok(Outcome::DroppedIndex { index, existed })
            }
            other => Err(Error::Unsupported(format!(
                "DROP {other} (only DROP TABLE and DROP INDEX are supported)"
            ))),
        }
    }

    fn execute_insert(&self, insert: &Insert) -> Result<Outcome> {
        if insert.overwrite {
            return Err(Error::Unsupported("INSERT OVERWRITE".into()));
        }
        if insert.or.is_some() || insert.ignore || insert.replace_into {
            return Err(Error::Unsupported("INSERT OR/IGNORE/REPLACE".into()));
        }
        if insert.on.is_some() {
            return Err(Error::Unsupported(
                "INSERT ... ON CONFLICT/DUPLICATE KEY".into(),
            ));
        }
        if insert.returning.is_some() || insert.output.is_some() {
            return Err(Error::Unsupported("INSERT ... RETURNING/OUTPUT".into()));
        }
        if insert.priority.is_some() {
            return Err(Error::Unsupported("INSERT priority modifiers".into()));
        }
        if insert.partitioned.is_some() || !insert.after_columns.is_empty() {
            return Err(Error::Unsupported("INSERT ... PARTITION".into()));
        }
        if insert.settings.is_some() || insert.format_clause.is_some() {
            return Err(Error::Unsupported("INSERT format/settings".into()));
        }
        if insert.multi_table_insert_type.is_some()
            || !insert.multi_table_into_clauses.is_empty()
            || !insert.multi_table_when_clauses.is_empty()
            || insert.multi_table_else_clause.is_some()
        {
            return Err(Error::Unsupported("multi-table INSERT".into()));
        }
        if !insert.assignments.is_empty() {
            return Err(Error::Unsupported("INSERT ... SET".into()));
        }

        let table = match &insert.table {
            TableObject::TableName(name) => single_name(name)?,
            other => {
                return Err(Error::Unsupported(format!(
                    "INSERT target `{other}` (only named tables are supported)"
                )))
            }
        };
        let schema = self.db.get_schema(&table)?;

        let Some(source) = insert.source.as_ref() else {
            return Err(Error::Unsupported("INSERT without VALUES".into()));
        };
        if source.with.is_some()
            || source.order_by.is_some()
            || source.limit_clause.is_some()
            || source.fetch.is_some()
            || !source.locks.is_empty()
        {
            return Err(Error::Unsupported(
                "INSERT source must be a plain VALUES list".into(),
            ));
        }
        let values = match source.body.as_ref() {
            SetExpr::Values(values) => values,
            other => {
                return Err(Error::Unsupported(format!(
                    "INSERT source `{other}` (only VALUES is supported)"
                )))
            }
        };

        let target_columns = if insert.columns.is_empty() {
            (0..schema.columns.len()).collect::<Vec<_>>()
        } else {
            let mut indexes = Vec::with_capacity(insert.columns.len());
            let mut seen = HashSet::new();
            for name in &insert.columns {
                let column = single_name(name)?;
                let index = schema.column_index_or_error(&column)?;
                if !seen.insert(index) {
                    return Err(Error::DuplicateColumn(column));
                }
                indexes.push(index);
            }
            indexes
        };

        let mut rows = Vec::with_capacity(values.rows.len());
        for (row_index, row) in values.rows.iter().enumerate() {
            let expressions = &row.content;
            if expressions.len() != target_columns.len() {
                return Err(Error::InvalidRow {
                    expected: target_columns.len(),
                    actual: expressions.len(),
                });
            }
            let mut row_values = vec![Value::Null; schema.columns.len()];
            for (expression, column_index) in expressions.iter().zip(&target_columns) {
                row_values[*column_index] = eval_literal(expression).map_err(|error| {
                    Error::InvalidValue(format!("row {}: {error}", row_index + 1))
                })?;
            }
            coerce_row(&schema, &mut row_values)?;
            rows.push(row_values);
        }
        if rows.is_empty() {
            return Err(Error::Unsupported(
                "INSERT must contain at least one row".into(),
            ));
        }

        let affected = self.db.insert_rows(&schema, rows)?;
        Ok(Outcome::Modified { affected })
    }

    fn execute_update(&self, update: &Update) -> Result<Outcome> {
        if update.from.is_some() {
            return Err(Error::Unsupported("UPDATE ... FROM".into()));
        }
        if update.returning.is_some() {
            return Err(Error::Unsupported("UPDATE ... RETURNING".into()));
        }
        if !update.order_by.is_empty() || update.limit.is_some() {
            return Err(Error::Unsupported("UPDATE ... ORDER BY/LIMIT".into()));
        }
        if update.or.is_some() {
            return Err(Error::Unsupported("UPDATE OR ...".into()));
        }
        let (schema, alias) = self.table_from_join(&update.table)?;

        let mut assignments = Vec::with_capacity(update.assignments.len());
        let mut seen = HashSet::new();
        for assignment in &update.assignments {
            let name = match &assignment.target {
                AssignmentTarget::ColumnName(name) => single_name(name)?,
                AssignmentTarget::Tuple(_) => {
                    return Err(Error::Unsupported("tuple UPDATE assignments".into()))
                }
            };
            let index = schema.column_index_or_error(&name)?;
            if !seen.insert(index) {
                return Err(Error::DuplicateColumn(name));
            }
            assignments.push((index, eval_literal(&assignment.value)?));
        }
        if assignments.is_empty() {
            return Err(Error::Unsupported("UPDATE without assignments".into()));
        }

        let selection = update.selection.as_ref();
        let affected = self.db.update_rows(&schema, |values| {
            if !row_matches(selection, &schema, alias.as_deref(), values)? {
                return Ok(None);
            }
            let mut new_values = values.to_vec();
            for (index, value) in &assignments {
                new_values[*index] = value.clone();
            }
            coerce_row(&schema, &mut new_values)?;
            Ok(Some(new_values))
        })?;
        Ok(Outcome::Modified { affected })
    }

    fn execute_delete(&self, delete: &Delete) -> Result<Outcome> {
        if !delete.tables.is_empty() {
            return Err(Error::Unsupported("multi-table DELETE".into()));
        }
        if delete.using.is_some() {
            return Err(Error::Unsupported("DELETE ... USING".into()));
        }
        if delete.returning.is_some() {
            return Err(Error::Unsupported("DELETE ... RETURNING".into()));
        }
        if !delete.order_by.is_empty() || delete.limit.is_some() {
            return Err(Error::Unsupported("DELETE ... ORDER BY/LIMIT".into()));
        }
        let tables = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables)
            | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
        };
        if tables.len() != 1 {
            return Err(Error::Unsupported(
                "DELETE must target exactly one table".into(),
            ));
        }
        let (schema, alias) = self.table_from_join(&tables[0])?;
        let selection = delete.selection.as_ref();
        let affected = self.db.delete_rows(&schema, |values| {
            row_matches(selection, &schema, alias.as_deref(), values)
        })?;
        Ok(Outcome::Modified { affected })
    }

    fn execute_query(&self, query: &Query) -> Result<Outcome> {
        if query.with.is_some() {
            return Err(Error::Unsupported("WITH (CTE)".into()));
        }
        if query.fetch.is_some() {
            return Err(Error::Unsupported("FETCH".into()));
        }
        if !query.locks.is_empty() {
            return Err(Error::Unsupported("locking reads".into()));
        }
        if query.for_clause.is_some() {
            return Err(Error::Unsupported("FOR XML/JSON".into()));
        }
        if query.settings.is_some() || query.format_clause.is_some() {
            return Err(Error::Unsupported("dialect-specific SELECT options".into()));
        }
        if !query.pipe_operators.is_empty() {
            return Err(Error::Unsupported("pipe operators".into()));
        }

        let select = match query.body.as_ref() {
            SetExpr::Select(select) => select.as_ref(),
            other => {
                return Err(Error::Unsupported(format!(
                    "query body `{other}` (only a single SELECT is supported)"
                )))
            }
        };
        self.validate_select(select)?;
        let (schema, alias) = self.table_from_select(select)?;
        let distinct = parse_distinct(select)?;
        let projections = resolve_projection(&select.projection, &schema, alias.as_deref())?;
        let group_by = parse_group_by(select, &schema, alias.as_deref())?;
        let has_group_by = !group_by.is_empty();
        let having = match select.having.as_ref() {
            Some(expression) => Some(resolve_having(
                expression,
                &schema,
                alias.as_deref(),
                &group_by,
            )?),
            None => None,
        };
        let has_aggregate = projections.iter().any(ResolvedProjection::is_aggregate)
            || having
                .as_ref()
                .is_some_and(|having| !having.aggregates.is_empty());
        if having.is_some() && !has_aggregate {
            return Err(Error::Unsupported(
                "HAVING requires an aggregate query".into(),
            ));
        }
        if has_group_by && !has_aggregate {
            return Err(Error::Unsupported(
                "GROUP BY without an aggregate projection".into(),
            ));
        }
        if has_aggregate {
            if group_by.is_empty() {
                if projections
                    .iter()
                    .any(|projection| !projection.is_aggregate())
                {
                    return Err(Error::Unsupported(
                        "cannot mix aggregate and non-aggregate projections without GROUP BY"
                            .into(),
                    ));
                }
                if query.order_by.is_some() {
                    return Err(Error::Unsupported(
                        "ORDER BY is not supported together with aggregate functions".into(),
                    ));
                }
            } else {
                for projection in &projections {
                    if let ResolvedProjection::Column { index, .. } = projection {
                        if !group_by.contains(index) {
                            return Err(Error::Unsupported(
                                "non-aggregate projection must appear in GROUP BY".into(),
                            ));
                        }
                    }
                }
            }
        }

        let order_keys = parse_order_by(query.order_by.as_ref(), &schema, alias.as_deref())?;
        if has_group_by {
            for key in &order_keys {
                if !group_by.contains(&key.index) {
                    return Err(Error::Unsupported(
                        "ORDER BY in a grouped query must use a GROUP BY column".into(),
                    ));
                }
            }
        }
        let (limit, offset) = parse_limit_offset(query)?;

        // A top-level `pk = literal` predicate can use a direct RocksDB point
        // lookup; everything else scans the table's row prefix.
        let point_lookup = point_lookup(select, &schema, alias.as_deref())?;

        if has_aggregate {
            let mut aggregates = AggregateAccumulator::new(
                &group_by,
                &projections,
                having
                    .as_ref()
                    .map(|having| having.aggregates.as_slice())
                    .unwrap_or(&[]),
            );
            match point_lookup {
                PointLookup::Missing => {}
                PointLookup::Value(ref primary_key) => {
                    if let Some(row) = self.db.get_row(&schema, primary_key)? {
                        if row_matches(
                            select.selection.as_ref(),
                            &schema,
                            alias.as_deref(),
                            &row.values,
                        )? {
                            aggregates.consume(&row, &schema, alias.as_deref(), &projections)?;
                        }
                    }
                }
                PointLookup::Scan => {
                    let indexes = self.db.list_indexes(&schema.name)?;
                    match plan_index_scan(select, &schema, alias.as_deref(), &indexes)? {
                        Some(plan) => {
                            for row in self.db.scan_index_rows(
                                &schema,
                                &plan.prefix,
                                &plan.lower,
                                plan.upper.as_deref(),
                            )? {
                                if row_matches(
                                    select.selection.as_ref(),
                                    &schema,
                                    alias.as_deref(),
                                    &row.values,
                                )? {
                                    aggregates.consume(
                                        &row,
                                        &schema,
                                        alias.as_deref(),
                                        &projections,
                                    )?;
                                }
                            }
                        }
                        None => {
                            self.db.for_each_row(&schema, |row| {
                                if row_matches(
                                    select.selection.as_ref(),
                                    &schema,
                                    alias.as_deref(),
                                    &row.values,
                                )? {
                                    aggregates.consume(
                                        row,
                                        &schema,
                                        alias.as_deref(),
                                        &projections,
                                    )?;
                                }
                                Ok(())
                            })?;
                        }
                    }
                }
            }
            let mut output_rows =
                aggregates.finish(&group_by, &projections, &order_keys, having.as_ref())?;
            if distinct {
                let mut seen = HashSet::new();
                output_rows.retain(|row| {
                    seen.insert(row.iter().map(Value::dedupe_key).collect::<Vec<_>>())
                });
            }
            paginate(&mut output_rows, limit, offset);
            return Ok(Outcome::Rows(QueryResult {
                columns: projections.iter().map(|item| item.header()).collect(),
                rows: output_rows,
            }));
        }

        let scanned = match point_lookup {
            PointLookup::Missing => Vec::new(),
            PointLookup::Value(primary_key) => self
                .db
                .get_row(&schema, &primary_key)?
                .into_iter()
                .collect(),
            PointLookup::Scan => {
                let indexes = self.db.list_indexes(&schema.name)?;
                match plan_index_scan(select, &schema, alias.as_deref(), &indexes)? {
                    Some(plan) => self.db.scan_index_rows(
                        &schema,
                        &plan.prefix,
                        &plan.lower,
                        plan.upper.as_deref(),
                    )?,
                    None => self.db.scan(&schema)?,
                }
            }
        };
        let mut rows = Vec::with_capacity(scanned.len());
        for row in scanned {
            if row_matches(
                select.selection.as_ref(),
                &schema,
                alias.as_deref(),
                &row.values,
            )? {
                rows.push(row);
            }
        }

        sort_rows(&mut rows, &order_keys);
        let mut projected = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut values = Vec::with_capacity(projections.len());
            for projection in &projections {
                values.push(match projection {
                    ResolvedProjection::Column { index, .. } => row.values[*index].clone(),
                    _ => unreachable!("aggregate projections are handled above"),
                });
            }
            projected.push(values);
        }
        if distinct {
            let mut seen = HashSet::new();
            projected
                .retain(|row| seen.insert(row.iter().map(Value::dedupe_key).collect::<Vec<_>>()));
        }
        paginate(&mut projected, limit, offset);
        Ok(Outcome::Rows(QueryResult {
            columns: projections.iter().map(|item| item.header()).collect(),
            rows: projected,
        }))
    }

    fn validate_select(&self, select: &Select) -> Result<()> {
        if select.top.is_some() {
            return Err(Error::Unsupported("SELECT TOP".into()));
        }
        if select.into.is_some() {
            return Err(Error::Unsupported("SELECT INTO".into()));
        }
        if !select.lateral_views.is_empty() {
            return Err(Error::Unsupported("LATERAL VIEW".into()));
        }
        if select.prewhere.is_some() {
            return Err(Error::Unsupported("PREWHERE".into()));
        }
        if let GroupByExpr::All(_) = &select.group_by {
            return Err(Error::Unsupported("GROUP BY ALL".into()));
        }
        if let GroupByExpr::Expressions(_, modifiers) = &select.group_by {
            if !modifiers.is_empty() {
                return Err(Error::Unsupported("GROUP BY modifiers".into()));
            }
        }
        if !select.named_window.is_empty() || select.qualify.is_some() {
            return Err(Error::Unsupported("WINDOW/QUALIFY".into()));
        }
        if !select.cluster_by.is_empty()
            || !select.distribute_by.is_empty()
            || !select.sort_by.is_empty()
        {
            return Err(Error::Unsupported("CLUSTER/DISTRIBUTE/SORT BY".into()));
        }
        Ok(())
    }

    fn table_from_select(&self, select: &Select) -> Result<(TableSchema, Option<String>)> {
        if select.from.len() != 1 {
            return Err(Error::Unsupported(
                "SELECT must read exactly one table".into(),
            ));
        }
        self.table_from_join(&select.from[0])
    }

    fn table_from_join(&self, table: &TableWithJoins) -> Result<(TableSchema, Option<String>)> {
        if !table.joins.is_empty() {
            return Err(Error::Unsupported("JOIN".into()));
        }
        let TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            partitions,
            ..
        } = &table.relation
        else {
            return Err(Error::Unsupported(format!(
                "table source `{}` (only named tables are supported)",
                table.relation
            )));
        };
        if args.is_some() {
            return Err(Error::Unsupported("table-valued function arguments".into()));
        }
        if !with_hints.is_empty() || version.is_some() || !partitions.is_empty() {
            return Err(Error::Unsupported(
                "table hints, versions and partitions".into(),
            ));
        }
        let table_name = single_name(name)?;
        let schema = self.db.get_schema(&table_name)?;
        let alias_name = match alias {
            Some(alias) => {
                if !alias.columns.is_empty() {
                    return Err(Error::Unsupported("table column aliases".into()));
                }
                Some(alias.name.value.clone())
            }
            None => None,
        };
        Ok((schema, alias_name))
    }
}

enum PointLookup {
    Scan,
    Missing,
    Value(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexPredicateOp {
    Eq,
    Gt,
    GtEq,
    Lt,
    LtEq,
}

#[derive(Debug, Clone)]
struct IndexPredicate {
    column: usize,
    op: IndexPredicateOp,
    value: Value,
}

struct IndexScanPlan {
    prefix: Vec<u8>,
    lower: Vec<u8>,
    upper: Option<Vec<u8>>,
    score: usize,
}

fn plan_index_scan(
    select: &Select,
    schema: &TableSchema,
    alias: Option<&str>,
    indexes: &[IndexSchema],
) -> Result<Option<IndexScanPlan>> {
    let Some(selection) = select.selection.as_ref() else {
        return Ok(None);
    };
    let mut conjuncts = Vec::new();
    collect_conjuncts(selection, &mut conjuncts);
    let mut predicates = Vec::new();
    for conjunct in conjuncts {
        if let Some(predicate) = parse_index_predicate(conjunct, schema, alias)? {
            predicates.push(predicate);
        }
    }
    if predicates.is_empty() {
        return Ok(None);
    }

    let mut best = None;
    for index in indexes.iter().filter(|index| index.ready) {
        let Some(plan) = build_index_plan(index, schema, &predicates)? else {
            continue;
        };
        if best
            .as_ref()
            .is_none_or(|best: &IndexScanPlan| plan.score > best.score)
        {
            best = Some(plan);
        }
    }
    Ok(best)
}

fn collect_conjuncts<'a>(expression: &'a Expr, conjuncts: &mut Vec<&'a Expr>) {
    let expression = strip_nested(expression);
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expression
    {
        collect_conjuncts(left, conjuncts);
        collect_conjuncts(right, conjuncts);
    } else {
        conjuncts.push(expression);
    }
}

fn parse_index_predicate(
    expression: &Expr,
    schema: &TableSchema,
    alias: Option<&str>,
) -> Result<Option<IndexPredicate>> {
    let Expr::BinaryOp { left, op, right } = strip_nested(expression) else {
        return Ok(None);
    };
    let (column_expression, literal_expression, op) =
        match (is_column_expression(left), is_column_expression(right)) {
            (true, false) => (left.as_ref(), right.as_ref(), op),
            (false, true) => (right.as_ref(), left.as_ref(), flip_comparison(op)),
            _ => return Ok(None),
        };
    let op = match op {
        BinaryOperator::Eq => IndexPredicateOp::Eq,
        BinaryOperator::Gt => IndexPredicateOp::Gt,
        BinaryOperator::GtEq => IndexPredicateOp::GtEq,
        BinaryOperator::Lt => IndexPredicateOp::Lt,
        BinaryOperator::LtEq => IndexPredicateOp::LtEq,
        _ => return Ok(None),
    };
    let column = resolve_column(column_expression, schema, alias)?;
    let literal = eval_literal(literal_expression)?;
    if literal.is_null() {
        return Ok(None);
    }
    let Some(value) = coerce_index_literal(&schema.columns[column], &literal) else {
        return Ok(None);
    };
    Ok(Some(IndexPredicate { column, op, value }))
}

fn flip_comparison(op: &BinaryOperator) -> &BinaryOperator {
    match op {
        BinaryOperator::Lt => &BinaryOperator::Gt,
        BinaryOperator::LtEq => &BinaryOperator::GtEq,
        BinaryOperator::Gt => &BinaryOperator::Lt,
        BinaryOperator::GtEq => &BinaryOperator::LtEq,
        other => other,
    }
}

fn coerce_index_literal(column: &Column, value: &Value) -> Option<Value> {
    match (column.data_type, value) {
        (DataType::Integer, Value::Integer(value)) => Some(Value::Integer(*value)),
        // SQL compares an INT column to a REAL literal after converting the
        // integer to f64. Above 2^53 that conversion can make multiple
        // integers compare equal to one literal, so an exact index probe
        // would be wrong; fall back to a scan for these predicates.
        (DataType::Integer, Value::Real(_)) => None,
        (DataType::Real, Value::Integer(value)) => Some(Value::Real(*value as f64)),
        (DataType::Real, Value::Real(value)) => Some(Value::Real(*value)),
        (DataType::Text, Value::Text(value)) => Some(Value::Text(value.clone())),
        (DataType::Boolean, Value::Boolean(value)) => Some(Value::Boolean(*value)),
        _ => None,
    }
}

fn build_index_plan(
    index: &IndexSchema,
    schema: &TableSchema,
    predicates: &[IndexPredicate],
) -> Result<Option<IndexScanPlan>> {
    let mut equality_values = Vec::new();
    let mut range_predicate = None;
    for name in &index.columns {
        let column = schema.column_index_or_error(name)?;
        if let Some(predicate) = predicates
            .iter()
            .find(|predicate| predicate.column == column && predicate.op == IndexPredicateOp::Eq)
        {
            equality_values.push(predicate.value.clone());
            continue;
        }
        range_predicate = predicates
            .iter()
            .find(|predicate| predicate.column == column)
            .cloned();
        break;
    }

    if equality_values.is_empty() && range_predicate.is_none() {
        return Ok(None);
    }

    let mut prefix = index_prefix(&schema.name, &index.name);
    for (position, value) in equality_values.iter().enumerate() {
        let column = schema.column_index_or_error(&index.columns[position])?;
        prefix.extend(encode_index_value(&schema.columns[column], value)?);
    }

    let has_range = range_predicate.is_some();
    let (lower, upper) = if let Some(range) = range_predicate {
        let column = schema.column_index_or_error(&index.columns[equality_values.len()])?;
        let mut bound = prefix.clone();
        bound.extend(encode_index_value(&schema.columns[column], &range.value)?);
        match range.op {
            IndexPredicateOp::Eq => unreachable!("equality predicates are consumed above"),
            IndexPredicateOp::Gt => (
                key_upper_bound(&bound).unwrap_or_else(|| bound.clone()),
                key_upper_bound(&prefix),
            ),
            IndexPredicateOp::GtEq => (bound, key_upper_bound(&prefix)),
            IndexPredicateOp::Lt => (prefix.clone(), Some(bound)),
            IndexPredicateOp::LtEq => (prefix.clone(), key_upper_bound(&bound)),
        }
    } else {
        (prefix.clone(), key_upper_bound(&prefix))
    };

    Ok(Some(IndexScanPlan {
        prefix,
        lower,
        upper,
        score: equality_values.len() * 2 + usize::from(has_range),
    }))
}

#[derive(Debug, Clone)]
struct SortKey {
    index: usize,
    descending: bool,
    nulls_first: bool,
}

#[derive(Debug, Clone)]
enum ResolvedProjection {
    Column { index: usize, header: String },
    CountStar { header: String },
    CountColumn { index: usize, header: String },
    Sum { expression: Expr, header: String },
    Avg { expression: Expr, header: String },
    Min { expression: Expr, header: String },
    Max { expression: Expr, header: String },
}

impl ResolvedProjection {
    fn is_aggregate(&self) -> bool {
        !matches!(self, ResolvedProjection::Column { .. })
    }

    fn header(&self) -> String {
        match self {
            ResolvedProjection::Column { header, .. }
            | ResolvedProjection::CountStar { header }
            | ResolvedProjection::CountColumn { header, .. }
            | ResolvedProjection::Sum { header, .. }
            | ResolvedProjection::Avg { header, .. }
            | ResolvedProjection::Min { header, .. }
            | ResolvedProjection::Max { header, .. } => header.clone(),
        }
    }
}

struct RowContext<'a> {
    schema: &'a TableSchema,
    alias: Option<&'a str>,
    values: &'a [Value],
}

impl<'a> RowContext<'a> {
    fn new(schema: &'a TableSchema, alias: Option<&'a str>, values: &'a [Value]) -> Self {
        Self {
            schema,
            alias,
            values,
        }
    }
}

fn schema_from_create(create: &CreateTable) -> Result<TableSchema> {
    let table_name = single_name(&create.name)?;
    if create.columns.is_empty() {
        return Err(Error::Unsupported(format!(
            "table `{table_name}` must declare at least one column"
        )));
    }

    let mut columns = Vec::with_capacity(create.columns.len());
    let mut primary_key = None;
    for definition in &create.columns {
        let name = definition.name.value.clone();
        crate::schema::validate_identifier(&name)?;
        if columns.iter().any(|column: &Column| column.name == name) {
            return Err(Error::DuplicateColumn(name));
        }
        let data_type = DataType::from_ast(&definition.data_type)?;
        let mut nullable = true;
        let mut column_primary_key = false;
        for option in &definition.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Null => nullable = true,
                ColumnOption::PrimaryKey(_) => {
                    if primary_key.is_some() {
                        return Err(Error::MultiplePrimaryKeys(table_name));
                    }
                    column_primary_key = true;
                    nullable = false;
                }
                other => {
                    return Err(Error::Unsupported(format!(
                        "column option `{other}` on `{name}`"
                    )))
                }
            }
        }
        if column_primary_key {
            primary_key = Some(columns.len());
        }
        columns.push(Column {
            name,
            data_type,
            nullable,
        });
    }

    for constraint in &create.constraints {
        match constraint {
            TableConstraint::PrimaryKey(constraint) => {
                if primary_key.is_some() {
                    return Err(Error::MultiplePrimaryKeys(table_name));
                }
                if constraint.columns.len() != 1 {
                    return Err(Error::CompositePrimaryKey);
                }
                let Expr::Identifier(identifier) = &constraint.columns[0].column.expr else {
                    return Err(Error::Unsupported(
                        "primary key expressions are not supported".into(),
                    ));
                };
                let index = columns
                    .iter()
                    .position(|column| column.name == identifier.value)
                    .ok_or_else(|| Error::ColumnNotFound {
                        table: table_name.clone(),
                        column: identifier.value.clone(),
                    })?;
                columns[index].nullable = false;
                primary_key = Some(index);
            }
            other => {
                return Err(Error::Unsupported(format!(
                    "table constraint `{other}` (only PRIMARY KEY is supported)"
                )))
            }
        }
    }

    let primary_key = primary_key.ok_or_else(|| Error::MissingPrimaryKey(table_name.clone()))?;
    if !columns[primary_key].data_type.supports_primary_key() {
        return Err(Error::InvalidPrimaryKeyType(
            columns[primary_key].name.clone(),
        ));
    }
    let schema = TableSchema {
        name: table_name,
        columns,
        primary_key,
    };
    schema.validate()?;
    Ok(schema)
}

fn single_name(name: &ObjectName) -> Result<String> {
    if name.0.len() != 1 {
        return Err(Error::Unsupported(format!(
            "qualified name `{name}` (only one-part names are supported)"
        )));
    }
    name.0[0]
        .as_ident()
        .map(|identifier| identifier.value.clone())
        .ok_or_else(|| Error::Unsupported(format!("dynamic object name `{name}`")))
}

fn parse_distinct(select: &Select) -> Result<bool> {
    match &select.distinct {
        None | Some(Distinct::All) => Ok(false),
        Some(Distinct::Distinct) => Ok(true),
        Some(other) => Err(Error::Unsupported(format!("DISTINCT `{other}`"))),
    }
}

fn parse_group_by(
    select: &Select,
    schema: &TableSchema,
    alias: Option<&str>,
) -> Result<Vec<usize>> {
    match &select.group_by {
        GroupByExpr::Expressions(expressions, modifiers) => {
            if !modifiers.is_empty() {
                return Err(Error::Unsupported("GROUP BY modifiers".into()));
            }
            let mut columns = Vec::with_capacity(expressions.len());
            for expression in expressions {
                columns.push(resolve_column(expression, schema, alias)?);
            }
            Ok(columns)
        }
        GroupByExpr::All(_) => Err(Error::Unsupported("GROUP BY ALL".into())),
    }
}

struct ResolvedHaving {
    expression: Expr,
    schema: TableSchema,
    aggregates: Vec<ResolvedProjection>,
}

fn resolve_having(
    expression: &Expr,
    schema: &TableSchema,
    alias: Option<&str>,
    group_by: &[usize],
) -> Result<ResolvedHaving> {
    let mut aggregates = Vec::new();
    let mut columns = group_by
        .iter()
        .map(|index| schema.columns[*index].clone())
        .collect::<Vec<_>>();
    let expression = resolve_having_expression(
        expression,
        schema,
        alias,
        group_by,
        &mut aggregates,
        &mut columns,
    )?;
    let schema = TableSchema {
        name: "__having".to_string(),
        columns,
        primary_key: 0,
    };
    Ok(ResolvedHaving {
        expression,
        schema,
        aggregates,
    })
}

fn resolve_having_expression(
    expression: &Expr,
    schema: &TableSchema,
    alias: Option<&str>,
    group_by: &[usize],
    aggregates: &mut Vec<ResolvedProjection>,
    columns: &mut Vec<Column>,
) -> Result<Expr> {
    match strip_nested(expression) {
        Expr::Function(function)
            if object_name_is(&function.name, "count")
                || object_name_is(&function.name, "sum")
                || object_name_is(&function.name, "avg")
                || object_name_is(&function.name, "min")
                || object_name_is(&function.name, "max") =>
        {
            let projection = projection_expression(expression, None, schema, alias)?;
            let data_type = having_aggregate_type(&projection, schema, alias)?;
            let name = format!("__having_agg_{}", aggregates.len());
            if columns.iter().any(|column| column.name == name) {
                return Err(Error::Unsupported(format!(
                    "HAVING synthetic column `{name}` conflicts with a GROUP BY column"
                )));
            }
            columns.push(Column {
                name: name.clone(),
                data_type,
                nullable: true,
            });
            aggregates.push(projection);
            Ok(Expr::Identifier(Ident::new(name)))
        }
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let index = resolve_column(expression, schema, alias)?;
            if !group_by.contains(&index) {
                return Err(Error::Unsupported(format!(
                    "HAVING column `{}` must appear in GROUP BY",
                    schema.columns[index].name
                )));
            }
            Ok(Expr::Identifier(Ident::new(
                schema.columns[index].name.clone(),
            )))
        }
        Expr::Value(_) | Expr::TypedString(_) | Expr::Interval(_) => {
            Ok(strip_nested(expression).clone())
        }
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(resolve_having_expression(
            inner, schema, alias, group_by, aggregates, columns,
        )?))),
        Expr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
            op: *op,
            expr: Box::new(resolve_having_expression(
                expr, schema, alias, group_by, aggregates, columns,
            )?),
        }),
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(resolve_having_expression(
                left, schema, alias, group_by, aggregates, columns,
            )?),
            op: op.clone(),
            right: Box::new(resolve_having_expression(
                right, schema, alias, group_by, aggregates, columns,
            )?),
        }),
        Expr::IsNull(expr) => Ok(Expr::IsNull(Box::new(resolve_having_expression(
            expr, schema, alias, group_by, aggregates, columns,
        )?))),
        Expr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(resolve_having_expression(
            expr, schema, alias, group_by, aggregates, columns,
        )?))),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => Ok(Expr::Like {
            negated: *negated,
            any: *any,
            expr: Box::new(resolve_having_expression(
                expr, schema, alias, group_by, aggregates, columns,
            )?),
            pattern: Box::new(resolve_having_expression(
                pattern, schema, alias, group_by, aggregates, columns,
            )?),
            escape_char: match escape_char {
                Some(escape) => Some(Box::new(resolve_having_expression(
                    escape, schema, alias, group_by, aggregates, columns,
                )?)),
                None => None,
            },
        }),
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => Ok(Expr::Between {
            expr: Box::new(resolve_having_expression(
                expr, schema, alias, group_by, aggregates, columns,
            )?),
            negated: *negated,
            low: Box::new(resolve_having_expression(
                low, schema, alias, group_by, aggregates, columns,
            )?),
            high: Box::new(resolve_having_expression(
                high, schema, alias, group_by, aggregates, columns,
            )?),
        }),
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let expr =
                resolve_having_expression(expr, schema, alias, group_by, aggregates, columns)?;
            let list = list
                .iter()
                .map(|item| {
                    resolve_having_expression(item, schema, alias, group_by, aggregates, columns)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Expr::InList {
                expr: Box::new(expr),
                list,
                negated: *negated,
            })
        }
        other => Err(Error::Unsupported(format!("HAVING expression `{other}`"))),
    }
}

fn having_aggregate_type(
    projection: &ResolvedProjection,
    schema: &TableSchema,
    alias: Option<&str>,
) -> Result<DataType> {
    Ok(match projection {
        ResolvedProjection::CountStar { .. } | ResolvedProjection::CountColumn { .. } => {
            DataType::Integer
        }
        ResolvedProjection::Sum { .. } | ResolvedProjection::Avg { .. } => DataType::Real,
        ResolvedProjection::Min { expression, .. } | ResolvedProjection::Max { expression, .. } => {
            match validate_aggregate_argument(expression, schema, alias, "HAVING")? {
                ScalarType::Null | ScalarType::Text => DataType::Text,
                ScalarType::Integer => DataType::Integer,
                ScalarType::Real => DataType::Real,
                ScalarType::Boolean => DataType::Boolean,
            }
        }
        ResolvedProjection::Column { .. } => {
            return Err(Error::Internal("HAVING aggregate is a column".into()))
        }
    })
}

fn resolve_projection(
    projection: &[SelectItem],
    schema: &TableSchema,
    alias: Option<&str>,
) -> Result<Vec<ResolvedProjection>> {
    let mut resolved = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => {
                for column in &schema.columns {
                    resolved.push(ResolvedProjection::Column {
                        index: schema
                            .column_index(&column.name)
                            .expect("schema column must resolve"),
                        header: column.name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(kind, _) => {
                let qualifier = match kind {
                    sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                        single_name(name)?
                    }
                    other => {
                        return Err(Error::Unsupported(format!("qualified wildcard `{other}`")))
                    }
                };
                if qualifier != schema.name && Some(qualifier.as_str()) != alias {
                    return Err(Error::Unsupported(format!(
                        "unknown table qualifier `{qualifier}`"
                    )));
                }
                for column in &schema.columns {
                    resolved.push(ResolvedProjection::Column {
                        index: schema
                            .column_index(&column.name)
                            .expect("schema column must resolve"),
                        header: column.name.clone(),
                    });
                }
            }
            SelectItem::UnnamedExpr(expression) => {
                resolved.push(projection_expression(expression, None, schema, alias)?);
            }
            SelectItem::ExprWithAlias {
                expr,
                alias: output_alias,
            } => {
                resolved.push(projection_expression(
                    expr,
                    Some(output_alias.value.clone()),
                    schema,
                    alias,
                )?);
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(Error::Unsupported("multiple projection aliases".into()))
            }
        }
    }
    if resolved.is_empty() {
        return Err(Error::Unsupported(
            "SELECT requires at least one projection".into(),
        ));
    }
    Ok(resolved)
}

fn projection_expression(
    expression: &Expr,
    alias: Option<String>,
    schema: &TableSchema,
    table_alias: Option<&str>,
) -> Result<ResolvedProjection> {
    if let Expr::Function(function) = strip_nested(expression) {
        if object_name_is(&function.name, "count") {
            return parse_count(function, alias, schema, table_alias);
        }
        if object_name_is(&function.name, "sum") || object_name_is(&function.name, "avg") {
            return parse_sum_avg(function, alias, schema, table_alias);
        }
        if object_name_is(&function.name, "min") || object_name_is(&function.name, "max") {
            return parse_min_max(function, alias, schema, table_alias);
        }
        return Err(Error::Unsupported(format!(
            "function `{}` (only COUNT, SUM, AVG, MIN and MAX are supported)",
            function.name
        )));
    }
    let index = resolve_column(expression, schema, table_alias)?;
    let header = alias.unwrap_or_else(|| schema.columns[index].name.clone());
    Ok(ResolvedProjection::Column { index, header })
}

fn parse_count(
    function: &Function,
    alias: Option<String>,
    schema: &TableSchema,
    table_alias: Option<&str>,
) -> Result<ResolvedProjection> {
    validate_aggregate_function(function, "COUNT")?;
    let list = aggregate_argument_list(function, "COUNT")?;
    if list.args.len() != 1 {
        return Err(Error::Unsupported(
            "COUNT requires exactly one argument".into(),
        ));
    }
    let header = alias.unwrap_or_else(|| "count".to_string());
    match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
            Ok(ResolvedProjection::CountStar { header })
        }
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => {
            let index = resolve_column(expression, schema, table_alias)?;
            Ok(ResolvedProjection::CountColumn { index, header })
        }
        other => Err(Error::Unsupported(format!("COUNT argument `{other}`"))),
    }
}

fn parse_sum_avg(
    function: &Function,
    alias: Option<String>,
    schema: &TableSchema,
    table_alias: Option<&str>,
) -> Result<ResolvedProjection> {
    let name = if object_name_is(&function.name, "sum") {
        "SUM"
    } else {
        "AVG"
    };
    validate_aggregate_function(function, name)?;
    let list = aggregate_argument_list(function, name)?;
    if list.args.len() != 1 {
        return Err(Error::Unsupported(format!(
            "{name} requires exactly one argument"
        )));
    }
    let expression = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression.clone(),
        other => return Err(Error::Unsupported(format!("{name} argument `{other}`"))),
    };
    let argument_type = validate_aggregate_argument(&expression, schema, table_alias, name)?;
    if !argument_type.is_numeric() {
        return Err(Error::Unsupported(format!(
            "{name} requires a numeric argument, got {}",
            argument_type.name()
        )));
    }
    let header = alias.unwrap_or_else(|| name.to_ascii_lowercase());
    Ok(if name == "SUM" {
        ResolvedProjection::Sum { expression, header }
    } else {
        ResolvedProjection::Avg { expression, header }
    })
}

fn parse_min_max(
    function: &Function,
    alias: Option<String>,
    schema: &TableSchema,
    table_alias: Option<&str>,
) -> Result<ResolvedProjection> {
    let name = if object_name_is(&function.name, "min") {
        "MIN"
    } else {
        "MAX"
    };
    validate_aggregate_function(function, name)?;
    let list = aggregate_argument_list(function, name)?;
    if list.args.len() != 1 {
        return Err(Error::Unsupported(format!(
            "{name} requires exactly one argument"
        )));
    }
    let expression = match &list.args[0] {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression.clone(),
        other => return Err(Error::Unsupported(format!("{name} argument `{other}`"))),
    };
    validate_aggregate_argument(&expression, schema, table_alias, name)?;
    let header = alias.unwrap_or_else(|| name.to_ascii_lowercase());
    Ok(if name == "MIN" {
        ResolvedProjection::Min { expression, header }
    } else {
        ResolvedProjection::Max { expression, header }
    })
}

fn validate_aggregate_function(function: &Function, name: &str) -> Result<()> {
    if !matches!(function.parameters, FunctionArguments::None)
        || !function.within_group.is_empty()
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
    {
        return Err(Error::Unsupported(format!(
            "{name} modifiers/window functions"
        )));
    }
    Ok(())
}

fn aggregate_argument_list<'a>(
    function: &'a Function,
    name: &str,
) -> Result<&'a sqlparser::ast::FunctionArgumentList> {
    let list = match &function.args {
        FunctionArguments::List(list) => list,
        other => return Err(Error::Unsupported(format!("{name} arguments `{other}`"))),
    };
    if list.duplicate_treatment.is_some() || !list.clauses.is_empty() {
        return Err(Error::Unsupported(format!("{name} requires no modifiers")));
    }
    Ok(list)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScalarType {
    Null,
    Integer,
    Real,
    Text,
    Boolean,
}

impl ScalarType {
    fn name(self) -> &'static str {
        match self {
            ScalarType::Null => "NULL",
            ScalarType::Integer => "INT",
            ScalarType::Real => "REAL",
            ScalarType::Text => "TEXT",
            ScalarType::Boolean => "BOOLEAN",
        }
    }

    fn is_numeric(self) -> bool {
        matches!(
            self,
            ScalarType::Null | ScalarType::Integer | ScalarType::Real
        )
    }
}

fn scalar_type_of(value: &Value) -> ScalarType {
    match value {
        Value::Null => ScalarType::Null,
        Value::Integer(_) => ScalarType::Integer,
        Value::Real(_) => ScalarType::Real,
        Value::Text(_) => ScalarType::Text,
        Value::Boolean(_) => ScalarType::Boolean,
    }
}

fn validate_aggregate_argument(
    expression: &Expr,
    schema: &TableSchema,
    table_alias: Option<&str>,
    name: &str,
) -> Result<ScalarType> {
    let inferred = match strip_nested(expression) {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let index = resolve_column(expression, schema, table_alias)?;
            match schema.columns[index].data_type {
                DataType::Integer => ScalarType::Integer,
                DataType::Real => ScalarType::Real,
                DataType::Text => ScalarType::Text,
                DataType::Boolean => ScalarType::Boolean,
            }
        }
        Expr::Value(value) => scalar_type_of(&Value::from_sql_literal(value)?),
        Expr::TypedString(typed) => {
            eval_typed_string(typed)?;
            ScalarType::Text
        }
        Expr::Interval(interval) => {
            eval_interval(interval)?;
            ScalarType::Integer
        }
        Expr::UnaryOp { op, expr } => {
            let operand = validate_aggregate_argument(expr, schema, table_alias, name)?;
            match op {
                UnaryOperator::Plus | UnaryOperator::Minus if operand.is_numeric() => operand,
                _ => {
                    return Err(Error::Unsupported(format!(
                        "{name} unary operator `{op}` on {}",
                        operand.name()
                    )))
                }
            }
        }
        Expr::BinaryOp { left, op, right } => {
            let left = validate_aggregate_argument(left, schema, table_alias, name)?;
            let right = validate_aggregate_argument(right, schema, table_alias, name)?;
            if left == ScalarType::Null || right == ScalarType::Null {
                ScalarType::Null
            } else {
                match op {
                    BinaryOperator::Plus | BinaryOperator::Minus => match (left, right) {
                        (ScalarType::Integer, ScalarType::Integer) => ScalarType::Integer,
                        (ScalarType::Integer, ScalarType::Real)
                        | (ScalarType::Real, ScalarType::Integer)
                        | (ScalarType::Real, ScalarType::Real) => ScalarType::Real,
                        _ => {
                            return Err(Error::Unsupported(format!(
                                "{name} arithmetic on {} and {}",
                                left.name(),
                                right.name()
                            )))
                        }
                    },
                    BinaryOperator::Multiply => match (left, right) {
                        (ScalarType::Integer, ScalarType::Integer) => ScalarType::Integer,
                        (ScalarType::Integer, ScalarType::Real)
                        | (ScalarType::Real, ScalarType::Integer)
                        | (ScalarType::Real, ScalarType::Real) => ScalarType::Real,
                        _ => {
                            return Err(Error::Unsupported(format!(
                                "{name} arithmetic on {} and {}",
                                left.name(),
                                right.name()
                            )))
                        }
                    },
                    BinaryOperator::Divide => {
                        if left.is_numeric() && right.is_numeric() {
                            ScalarType::Real
                        } else {
                            return Err(Error::Unsupported(format!(
                                "{name} arithmetic on {} and {}",
                                left.name(),
                                right.name()
                            )));
                        }
                    }
                    other => {
                        return Err(Error::Unsupported(format!(
                            "{name} arithmetic operator `{other}`"
                        )))
                    }
                }
            }
        }
        other => {
            return Err(Error::Unsupported(format!(
                "{name} argument expression `{other}`"
            )))
        }
    };
    Ok(inferred)
}

fn parse_order_by(
    order_by: Option<&OrderBy>,
    schema: &TableSchema,
    alias: Option<&str>,
) -> Result<Vec<SortKey>> {
    let Some(order_by) = order_by else {
        return Ok(Vec::new());
    };
    let expressions = match &order_by.kind {
        OrderByKind::Expressions(expressions) => expressions,
        OrderByKind::All(_) => return Err(Error::Unsupported("ORDER BY ALL".into())),
    };
    let mut keys = Vec::with_capacity(expressions.len());
    for expression in expressions {
        let index = resolve_column(&expression.expr, schema, alias)?;
        let descending = match expression.options.sort {
            None | Some(OrderBySort::Asc) => false,
            Some(OrderBySort::Desc) => true,
            Some(OrderBySort::Using(_)) => return Err(Error::Unsupported("ORDER BY USING".into())),
        };
        keys.push(SortKey {
            index,
            descending,
            nulls_first: expression.options.nulls_first.unwrap_or(false),
        });
    }
    Ok(keys)
}

fn parse_limit_offset(query: &Query) -> Result<(Option<u64>, u64)> {
    let Some(limit_clause) = &query.limit_clause else {
        return Ok((None, 0));
    };
    match limit_clause {
        LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(Error::Unsupported("LIMIT BY".into()));
            }
            let limit = limit
                .as_ref()
                .map(|value| parse_u64(value, "LIMIT"))
                .transpose()?;
            let offset = offset
                .as_ref()
                .map(|offset| parse_u64(&offset.value, "OFFSET"))
                .transpose()?
                .unwrap_or(0);
            Ok((limit, offset))
        }
        LimitClause::OffsetCommaLimit { offset, limit } => Ok((
            Some(parse_u64(limit, "LIMIT")?),
            parse_u64(offset, "OFFSET")?,
        )),
    }
}

fn parse_u64(expression: &Expr, context: &str) -> Result<u64> {
    match eval_literal(expression)? {
        Value::Integer(value) if value >= 0 => Ok(value as u64),
        other => Err(Error::InvalidValue(format!(
            "{context} must be a non-negative integer, got {other}"
        ))),
    }
}

fn paginate<T>(rows: &mut Vec<T>, limit: Option<u64>, offset: u64) {
    let offset = usize::try_from(offset).unwrap_or(usize::MAX);
    if offset >= rows.len() {
        rows.clear();
        return;
    }
    if offset > 0 {
        rows.drain(0..offset);
    }
    if let Some(limit) = limit {
        rows.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
    }
}

fn sort_rows(rows: &mut [crate::storage::StoredRow], keys: &[SortKey]) {
    if keys.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        for key in keys {
            let ordering = compare_for_sort(
                &left.values[key.index],
                &right.values[key.index],
                key.descending,
                key.nulls_first,
            );
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    });
}

fn compare_for_sort(left: &Value, right: &Value, descending: bool, nulls_first: bool) -> Ordering {
    match (left.is_null(), right.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let ordering = left.sort_cmp(right);
            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        }
    }
}

#[derive(Debug, Clone)]
enum AggregateState {
    CountStar(u64),
    CountColumn(u64),
    Sum { sum: f64, count: u64 },
    Avg { sum: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
}

impl AggregateState {
    fn from_projection(projection: &ResolvedProjection) -> Option<Self> {
        match projection {
            ResolvedProjection::Column { .. } => None,
            ResolvedProjection::CountStar { .. } => Some(Self::CountStar(0)),
            ResolvedProjection::CountColumn { .. } => Some(Self::CountColumn(0)),
            ResolvedProjection::Sum { .. } => Some(Self::Sum { sum: 0.0, count: 0 }),
            ResolvedProjection::Avg { .. } => Some(Self::Avg { sum: 0.0, count: 0 }),
            ResolvedProjection::Min { .. } => Some(Self::Min(None)),
            ResolvedProjection::Max { .. } => Some(Self::Max(None)),
        }
    }

    fn add_numeric(&mut self, value: &Value, name: &str) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let number = match value {
            Value::Integer(value) => *value as f64,
            Value::Real(value) => *value,
            other => {
                return Err(Error::InvalidValue(format!(
                    "{name} requires a numeric argument, got {other}"
                )))
            }
        };
        match self {
            AggregateState::CountStar(count) => *count += 1,
            AggregateState::CountColumn(count) => *count += 1,
            AggregateState::Sum { sum, count } | AggregateState::Avg { sum, count } => {
                *sum += number;
                *count += 1;
            }
            AggregateState::Min(_) | AggregateState::Max(_) => {
                return Err(Error::Internal(format!("{name} state mismatch")));
            }
        }
        Ok(())
    }

    fn add_extreme(&mut self, value: &Value, name: &str) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let (slot, want_max) = match self {
            AggregateState::Min(slot) => (slot, false),
            AggregateState::Max(slot) => (slot, true),
            _ => return Err(Error::Internal(format!("{name} state mismatch"))),
        };
        match slot {
            None => *slot = Some(value.clone()),
            Some(current) => {
                let Some(ordering) = value.compare(current)? else {
                    return Ok(());
                };
                let replace = if want_max {
                    ordering == Ordering::Greater
                } else {
                    ordering == Ordering::Less
                };
                if replace {
                    *slot = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finish(&self) -> Value {
        match self {
            AggregateState::CountStar(count) | AggregateState::CountColumn(count) => {
                Value::Integer(*count as i64)
            }
            AggregateState::Sum { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Real(*sum)
                }
            }
            AggregateState::Avg { sum, count } => {
                if *count == 0 {
                    Value::Null
                } else {
                    Value::Real(*sum / *count as f64)
                }
            }
            AggregateState::Min(value) | AggregateState::Max(value) => {
                value.clone().unwrap_or(Value::Null)
            }
        }
    }
}

#[derive(Debug)]
struct AggregateGroup {
    keys: Vec<Value>,
    states: Vec<AggregateState>,
}

#[derive(Debug)]
struct AggregateAccumulator {
    group_by: Vec<usize>,
    groups: Vec<AggregateGroup>,
    lookup: HashMap<Vec<u8>, usize>,
    states_template: Vec<AggregateState>,
    having_aggregates: Vec<ResolvedProjection>,
}

impl AggregateAccumulator {
    fn new(
        group_by: &[usize],
        projections: &[ResolvedProjection],
        having_aggregates: &[ResolvedProjection],
    ) -> Self {
        let mut states = projections
            .iter()
            .filter_map(AggregateState::from_projection)
            .collect::<Vec<_>>();
        states.extend(
            having_aggregates
                .iter()
                .filter_map(AggregateState::from_projection),
        );
        let groups = if group_by.is_empty() {
            vec![AggregateGroup {
                keys: Vec::new(),
                states: states.clone(),
            }]
        } else {
            Vec::new()
        };
        Self {
            group_by: group_by.to_vec(),
            groups,
            lookup: HashMap::new(),
            states_template: states,
            having_aggregates: having_aggregates.to_vec(),
        }
    }

    fn consume(
        &mut self,
        row: &crate::storage::StoredRow,
        schema: &TableSchema,
        alias: Option<&str>,
        projections: &[ResolvedProjection],
    ) -> Result<()> {
        let keys = self
            .group_by
            .iter()
            .map(|index| row.values[*index].clone())
            .collect::<Vec<_>>();
        let index = if self.group_by.is_empty() {
            0
        } else {
            let encoded = group_key_bytes(&keys);
            if let Some(index) = self.lookup.get(&encoded) {
                *index
            } else {
                let index = self.groups.len();
                self.lookup.insert(encoded, index);
                self.groups.push(AggregateGroup {
                    keys,
                    states: self.states_template.clone(),
                });
                index
            }
        };
        let context = RowContext::new(schema, alias, &row.values);
        let group = &mut self.groups[index];
        let mut aggregate_index = 0;
        for projection in projections {
            if projection.is_aggregate() {
                add_aggregate_state(&mut group.states[aggregate_index], projection, &context)?;
                aggregate_index += 1;
            }
        }
        for projection in &self.having_aggregates {
            add_aggregate_state(&mut group.states[aggregate_index], projection, &context)?;
            aggregate_index += 1;
        }
        Ok(())
    }

    fn finish(
        mut self,
        group_by: &[usize],
        projections: &[ResolvedProjection],
        order_keys: &[SortKey],
        having: Option<&ResolvedHaving>,
    ) -> Result<Vec<Vec<Value>>> {
        if group_by.is_empty() {
            // There is exactly one state for an ungrouped aggregate, even when
            // the input is empty.
            debug_assert_eq!(self.groups.len(), 1);
        } else if self.groups.is_empty() {
            return Ok(Vec::new());
        }

        if !group_by.is_empty() && !order_keys.is_empty() {
            let mut positions = Vec::with_capacity(order_keys.len());
            for key in order_keys {
                let position = group_by
                    .iter()
                    .position(|index| *index == key.index)
                    .ok_or_else(|| Error::Internal("ORDER BY key is not in GROUP BY".into()))?;
                positions.push(position);
            }
            self.groups.sort_by(|left, right| {
                for (key, position) in order_keys.iter().zip(&positions) {
                    let ordering = compare_for_sort(
                        &left.keys[*position],
                        &right.keys[*position],
                        key.descending,
                        key.nulls_first,
                    );
                    if ordering != Ordering::Equal {
                        return ordering;
                    }
                }
                Ordering::Equal
            });
        }

        let projection_aggregates = projections
            .iter()
            .filter(|projection| projection.is_aggregate())
            .count();
        let mut output = Vec::with_capacity(self.groups.len());
        for group in self.groups {
            if let Some(having) = having {
                let mut values = group.keys.clone();
                values.extend(
                    group.states[projection_aggregates..]
                        .iter()
                        .map(AggregateState::finish),
                );
                if !row_matches(Some(&having.expression), &having.schema, None, &values)? {
                    continue;
                }
            }
            let mut row = Vec::with_capacity(projections.len());
            let mut aggregate_index = 0;
            for projection in projections {
                match projection {
                    ResolvedProjection::Column { index, .. } => {
                        let position =
                            group_by
                                .iter()
                                .position(|key| key == index)
                                .ok_or_else(|| {
                                    Error::Internal(
                                        "non-aggregate projection missing from GROUP BY".into(),
                                    )
                                })?;
                        row.push(group.keys[position].clone());
                    }
                    ResolvedProjection::CountStar { .. }
                    | ResolvedProjection::CountColumn { .. }
                    | ResolvedProjection::Sum { .. }
                    | ResolvedProjection::Avg { .. }
                    | ResolvedProjection::Min { .. }
                    | ResolvedProjection::Max { .. } => {
                        row.push(group.states[aggregate_index].finish());
                        aggregate_index += 1;
                    }
                }
            }
            output.push(row);
        }
        Ok(output)
    }
}

fn add_aggregate_state(
    state: &mut AggregateState,
    projection: &ResolvedProjection,
    context: &RowContext<'_>,
) -> Result<()> {
    match projection {
        ResolvedProjection::Column { .. } => {
            return Err(Error::Internal("column passed as aggregate".into()))
        }
        ResolvedProjection::CountStar { .. } => {
            if let AggregateState::CountStar(count) = state {
                *count += 1;
            }
        }
        ResolvedProjection::CountColumn { index, .. } => {
            if !context.values[*index].is_null() {
                if let AggregateState::CountColumn(count) = state {
                    *count += 1;
                }
            }
        }
        ResolvedProjection::Sum { expression, .. } | ResolvedProjection::Avg { expression, .. } => {
            let value = eval_scalar(expression, context)?;
            let name = if matches!(projection, ResolvedProjection::Sum { .. }) {
                "SUM"
            } else {
                "AVG"
            };
            state.add_numeric(&value, name)?;
        }
        ResolvedProjection::Min { expression, .. } | ResolvedProjection::Max { expression, .. } => {
            let value = eval_scalar(expression, context)?;
            let name = if matches!(projection, ResolvedProjection::Min { .. }) {
                "MIN"
            } else {
                "MAX"
            };
            state.add_extreme(&value, name)?;
        }
    }
    Ok(())
}

fn group_key_bytes(keys: &[Value]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for key in keys {
        let bytes = key.dedupe_key();
        encoded.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        encoded.extend_from_slice(&bytes);
    }
    encoded
}

fn point_lookup(select: &Select, schema: &TableSchema, alias: Option<&str>) -> Result<PointLookup> {
    let Some(selection) = select.selection.as_ref() else {
        return Ok(PointLookup::Scan);
    };
    let Expr::BinaryOp { left, op, right } = strip_nested(selection) else {
        return Ok(PointLookup::Scan);
    };
    if *op != BinaryOperator::Eq {
        return Ok(PointLookup::Scan);
    }
    let (column_expression, literal_expression) =
        if is_column_expression(left) && !is_column_expression(right) {
            (left.as_ref(), right.as_ref())
        } else if is_column_expression(right) && !is_column_expression(left) {
            (right.as_ref(), left.as_ref())
        } else {
            return Ok(PointLookup::Scan);
        };
    let column_index = resolve_column(column_expression, schema, alias)?;
    if column_index != schema.primary_key {
        return Ok(PointLookup::Scan);
    }
    let literal = match eval_literal(literal_expression) {
        Ok(value) => value,
        Err(Error::Unsupported(_)) => return Ok(PointLookup::Scan),
        Err(error) => return Err(error),
    };
    if literal.is_null() {
        return Ok(PointLookup::Missing);
    }
    let primary_key_column = schema.primary_key_column();
    let primary_key = if primary_key_column.data_type == DataType::Integer {
        match literal {
            // `id = 1.0` compares numerically, so accept it as a point lookup
            // rather than failing an INT coercion that a scan would not do.
            Value::Real(value) if value.fract() == 0.0 => {
                let lower = -9_223_372_036_854_775_808.0_f64;
                let upper_exclusive = 9_223_372_036_854_775_808.0_f64;
                if value >= lower && value < upper_exclusive {
                    Value::Integer(value as i64)
                } else {
                    return Ok(PointLookup::Missing);
                }
            }
            Value::Real(_) => return Ok(PointLookup::Missing),
            value => value.coerce_to(&primary_key_column.name, primary_key_column.data_type)?,
        }
    } else {
        literal.coerce_to(&primary_key_column.name, primary_key_column.data_type)?
    };
    Ok(PointLookup::Value(primary_key))
}

fn is_column_expression(expression: &Expr) -> bool {
    matches!(
        strip_nested(expression),
        Expr::Identifier(_) | Expr::CompoundIdentifier(_)
    )
}

fn strip_nested(mut expression: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expression {
        expression = inner;
    }
    expression
}

fn resolve_column(expression: &Expr, schema: &TableSchema, alias: Option<&str>) -> Result<usize> {
    match strip_nested(expression) {
        Expr::Identifier(identifier) => schema.column_index_or_error(&identifier.value),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = &parts[0].value;
            if qualifier != &schema.name && Some(qualifier.as_str()) != alias {
                return Err(Error::Unsupported(format!(
                    "unknown table qualifier `{qualifier}`"
                )));
            }
            schema.column_index_or_error(&parts[1].value)
        }
        other => Err(Error::Unsupported(format!(
            "expected a column reference, got `{other}`"
        ))),
    }
}

fn object_name_is(name: &ObjectName, expected: &str) -> bool {
    name.0.len() == 1
        && name.0[0]
            .as_ident()
            .map(|identifier| identifier.value.eq_ignore_ascii_case(expected))
            .unwrap_or(false)
}

fn coerce_row(schema: &TableSchema, values: &mut [Value]) -> Result<()> {
    if values.len() != schema.columns.len() {
        return Err(Error::InvalidRow {
            expected: schema.columns.len(),
            actual: values.len(),
        });
    }
    if values[schema.primary_key].is_null() {
        return Err(Error::NullPrimaryKey(
            schema.primary_key_column().name.clone(),
        ));
    }
    for (column, value) in schema.columns.iter().zip(values.iter_mut()) {
        if value.is_null() {
            if !column.nullable {
                return Err(Error::NotNullConstraint(column.name.clone()));
            }
            continue;
        }
        let coerced = value.coerce_to(&column.name, column.data_type)?;
        *value = coerced;
    }
    Ok(())
}

fn eval_literal(expression: &Expr) -> Result<Value> {
    match strip_nested(expression) {
        Expr::Value(value) => Value::from_sql_literal(value),
        Expr::TypedString(typed) => eval_typed_string(typed),
        Expr::Interval(interval) => eval_interval(interval),
        Expr::UnaryOp { op, expr } => {
            let value = eval_literal(expr)?;
            eval_unary(op, value)
        }
        Expr::BinaryOp { left, op, right } => {
            let left = eval_literal(left)?;
            let right = eval_literal(right)?;
            eval_arithmetic(op, left, right)
        }
        other => Err(Error::Unsupported(format!(
            "expression `{other}` (only literals are supported here)"
        ))),
    }
}

fn eval_typed_string(typed: &TypedString) -> Result<Value> {
    match typed.data_type {
        SqlDataType::Date | SqlDataType::Date32 => {
            let value = Value::from_sql_literal(&typed.value)?;
            match value {
                Value::Text(text) => Ok(Value::Text(normalize_date(&text)?)),
                other => Err(Error::InvalidLiteral(format!(
                    "DATE literal must be a string, got {other}"
                ))),
            }
        }
        ref other => Err(Error::Unsupported(format!(
            "typed literal `{other}` (only DATE is supported)"
        ))),
    }
}

fn eval_interval(interval: &Interval) -> Result<Value> {
    if interval.leading_precision.is_some()
        || interval.last_field.is_some()
        || interval.fractional_seconds_precision.is_some()
    {
        return Err(Error::Unsupported("INTERVAL precision/ranges".into()));
    }
    if !matches!(interval.leading_field, Some(DateTimeField::Day)) {
        return Err(Error::Unsupported(format!(
            "INTERVAL unit `{:?}` (only DAY is supported)",
            interval.leading_field
        )));
    }
    match eval_literal(&interval.value)? {
        Value::Integer(days) => Ok(Value::Integer(days)),
        Value::Real(days) if days.fract() == 0.0 => Ok(Value::Integer(days as i64)),
        Value::Text(days) => days
            .trim()
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| Error::InvalidLiteral(format!("invalid INTERVAL `{days}` DAY"))),
        other => Err(Error::InvalidLiteral(format!(
            "INTERVAL value must be numeric, got {other}"
        ))),
    }
}

fn eval_unary(op: &UnaryOperator, value: Value) -> Result<Value> {
    match (op, value) {
        (UnaryOperator::Plus, value @ (Value::Integer(_) | Value::Real(_) | Value::Null)) => {
            Ok(value)
        }
        (UnaryOperator::Minus, Value::Integer(value)) => value
            .checked_neg()
            .map(Value::Integer)
            .ok_or_else(|| Error::InvalidLiteral("INT literal out of range".into())),
        (UnaryOperator::Minus, Value::Real(value)) => Ok(Value::Real(-value)),
        (UnaryOperator::Minus, Value::Null) => Ok(Value::Null),
        (op, other) => Err(Error::Unsupported(format!(
            "unary operator `{op}` on {other}"
        ))),
    }
}

fn eval_arithmetic(op: &BinaryOperator, left: Value, right: Value) -> Result<Value> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    if let (Value::Text(left_text), Value::Integer(days)) = (&left, &right) {
        if matches!(op, BinaryOperator::Plus | BinaryOperator::Minus) {
            let base = parse_date_days(left_text)?;
            let delta = if *op == BinaryOperator::Plus {
                *days
            } else {
                -*days
            };
            return Ok(Value::Text(format_date_days(base + delta)?));
        }
    }
    let (left_number, right_number, use_real) = match (&left, &right) {
        (Value::Integer(left), Value::Integer(right)) => (*left as f64, *right as f64, false),
        (Value::Integer(left), Value::Real(right)) => (*left as f64, *right, true),
        (Value::Real(left), Value::Integer(right)) => (*left, *right as f64, true),
        (Value::Real(left), Value::Real(right)) => (*left, *right, true),
        _ => {
            return Err(Error::Unsupported(format!(
                "arithmetic `{left} {op} {right}`"
            )))
        }
    };
    match op {
        BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply => {
            if use_real {
                let value = match op {
                    BinaryOperator::Plus => left_number + right_number,
                    BinaryOperator::Minus => left_number - right_number,
                    BinaryOperator::Multiply => left_number * right_number,
                    _ => unreachable!(),
                };
                if !value.is_finite() {
                    return Err(Error::InvalidValue(
                        "arithmetic produced a non-finite REAL".into(),
                    ));
                }
                Ok(Value::Real(value))
            } else {
                let left = left_number as i64;
                let right = right_number as i64;
                let value = match op {
                    BinaryOperator::Plus => left.checked_add(right),
                    BinaryOperator::Minus => left.checked_sub(right),
                    BinaryOperator::Multiply => left.checked_mul(right),
                    _ => unreachable!(),
                };
                value
                    .map(Value::Integer)
                    .ok_or_else(|| Error::InvalidValue("INT arithmetic overflow".into()))
            }
        }
        BinaryOperator::Divide => {
            if right_number == 0.0 {
                return Err(Error::InvalidValue("division by zero".into()));
            }
            let value = left_number / right_number;
            if !value.is_finite() {
                return Err(Error::InvalidValue(
                    "division produced a non-finite REAL".into(),
                ));
            }
            Ok(Value::Real(value))
        }
        other => Err(Error::Unsupported(format!("arithmetic operator `{other}`"))),
    }
}

fn parse_date_days(text: &str) -> Result<i64> {
    let mut parts = text.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(Error::InvalidLiteral(format!(
            "`{text}` is not an ISO date (YYYY-MM-DD)"
        )));
    };
    let year = year
        .parse::<i64>()
        .map_err(|_| Error::InvalidLiteral(format!("invalid date `{text}`")))?;
    let month = month
        .parse::<u32>()
        .map_err(|_| Error::InvalidLiteral(format!("invalid date `{text}`")))?;
    let day = day
        .parse::<u32>()
        .map_err(|_| Error::InvalidLiteral(format!("invalid date `{text}`")))?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        return Err(Error::InvalidLiteral(format!("invalid date `{text}`")));
    }
    // Howard Hinnant's civil-date algorithm: days since 1970-01-01.
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month_prime = i64::from(month) + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * month_prime + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Ok(era * 146_097 + day_of_era - 719_468)
}

fn format_date_days(days: i64) -> Result<String> {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    if !(1..=9999).contains(&year) {
        return Err(Error::InvalidValue(format!(
            "date arithmetic produced year {year}, outside 0001..=9999"
        )));
    }
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn normalize_date(text: &str) -> Result<String> {
    format_date_days(parse_date_days(text)?)
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if (year % 4 == 0 && year % 100 != 0) || year % 400 == 0 => 29,
        2 => 28,
        _ => 0,
    }
}

fn row_matches(
    selection: Option<&Expr>,
    schema: &TableSchema,
    alias: Option<&str>,
    values: &[Value],
) -> Result<bool> {
    match selection {
        None => Ok(true),
        Some(expression) => {
            Ok(eval_predicate(expression, &RowContext::new(schema, alias, values))? == Some(true))
        }
    }
}

fn eval_predicate(expression: &Expr, context: &RowContext<'_>) -> Result<Option<bool>> {
    match strip_nested(expression) {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left = eval_predicate(left, context)?;
                let right = eval_predicate(right, context)?;
                Ok(sql_and(left, right))
            }
            BinaryOperator::Or => {
                let left = eval_predicate(left, context)?;
                let right = eval_predicate(right, context)?;
                Ok(sql_or(left, right))
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq => {
                let left = eval_scalar(left, context)?;
                let right = eval_scalar(right, context)?;
                let Some(ordering) = left.compare(&right)? else {
                    return Ok(None);
                };
                let matched = match op {
                    BinaryOperator::Eq => ordering == Ordering::Equal,
                    BinaryOperator::NotEq => ordering != Ordering::Equal,
                    BinaryOperator::Lt => ordering == Ordering::Less,
                    BinaryOperator::LtEq => ordering != Ordering::Greater,
                    BinaryOperator::Gt => ordering == Ordering::Greater,
                    BinaryOperator::GtEq => ordering != Ordering::Less,
                    _ => unreachable!("comparison operators handled above"),
                };
                Ok(Some(matched))
            }
            other => Err(Error::Unsupported(format!("operator `{other}` in WHERE"))),
        },
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(eval_predicate(expr, context)?.map(|value| !value)),
        Expr::IsNull(expr) => Ok(Some(eval_scalar(expr, context)?.is_null())),
        Expr::IsNotNull(expr) => Ok(Some(!eval_scalar(expr, context)?.is_null())),
        Expr::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } => {
            if *any {
                return Err(Error::Unsupported("LIKE ANY".into()));
            }
            let text = eval_scalar(expr, context)?;
            let pattern = eval_scalar(pattern, context)?;
            let escape = match escape_char {
                None => None,
                Some(expression) => match eval_scalar(expression, context)? {
                    Value::Text(value) => {
                        let mut chars = value.chars();
                        let escape = chars.next().ok_or_else(|| {
                            Error::InvalidValue("ESCAPE must be one character".into())
                        })?;
                        if chars.next().is_some() {
                            return Err(Error::InvalidValue("ESCAPE must be one character".into()));
                        }
                        Some(escape)
                    }
                    other => {
                        return Err(Error::InvalidValue(format!(
                            "ESCAPE must be TEXT, got {other}"
                        )))
                    }
                },
            };
            match (text, pattern) {
                (Value::Null, _) | (_, Value::Null) => Ok(None),
                (Value::Text(text), Value::Text(pattern)) => {
                    let matched = like_match(&text, &pattern, escape);
                    Ok(Some(if *negated { !matched } else { matched }))
                }
                (text, pattern) => Err(Error::IncomparableTypes {
                    left: text.type_name().to_string(),
                    right: pattern.type_name().to_string(),
                }),
            }
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = eval_scalar(expr, context)?;
            let low = eval_scalar(low, context)?;
            let high = eval_scalar(high, context)?;
            let lower = value
                .compare(&low)?
                .map(|ordering| ordering != Ordering::Less);
            let upper = value
                .compare(&high)?
                .map(|ordering| ordering != Ordering::Greater);
            let matched = sql_and(lower, upper);
            Ok(if *negated {
                matched.map(|value| !value)
            } else {
                matched
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_scalar(expr, context)?;
            if value.is_null() {
                return Ok(None);
            }
            let mut saw_null = false;
            let mut matched = false;
            for item in list {
                let item = eval_scalar(item, context)?;
                match value.compare(&item)? {
                    Some(Ordering::Equal) => {
                        matched = true;
                        break;
                    }
                    None => saw_null = true,
                    Some(_) => {}
                }
            }
            let result = if matched {
                Some(true)
            } else if saw_null {
                None
            } else {
                Some(false)
            };
            Ok(if *negated {
                result.map(|value| !value)
            } else {
                result
            })
        }
        other => match eval_scalar(other, context)? {
            Value::Boolean(value) => Ok(Some(value)),
            Value::Null => Ok(None),
            other => Err(Error::InvalidValue(format!(
                "predicate expression must be BOOLEAN, got {other}"
            ))),
        },
    }
}

fn eval_scalar(expression: &Expr, context: &RowContext<'_>) -> Result<Value> {
    match strip_nested(expression) {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
            let index = resolve_column(expression, context.schema, context.alias)?;
            Ok(context.values[index].clone())
        }
        Expr::Value(value) => Value::from_sql_literal(value),
        Expr::TypedString(typed) => eval_typed_string(typed),
        Expr::Interval(interval) => eval_interval(interval),
        Expr::UnaryOp { op, expr } => {
            let value = eval_scalar(expr, context)?;
            eval_unary(op, value)
        }
        Expr::BinaryOp { left, op, right } => {
            let left = eval_scalar(left, context)?;
            let right = eval_scalar(right, context)?;
            eval_arithmetic(op, left, right)
        }
        other => Err(Error::Unsupported(format!("expression `{other}` in WHERE"))),
    }
}

fn sql_and(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn sql_or(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

fn like_match(text: &str, pattern: &str, escape: Option<char>) -> bool {
    #[derive(Clone, Copy)]
    enum Token {
        Any,
        One,
        Literal(char),
    }

    let pattern_chars: Vec<char> = pattern.chars().collect();
    let mut tokens = Vec::with_capacity(pattern_chars.len());
    let mut index = 0;
    while index < pattern_chars.len() {
        let character = pattern_chars[index];
        if Some(character) == escape {
            if let Some(escaped) = pattern_chars.get(index + 1) {
                tokens.push(Token::Literal(*escaped));
                index += 2;
                continue;
            }
            tokens.push(Token::Literal(character));
            index += 1;
            continue;
        }
        tokens.push(match character {
            '%' => Token::Any,
            '_' => Token::One,
            other => Token::Literal(other),
        });
        index += 1;
    }

    let text_chars: Vec<char> = text.chars().collect();
    let mut table = vec![vec![false; tokens.len() + 1]; text_chars.len() + 1];
    table[0][0] = true;
    for token_index in 1..=tokens.len() {
        if matches!(tokens[token_index - 1], Token::Any) {
            table[0][token_index] = table[0][token_index - 1];
        }
    }
    for text_index in 1..=text_chars.len() {
        for token_index in 1..=tokens.len() {
            table[text_index][token_index] = match tokens[token_index - 1] {
                Token::Any => {
                    table[text_index][token_index - 1] || table[text_index - 1][token_index]
                }
                Token::One => table[text_index - 1][token_index - 1],
                Token::Literal(expected) => {
                    table[text_index - 1][token_index - 1] && text_chars[text_index - 1] == expected
                }
            };
        }
    }
    table[text_chars.len()][tokens.len()]
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::SetExpr;

    fn test_schema() -> TableSchema {
        TableSchema {
            name: "t".into(),
            columns: vec![
                Column {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                Column {
                    name: "a".into(),
                    data_type: DataType::Integer,
                    nullable: true,
                },
                Column {
                    name: "b".into(),
                    data_type: DataType::Integer,
                    nullable: true,
                },
            ],
            primary_key: 0,
        }
    }

    fn parse_select(sql: &str) -> Select {
        let statements = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        let Statement::Query(query) = statements.into_iter().next().unwrap() else {
            panic!("expected query");
        };
        match *query.body {
            SetExpr::Select(select) => *select,
            other => panic!("expected SELECT, got {other:?}"),
        }
    }

    fn index(columns: &[&str]) -> IndexSchema {
        IndexSchema {
            name: "idx".into(),
            columns: columns.iter().map(|name| (*name).to_string()).collect(),
            unique: false,
            ready: true,
        }
    }

    #[test]
    fn planner_uses_equality_prefix_and_range() {
        let schema = test_schema();
        let select = parse_select("SELECT * FROM t WHERE a = 1 AND b > 2");
        let plan = plan_index_scan(&select, &schema, None, &[index(&["a", "b"])])
            .unwrap()
            .expect("index should be usable");
        assert_eq!(plan.score, 3);
        assert!(!plan.prefix.is_empty());
        assert!(plan.lower.starts_with(&plan.prefix));
        assert!(plan.upper.is_some());
    }

    #[test]
    fn planner_ignores_non_leading_or_unready_indexes() {
        let schema = test_schema();
        let select = parse_select("SELECT * FROM t WHERE b = 2");
        assert!(
            plan_index_scan(&select, &schema, None, &[index(&["a", "b"])])
                .unwrap()
                .is_none()
        );

        let select = parse_select("SELECT * FROM t WHERE a = 1");
        let mut unready = index(&["a"]);
        unready.ready = false;
        assert!(plan_index_scan(&select, &schema, None, &[unready])
            .unwrap()
            .is_none());
    }

    #[test]
    fn planner_uses_a_range_on_the_first_column() {
        let schema = test_schema();
        let select = parse_select("SELECT * FROM t WHERE a >= 10");
        let plan = plan_index_scan(&select, &schema, None, &[index(&["a"])])
            .unwrap()
            .expect("range should use the index");
        assert_eq!(plan.score, 1);
        assert!(plan.lower.starts_with(&plan.prefix));
        assert!(plan.lower.len() > plan.prefix.len());
        assert_eq!(plan.upper, key_upper_bound(&plan.prefix));
    }
}
