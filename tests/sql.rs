use rocksdb_sql_table::{Outcome, QueryResult, TableEngine, Value};

struct TestDb {
    engine: TableEngine,
    _dir: tempfile::TempDir,
}

impl TestDb {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = TableEngine::open(dir.path()).expect("open engine");
        Self { engine, _dir: dir }
    }

    fn execute(&self, sql: &str) -> Vec<Outcome> {
        self.engine.execute(sql).expect(sql)
    }

    fn query(&self, sql: &str) -> QueryResult {
        match self.execute(sql).pop().expect("one outcome") {
            Outcome::Rows(result) => result,
            other => panic!("expected rows from `{sql}`, got {other:?}"),
        }
    }

    fn affected(&self, sql: &str) -> usize {
        match self.execute(sql).pop().expect("one outcome") {
            Outcome::Modified { affected } => affected,
            other => panic!("expected modification from `{sql}`, got {other:?}"),
        }
    }

    fn error(&self, sql: &str) -> String {
        self.engine
            .execute(sql)
            .expect_err(&format!("expected `{sql}` to fail"))
            .to_string()
    }

    fn setup_users(&self) {
        self.execute(
            "CREATE TABLE users (\
               id INT PRIMARY KEY,\
               name TEXT NOT NULL,\
               age INT,\
               active BOOLEAN\
             )",
        );
        assert_eq!(
            self.affected(
                "INSERT INTO users (id, name, age, active) VALUES \
                 (1, 'Ada', 36, TRUE),\
                 (2, 'Grace', 45, FALSE),\
                 (3, 'Linus', NULL, NULL)"
            ),
            3
        );
    }
}

fn query(db: &TestDb, sql: &str) -> QueryResult {
    db.query(sql)
}

fn rendered(result: &QueryResult) -> Vec<Vec<String>> {
    result
        .rows
        .iter()
        .map(|row| row.iter().map(Value::render).collect())
        .collect()
}

fn expect(result: &QueryResult, expected: &[&[&str]]) {
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect();
    assert_eq!(rendered(result), expected);
}

fn assert_header(result: &QueryResult, expected: &[&str]) {
    assert_eq!(result.columns, expected);
}

fn assert_error_contains(db: &TestDb, sql: &str, needle: &str) {
    let error = db.error(sql);
    assert!(
        error.contains(needle),
        "expected `{needle}` in error for `{sql}`, got `{error}`"
    );
}

#[test]
fn create_insert_and_select() {
    let db = TestDb::new();
    let outcome = db
        .execute(
            "CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL, age INT, active BOOLEAN)",
        )
        .pop()
        .unwrap();
    assert_eq!(
        outcome,
        Outcome::Created {
            table: "users".into(),
            existed: false
        }
    );
    assert_eq!(
        db.affected(
            "INSERT INTO users (id, name, age, active) VALUES \
             (1, 'Ada', 36, TRUE), (2, 'Grace', 45, FALSE), (3, 'Linus', NULL, NULL)"
        ),
        3
    );

    let result = query(&db, "SELECT id, name, age FROM users ORDER BY id");
    assert_header(&result, &["id", "name", "age"]);
    expect(
        &result,
        &[
            &["1", "Ada", "36"],
            &["2", "Grace", "45"],
            &["3", "Linus", "NULL"],
        ],
    );

    let result = query(&db, "SELECT * FROM users WHERE id = 2");
    assert_header(&result, &["id", "name", "age", "active"]);
    expect(&result, &[&["2", "Grace", "45", "false"]]);

    assert_eq!(
        db.affected("INSERT INTO users VALUES (4, 'Barbara', 60, TRUE)"),
        1
    );
}

#[test]
fn point_lookups_where_logic_and_aliases() {
    let db = TestDb::new();
    db.setup_users();

    expect(
        &query(&db, "SELECT name FROM users WHERE id = 2"),
        &[&["Grace"]],
    );
    expect(&query(&db, "SELECT id FROM users WHERE id = 99"), &[]);
    expect(
        &query(&db, "SELECT id FROM users WHERE id = 1.0"),
        &[&["1"]],
    );
    expect(&query(&db, "SELECT id FROM users WHERE id = 1.5"), &[]);
    expect(
        &query(&db, "SELECT id FROM users WHERE id = 2 AND active = FALSE"),
        &[&["2"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE id = 1 OR id = 3 ORDER BY id",
        ),
        &[&["1"], &["3"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE NOT (id = 2) ORDER BY id"),
        &[&["1"], &["3"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE (id = 1 OR id = 2) AND age > 40",
        ),
        &[&["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE id = 1 AND 1 = 0"),
        &[],
    );
    expect(
        &query(&db, "SELECT u.name FROM users AS u WHERE u.id = 1"),
        &[&["Ada"]],
    );
    expect(
        &query(&db, "SELECT users.name FROM users WHERE users.id = 2"),
        &[&["Grace"]],
    );
    assert_error_contains(
        &db,
        "SELECT other.name FROM users WHERE other.id = 1",
        "unknown table qualifier",
    );
}

#[test]
fn sql_three_valued_null_logic() {
    let db = TestDb::new();
    db.setup_users();

    expect(&query(&db, "SELECT id FROM users WHERE age = NULL"), &[]);
    expect(
        &query(&db, "SELECT id FROM users WHERE age IS NULL"),
        &[&["3"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE age IS NOT NULL ORDER BY id",
        ),
        &[&["1"], &["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE age <> 36"),
        &[&["2"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE active = TRUE OR active IS NULL ORDER BY id",
        ),
        &[&["1"], &["3"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE NOT (age > 40)"),
        &[&["1"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE age = 36 OR age = 45 ORDER BY id",
        ),
        &[&["1"], &["2"]],
    );
}

#[test]
fn order_by_limit_offset_and_distinct() {
    let db = TestDb::new();
    db.setup_users();

    expect(
        &query(&db, "SELECT id FROM users ORDER BY age DESC"),
        &[&["2"], &["1"], &["3"]],
    );
    expect(
        &query(&db, "SELECT id FROM users ORDER BY age ASC"),
        &[&["1"], &["2"], &["3"]],
    );
    expect(
        &query(&db, "SELECT id FROM users ORDER BY age ASC LIMIT 2"),
        &[&["1"], &["2"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users ORDER BY age ASC LIMIT 2 OFFSET 1",
        ),
        &[&["2"], &["3"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users ORDER BY age DESC LIMIT 1 OFFSET 1",
        ),
        &[&["1"]],
    );
    expect(&query(&db, "SELECT id FROM users LIMIT 0"), &[]);
    expect(&query(&db, "SELECT id FROM users OFFSET 5"), &[]);

    db.execute("CREATE TABLE visits (id INT PRIMARY KEY, city TEXT)");
    db.execute("INSERT INTO visits VALUES (1, 'Paris'), (2, 'Paris'), (3, 'Rome')");
    expect(
        &query(&db, "SELECT DISTINCT city FROM visits"),
        &[&["Paris"], &["Rome"]],
    );
    expect(
        &query(&db, "SELECT DISTINCT city FROM visits ORDER BY city DESC"),
        &[&["Rome"], &["Paris"]],
    );
    expect(
        &query(&db, "SELECT DISTINCT active FROM users ORDER BY active"),
        &[&["false"], &["true"], &["NULL"]],
    );
    assert_header(
        &query(&db, "SELECT DISTINCT city AS place FROM visits"),
        &["place"],
    );
}

#[test]
fn update_and_delete() {
    let db = TestDb::new();
    db.setup_users();

    assert_eq!(db.affected("UPDATE users SET age = 37 WHERE id = 1"), 1);
    expect(
        &query(&db, "SELECT age FROM users WHERE id = 1"),
        &[&["37"]],
    );

    assert_eq!(db.affected("UPDATE users SET active = TRUE"), 3);
    expect(
        &query(&db, "SELECT COUNT(*) FROM users WHERE active = TRUE"),
        &[&["3"]],
    );

    assert_eq!(db.affected("UPDATE users SET age = NULL WHERE id = 2"), 1);
    expect(
        &query(&db, "SELECT id FROM users WHERE age IS NULL ORDER BY id"),
        &[&["2"], &["3"]],
    );

    assert_eq!(db.affected("UPDATE users SET id = 10 WHERE id = 1"), 1);
    expect(
        &query(&db, "SELECT id FROM users ORDER BY id"),
        &[&["2"], &["3"], &["10"]],
    );

    assert_eq!(db.affected("DELETE FROM users WHERE id = 2"), 1);
    expect(
        &query(&db, "SELECT id FROM users ORDER BY id"),
        &[&["3"], &["10"]],
    );

    assert_eq!(db.affected("DELETE FROM users WHERE age = NULL"), 0);
    assert_eq!(db.affected("DELETE FROM users"), 2);
    expect(&query(&db, "SELECT COUNT(*) FROM users"), &[&["0"]]);
}

#[test]
fn update_primary_key_conflicts_leave_data_untouched() {
    let db = TestDb::new();
    db.execute("CREATE TABLE pairs (id INT PRIMARY KEY, label TEXT)");
    db.execute("INSERT INTO pairs VALUES (1, 'one'), (2, 'two'), (3, 'three')");

    assert_eq!(db.affected("UPDATE pairs SET id = 4 WHERE id = 1"), 1);
    expect(
        &query(&db, "SELECT id, label FROM pairs ORDER BY id"),
        &[&["2", "two"], &["3", "three"], &["4", "one"]],
    );

    assert_error_contains(
        &db,
        "UPDATE pairs SET id = 2 WHERE id = 4",
        "duplicate primary key",
    );
    expect(
        &query(&db, "SELECT id, label FROM pairs ORDER BY id"),
        &[&["2", "two"], &["3", "three"], &["4", "one"]],
    );

    assert_error_contains(&db, "UPDATE pairs SET id = 1", "duplicate primary key");
    expect(
        &query(&db, "SELECT id, label FROM pairs ORDER BY id"),
        &[&["2", "two"], &["3", "three"], &["4", "one"]],
    );

    assert_eq!(db.affected("UPDATE pairs SET id = 1 WHERE id = 2"), 1);
    expect(
        &query(&db, "SELECT id, label FROM pairs ORDER BY id"),
        &[&["1", "two"], &["3", "three"], &["4", "one"]],
    );
}

#[test]
fn count_aggregates() {
    let db = TestDb::new();
    db.setup_users();

    expect(&query(&db, "SELECT COUNT(*) FROM users"), &[&["3"]]);
    expect(&query(&db, "SELECT COUNT(age) FROM users"), &[&["2"]]);
    expect(
        &query(&db, "SELECT COUNT(*) AS n FROM users WHERE age >= 36"),
        &[&["2"]],
    );
    expect(
        &query(&db, "SELECT COUNT(*) FROM users WHERE id = 2"),
        &[&["1"]],
    );
    expect(
        &query(&db, "SELECT COUNT(*) FROM users WHERE id = 99"),
        &[&["0"]],
    );
    expect(&query(&db, "SELECT COUNT(*) FROM users LIMIT 0"), &[]);
    assert_header(&query(&db, "SELECT COUNT(*) AS n FROM users"), &["n"]);

    db.execute("CREATE TABLE empty_table (id INT PRIMARY KEY)");
    expect(&query(&db, "SELECT COUNT(*) FROM empty_table"), &[&["0"]]);

    assert_error_contains(
        &db,
        "SELECT name, COUNT(*) FROM users",
        "cannot mix aggregate and non-aggregate",
    );
    assert_error_contains(
        &db,
        "SELECT COUNT(*) FROM users ORDER BY id",
        "ORDER BY is not supported",
    );
    assert_error_contains(&db, "SELECT SUM(age) FROM users", "only COUNT is supported");
}

#[test]
fn like_between_and_in() {
    let db = TestDb::new();
    db.setup_users();

    expect(
        &query(&db, "SELECT name FROM users WHERE name LIKE 'A%'"),
        &[&["Ada"]],
    );
    expect(
        &query(
            &db,
            "SELECT name FROM users WHERE name LIKE '%a%' ORDER BY name",
        ),
        &[&["Ada"], &["Grace"]],
    );
    expect(
        &query(&db, "SELECT name FROM users WHERE name NOT LIKE '%a%'"),
        &[&["Linus"]],
    );
    expect(
        &query(&db, "SELECT name FROM users WHERE name LIKE '_d_'"),
        &[&["Ada"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE age BETWEEN 36 AND 45 ORDER BY id",
        ),
        &[&["1"], &["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE age NOT BETWEEN 40 AND 50"),
        &[&["1"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE age IN (36, 45) ORDER BY id",
        ),
        &[&["1"], &["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE age IN (36, NULL)"),
        &[&["1"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE age NOT IN (36)"),
        &[&["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE age NOT IN (36, NULL)"),
        &[],
    );

    db.execute("CREATE TABLE patterns (id INT PRIMARY KEY, s TEXT)");
    db.execute("INSERT INTO patterns VALUES (1, 'A%B'), (2, 'AxB')");
    expect(
        &query(&db, "SELECT s FROM patterns WHERE s LIKE 'A!%B' ESCAPE '!'"),
        &[&["A%B"]],
    );
    assert_error_contains(
        &db,
        "SELECT id FROM users WHERE age LIKE '3%'",
        "cannot compare",
    );
}

#[test]
fn text_primary_keys_are_ordered_and_escaped() {
    let db = TestDb::new();
    db.execute("CREATE TABLE notes (slug TEXT PRIMARY KEY, body TEXT)");
    assert_eq!(
        db.affected(
            "INSERT INTO notes VALUES \
             ('', 'empty'), ('a', 'A'), ('aa', 'AA'), ('b', 'B'), \
             ('O''Brien', 'quote'), ('中文', 'unicode')"
        ),
        6
    );

    expect(
        &query(&db, "SELECT slug FROM notes ORDER BY slug"),
        &[&[""], &["O'Brien"], &["a"], &["aa"], &["b"], &["中文"]],
    );
    expect(
        &query(&db, "SELECT body FROM notes WHERE slug = 'O''Brien'"),
        &[&["quote"]],
    );
    assert_eq!(
        db.affected("UPDATE notes SET body = 'changed' WHERE slug = 'b'"),
        1
    );
    expect(
        &query(&db, "SELECT body FROM notes WHERE slug = 'b'"),
        &[&["changed"]],
    );
    assert_eq!(db.affected("DELETE FROM notes WHERE slug = ''"), 1);
    expect(&query(&db, "SELECT COUNT(*) FROM notes"), &[&["5"]]);

    assert_error_contains(
        &db,
        "INSERT INTO notes VALUES ('a', 'duplicate')",
        "duplicate primary key",
    );
    assert_error_contains(
        &db,
        "INSERT INTO notes VALUES (NULL, 'missing key')",
        "cannot be NULL",
    );
}

#[test]
fn real_and_boolean_values_are_strictly_typed() {
    let db = TestDb::new();
    db.execute("CREATE TABLE metrics (id INT PRIMARY KEY, score REAL, ok BOOLEAN)");
    db.execute("INSERT INTO metrics VALUES (1, 1, TRUE), (2, 2.5, FALSE)");

    expect(
        &query(&db, "SELECT score FROM metrics ORDER BY score"),
        &[&["1"], &["2.5"]],
    );
    expect(
        &query(&db, "SELECT id FROM metrics WHERE score > 1.5"),
        &[&["2"]],
    );
    expect(
        &query(&db, "SELECT id FROM metrics WHERE score = 1"),
        &[&["1"]],
    );
    expect(
        &query(&db, "SELECT id FROM metrics WHERE ok = TRUE"),
        &[&["1"]],
    );
    expect(&query(&db, "SELECT COUNT(score) FROM metrics"), &[&["2"]]);

    db.execute("INSERT INTO metrics VALUES (3, NULL, NULL)");
    expect(&query(&db, "SELECT COUNT(score) FROM metrics"), &[&["2"]]);

    assert_error_contains(
        &db,
        "INSERT INTO metrics VALUES (4, 'bad', TRUE)",
        "type mismatch for column `score`",
    );
    assert_error_contains(
        &db,
        "INSERT INTO metrics VALUES (5, 1, 1)",
        "type mismatch for column `ok`",
    );
    assert_error_contains(
        &db,
        "UPDATE metrics SET score = 'bad' WHERE id = 1",
        "type mismatch for column `score`",
    );
}

#[test]
fn constraints_and_unsupported_features() {
    let db = TestDb::new();
    db.setup_users();

    assert_error_contains(
        &db,
        "INSERT INTO users VALUES (1, 'Duplicate', 1, TRUE)",
        "duplicate primary key",
    );
    assert_error_contains(
        &db,
        "INSERT OR REPLACE INTO users VALUES (1, 'Replaced', 1, TRUE)",
        "INSERT OR/IGNORE/REPLACE",
    );
    assert_error_contains(
        &db,
        "INSERT INTO users (id) VALUES (4)",
        "NOT NULL constraint failed for column `name`",
    );
    assert_error_contains(
        &db,
        "INSERT INTO users VALUES (4, 123, 1, TRUE)",
        "type mismatch for column `name`",
    );
    assert_error_contains(
        &db,
        "INSERT INTO users (id, unknown) VALUES (4, 1)",
        "column `unknown` does not exist",
    );
    assert_error_contains(
        &db,
        "INSERT INTO users VALUES (4, 'Too', 'many', TRUE, 1)",
        "invalid row",
    );
    assert_error_contains(
        &db,
        "SELECT unknown FROM users",
        "column `unknown` does not exist",
    );
    assert_error_contains(
        &db,
        "SELECT id FROM users WHERE age = 'old'",
        "cannot compare",
    );
    assert_error_contains(
        &db,
        "UPDATE users SET age = age + 1",
        "only literals are supported",
    );
    assert_error_contains(
        &db,
        "UPDATE users SET age = 1, age = 2",
        "specified more than once",
    );
    assert_error_contains(&db, "ALTER TABLE users ADD COLUMN x INT", "unsupported");
    assert_error_contains(&db, "SELECT id FROM users JOIN users AS u ON 1 = 1", "JOIN");
    assert_error_contains(&db, "SELECT 1", "exactly one table");

    assert_error_contains(
        &db,
        "CREATE TABLE no_key (id INT)",
        "must declare exactly one primary key",
    );
    assert_error_contains(
        &db,
        "CREATE TABLE bad_key (id REAL PRIMARY KEY)",
        "must use INT or TEXT",
    );
    assert_error_contains(
        &db,
        "CREATE TABLE duplicate_columns (id INT PRIMARY KEY, id TEXT)",
        "specified more than once",
    );
    assert_error_contains(
        &db,
        "CREATE TABLE users (id INT PRIMARY KEY)",
        "already exists",
    );
    assert_error_contains(
        &db,
        "CREATE TABLE t (id INT, PRIMARY KEY (id, id))",
        "composite primary keys",
    );

    let outcome = db
        .execute("CREATE TABLE IF NOT EXISTS users (id INT PRIMARY KEY)")
        .pop()
        .unwrap();
    assert_eq!(
        outcome,
        Outcome::Created {
            table: "users".into(),
            existed: true
        }
    );

    assert_error_contains(&db, "DROP TABLE missing", "does not exist");
    let outcome = db.execute("DROP TABLE IF EXISTS missing").pop().unwrap();
    assert_eq!(
        outcome,
        Outcome::Dropped {
            table: "missing".into(),
            existed: false
        }
    );
}

#[test]
fn multiple_statements_and_parse_errors() {
    let db = TestDb::new();
    let outcomes = db.execute(
        "CREATE TABLE t (id INT PRIMARY KEY, name TEXT);\
         INSERT INTO t VALUES (1, 'one'), (2, 'two');\
         SELECT id FROM t ORDER BY id;",
    );
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], Outcome::Created { .. }));
    assert_eq!(outcomes[1], Outcome::Modified { affected: 2 });
    assert!(matches!(outcomes[2], Outcome::Rows(_)));

    assert!(db.engine.execute("").unwrap().is_empty());
    let error = db.error("SELEC 1");
    assert!(
        error.contains("parse error") || error.contains("Expected"),
        "{error}"
    );
}

#[test]
fn persistence_listing_and_drop_recreate() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = TableEngine::open(dir.path()).unwrap();
        engine
            .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")
            .unwrap();
        engine
            .execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')")
            .unwrap();
        let tables = engine.list_tables().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].name, "users");
        assert!(tables[0].describe().contains("PRIMARY KEY"));
        assert_eq!(engine.table_schema("users").unwrap().columns.len(), 2);
    }

    {
        let engine = TableEngine::open(dir.path()).unwrap();
        let outcomes = engine
            .execute("SELECT name FROM users ORDER BY id")
            .unwrap();
        match &outcomes[0] {
            Outcome::Rows(result) => expect(result, &[&["Ada"], &["Grace"]]),
            other => panic!("unexpected outcome {other:?}"),
        }

        engine.execute("DROP TABLE users").unwrap();
        assert!(engine.list_tables().unwrap().is_empty());
        assert!(engine.execute("SELECT * FROM users").is_err());

        engine
            .execute("CREATE TABLE users (id INT PRIMARY KEY, email TEXT)")
            .unwrap();
        let outcomes = engine.execute("SELECT COUNT(*) FROM users").unwrap();
        match &outcomes[0] {
            Outcome::Rows(result) => expect(result, &[&["0"]]),
            other => panic!("unexpected outcome {other:?}"),
        }
    }
}

#[test]
fn boolean_and_null_predicates() {
    let db = TestDb::new();
    db.setup_users();

    expect(
        &query(&db, "SELECT id FROM users WHERE active ORDER BY id"),
        &[&["1"]],
    );
    expect(
        &query(&db, "SELECT id FROM users WHERE NOT active ORDER BY id"),
        &[&["2"]],
    );
    expect(
        &query(
            &db,
            "SELECT id FROM users WHERE active IS NOT NULL ORDER BY id",
        ),
        &[&["1"], &["2"]],
    );
}

#[test]
fn default_nullable_columns_and_primary_key_position() {
    let db = TestDb::new();
    db.execute("CREATE TABLE t (name TEXT, id INT, PRIMARY KEY (id))");
    db.execute("INSERT INTO t (id) VALUES (1)");
    expect(&query(&db, "SELECT id, name FROM t"), &[&["1", "NULL"]]);
    assert_error_contains(
        &db,
        "INSERT INTO t (name) VALUES ('missing id')",
        "cannot be NULL",
    );
}
