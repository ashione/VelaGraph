//! Run parser stress cases in child processes: a regression must fail this test,
//! not abort the test runner that is coordinating the rest of the suite.
use std::process::Command;
use velagraph::{
    Error, GremlinQuery, GremlinStart, ObservedGremlinQuery, MAX_GREMLIN_STRUCTURAL_MARKS,
};

#[test]
fn parser_stress_isolated() {
    const CHILD: &str = "ROCKSQL_PARSER_STRESS_CHILD";
    if let Ok(case) = std::env::var(CHILD) {
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || stress_case(&case))
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    for case in ["ids", "chain", "nested", "repeat", "malformed", "lexical"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "parser_stress_isolated", "--nocapture"])
            .env(CHILD, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "case {case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

fn complexity(input: &str) {
    assert!(
        matches!(
            ObservedGremlinQuery::parse(input),
            Err(Error::GremlinComplexityLimit {
                limit: MAX_GREMLIN_STRUCTURAL_MARKS,
                ..
            })
        ),
        "expected observed complexity rejection"
    );
    assert!(
        matches!(
            GremlinQuery::parse(input),
            Err(Error::GremlinComplexityLimit {
                limit: MAX_GREMLIN_STRUCTURAL_MARKS,
                ..
            })
        ),
        "expected complexity rejection"
    );
}

fn stress_case(case: &str) {
    match case {
        "ids" => {
            for start in ["V", "E"] {
                complexity(&format!("g.{start}({})", vec!["1"; 2000].join(",")));
                // g.V(1) has three structural marks; every extra id adds one.
                let n = MAX_GREMLIN_STRUCTURAL_MARKS - 2;
                let query =
                    GremlinQuery::parse(&format!("g.{start}({})", vec!["1"; n].join(","))).unwrap();
                assert!(
                    matches!(query.start(), GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) if ids == &vec![1; n])
                );
                complexity(&format!("g.{start}({})", vec!["1"; n + 1].join(",")));
            }
        }
        "chain" => {
            GremlinQuery::parse(&format!("g.V(){}", ".has('x')".repeat(20))).unwrap();
            complexity(&format!("g.V(){}", ".has('x')".repeat(21)));
            complexity(&format!("g.V(){}", ".has('x')".repeat(1000)));
        }
        "nested" => {
            // Valid official nested traversal syntax is still unsupported by v0,
            // but it must safely reach semantic rejection below admission.
            let nested = format!(
                "g.V().filter({}__.out(){})",
                "__.filter(".repeat(10),
                ")".repeat(10)
            );
            assert!(matches!(
                GremlinQuery::parse(&nested),
                Err(Error::UnsupportedGremlinStep(_))
            ));
            for delimiters in [("[", "]"), ("(", ")"), ("{", "}")] {
                for n in [30, 31] {
                    let result = GremlinQuery::parse(&format!(
                        "g.V({}1{})",
                        delimiters.0.repeat(n),
                        delimiters.1.repeat(n)
                    ));
                    assert!(result.is_err());
                }
                complexity(&format!(
                    "g.V({}1{})",
                    delimiters.0.repeat(2000),
                    delimiters.1.repeat(2000)
                ));
            }
        }
        "repeat" => {
            // Max admitted repeated bodies and modulators must also fit the
            // established 2 MiB parser/validation/destruction stack envelope.
            let suffix = ").repeat(__.out()).emit().times(32)";
            let one = format!("g.V(1{suffix}");
            let marks = one.bytes().filter(|b| b"()[]{},.:;".contains(b)).count();
            let ids = vec!["1"; MAX_GREMLIN_STRUCTURAL_MARKS + 1 - marks].join(",");
            ObservedGremlinQuery::parse(&format!("g.V({ids}{suffix}")).unwrap();
            complexity(&format!("g.V(1,{ids}{suffix}"));
            let nested = format!(
                "g.V(1).{}__.out(){}",
                "repeat(__.".repeat(9),
                ").times(1)".repeat(9),
            );
            assert!(ObservedGremlinQuery::parse(&nested).is_err());
        }
        "malformed" => {
            for prefix in [
                "g.V('",
                "g.V(\"",
                "g.V('\\q",
                "g.V('//\"",
                "// '",
                "g.V() // '\"",
            ] {
                complexity(&format!("{prefix}{}", ",".repeat(2000)));
            }
            // Missing closing marks cannot evade the depth implied by the raw
            // budget; exercise nesting after a real traversal prefix as well.
            for ch in ['(', '[', '{'] {
                assert!(GremlinQuery::parse(&format!(
                    "g.V({}",
                    ch.to_string().repeat(MAX_GREMLIN_STRUCTURAL_MARKS - 2)
                ))
                .is_err());
            }
            // Maximum admitted malformed nesting still must return an error.
            for ch in ['(', '[', '{', ',', '.'] {
                assert!(
                    GremlinQuery::parse(&ch.to_string().repeat(MAX_GREMLIN_STRUCTURAL_MARKS))
                        .is_err()
                );
            }
        }
        "lexical" => {
            // Long lexer-recovery inputs without recursive structural marks.
            for text in [
                "x ", "V ", "1 ", "null ", "''", "\"\"", "\\q", "'", "\"", " ", "💡",
            ] {
                let input = text.repeat(16_000 / text.len());
                assert!(GremlinQuery::parse(&input).is_err());
            }
        }
        _ => panic!("unknown case"),
    }
}

#[test]
fn raw_marks_count_even_in_strings_and_comments() {
    GremlinQuery::parse(&format!("g.V().has('x','{}')", ",".repeat(57))).unwrap();
    assert!(matches!(
        GremlinQuery::parse(&format!("g.V().has('x','{}')", ",".repeat(58))),
        Err(Error::GremlinComplexityLimit {
            marks: 65,
            limit: 64
        })
    ));
    let input = format!("g.V().has('x','{}')", ",".repeat(60));
    // The wrapper contributes seven marks, so this must not reach the parser.
    complexity(&input);
    complexity(&format!("g.V() // {}", ",".repeat(64)));
    GremlinQuery::parse("g.V().has('x','(a),[b].{c}:;')").unwrap();
    GremlinQuery::parse("g.V().has('x','it\\'s \\\\ fine')").unwrap();
    GremlinQuery::parse("g.V().has(\"x\",\"a\\\"b\")").unwrap();
    GremlinQuery::parse(&format!("g.V().has('x','{}')", "漢字".repeat(2000))).unwrap();
}
