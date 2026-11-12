//! A disposable, local graph demonstration: no servers, ports or persistent data.
use velagraph::{Edge, GraphEngine, GremlinRow, Value, Vertex};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    // Close RocksDB before removing the directory. TempDir also attempts cleanup
    // if an earlier operation returns an error; forced process death may leave it.
    {
        let graph = GraphEngine::open(directory.path())?;
        for (id, name) in [(1, "Ada"), (2, "Grace")] {
            let mut person = Vertex::new(id);
            person.labels.push("Person".into());
            person
                .properties
                .insert("name".into(), Value::Text(name.into()));
            graph.create_vertex(&person)?;
        }
        graph.create_edge(&Edge::new(10, 1, 2, "KNOWS"))?;

        let query = "g.V(1).out('KNOWS').values('name')";
        let result = graph.gremlin(query)?;
        if result.rows != [GremlinRow::Value(Value::Text("Grace".into()))] {
            return Err(std::io::Error::other("unexpected quickstart query result").into());
        }
        println!("Created: Ada --KNOWS--> Grace");
        println!("Query: {query}");
        if let GremlinRow::Value(Value::Text(name)) = &result.rows[0] {
            println!("Result: {name}");
        }
    }
    directory.close()?;
    println!("Temporary graph removed. No server was started.");
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn quickstart_is_repeatable() {
        super::main().unwrap();
        super::main().unwrap();
    }
}
