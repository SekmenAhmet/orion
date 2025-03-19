use std::io;

use database::database::Database;

mod database;
mod datatypes;
mod storage;
mod table;

fn main() -> io::Result<()> {
    let db_path = "data";

    let mut db = Database::new("my_base", db_path)?;
    print!("Base de donnée {} initialisée", db.name());

    let colonnes = vec![
        "id".to_string(),
        "firstname".to_string(),
        "lastname".to_string(),
        "email".to_string(),
    ];

    db.create_table("user", colonnes)?;
    println!("Table 'user' créee");

    db.insert(
        "user",
        vec![
            "1".to_string(),
            "Ahmet".to_string(),
            "Sekmen".to_string(),
            "sekmenahmet04@gmail.com".to_string(),
        ],
    )?;

    db.insert(
        "user",
        vec![
            "2".to_string(),
            "Bob".to_string(),
            "25".to_string(),
            "bob@example.com".to_string(),
        ],
    )?;

    db.insert(
        "user",
        vec![
            "3".to_string(),
            "Charlie".to_string(),
            "35".to_string(),
            "charlie@example.com".to_string(),
        ],
    )?;

    println!("Utilisateurs insérés");

    let table = db.get_table("user")?;
    let schema = table.schema();

    println!("{}", schema.to_csv_header());

    let rows = db.select_all("user")?;

    for row in rows {
        println!("{}", row.to_csv_string());
    }

    let json = db.export_table_json("user")?;
    println!("\nDonnées au format JSON");
    println!("{}", json);

    let tables = db.list_opened_tables();
    println!("\nTables ouvertes: {:?}", tables);

    let db_schema = db.to_json_schema()?;
    println!("\nSchéma de la base de donnée au format JSON");
    println!("{}", serde_json::to_string_pretty(&db_schema)?);

    println!("\nOpérations terminées");

    Ok(())
}
