use std::{io, path::PathBuf};

use db::database::Database;
use serde::{Deserialize, Serialize};

mod db;

fn main() -> io::Result<()> {
    /*
     * Create Database
     */
    let db_path = PathBuf::from("./src/db/test");
    let db = Database::new(db_path)?;

    /*
     * Create Table
     */
    let table = db.create_table(String::from("table"))?;

    /*
     *Struct for Test
     */
    #[derive(Debug, Serialize, Deserialize)]
    struct Person {
        name: String,
    }

    // let test = Person {
    //     name: String::from("Ahmet"),
    // };

    /*
     * Insert
     */
    // let _ = table.insert(&test);

    /*
    * Update
    let _ = table.update::<Person>("01", |person| person.name = String::from("Sekmen"))?;
    */

    /*
     * Fetch All
     */
    let _: Vec<Person> = table.fetch_all()?;

    // for person in &people {
    //     println!("Nom : {}", person.name);
    // }

    /*
    * Fetch One

    let user = table.fetch_one::<Person>("1")?;

    match user {
        Some(user) => println!("Utilisateur trouvé: {:?}", user.name),
        None => println!("Aucun utilisateur trouvé avec cet ID"),
    }
    */

    /*
    * Delete
    let _ = table.delete::<Person>("1")?;
    */

    Ok(())
}
