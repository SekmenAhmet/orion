use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    fs::{create_dir_all, remove_file, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

/*
* Database structure
*/
pub struct Database {
    db_path: PathBuf,
}

/*
* Table structure
*/
pub struct Table {
    file_path: PathBuf,
}

/*
* Database Structure implementations
TODO: Create Functions to use on Database Objects
*/
impl Database {
    /*
    * Database Object Constructor
    TODO: Create Database Object
    @param P: Path -> AsRef<Path>
    */
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let db_path: PathBuf = PathBuf::from(path.as_ref());

        if !db_path.exists() {
            create_dir_all(&db_path)?;
        }

        Ok(Database { db_path })
    }

    /*
    * Create Table
    @param &self -> using this method on object
    @param name: String -> table name
     */
    pub fn create_table(&self, name: String) -> io::Result<Table> {
        let file_path: PathBuf = self.db_path.join(format!("{}.db", name));

        if !file_path.exists() {
            File::create(&file_path)?;
        }

        Ok(Table { file_path })
    }

    /*
    * Remove Table
    @param &self -> using this method on object
    @param name: String -> table name
     */
    #[allow(dead_code)]
    pub fn remove_table(&self, name: String) -> io::Result<()> {
        let file_path: PathBuf = self.db_path.join(format!("{}.db", name));

        if file_path.exists() {
            remove_file(file_path)?;
        } else {
            println!("Nom de la table incorrect");
        }

        Ok(())
    }
}

/*
* Table Structure implementations
TODO: Create Functions to use on Table Objects
*/
impl Table {
    /*
    * Insert Fn()
    TODO: Insert Record in tables
    @params T: Serialize
    @params &self -> using this method on object
    @params record: &T -> Values to insert
    */
    #[allow(dead_code)]
    pub fn insert<T: Serialize>(&self, record: &T) -> io::Result<()> {
        let mut value = match serde_json::to_value(record) {
            Ok(v) => v,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };

        let max_id = self.get_max_id()?;
        let next_id = (max_id + 1).to_string();

        if let Some(obj) = value.as_object_mut() {
            obj.insert("id".to_string(), Value::String(next_id));
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "L'enregistrement doit être un objet JSON",
            ));
        };

        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)?;

        let mut writer = BufWriter::new(file);
        let serialized: String = value.to_string();

        writeln!(writer, "{}", serialized)?;

        Ok(())
    }

    /*
    * get_max_id
    TODO: Get the max id on the table for the autoincrement
    @param &self -> to use this function on Table objects
    */
    #[allow(dead_code)]
    fn get_max_id(&self) -> io::Result<u32> {
        let file = match File::open(&self.file_path) {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };

        let reader = BufReader::new(file);
        let mut max_id = 0;

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let value: Value = match serde_json::from_str(&line) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if let Some(obj) = value.as_object() {
                    if let Some(id_value) = obj.get("id") {
                        if let Some(id_str) = id_value.as_str() {
                            if let Ok(id_num) = id_str.parse::<u32>() {
                                max_id = max_id.max(id_num);
                            }
                        }
                    }
                }
            }
        }

        Ok(max_id)
    }

    pub fn fetch_all<T: for<'de> Deserialize<'de>>(&self) -> io::Result<Vec<T>> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        let mut records = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let record: T = serde_json::from_str(&line)?;
                records.push(record);
            }
        }
        Ok(records)
    }

    #[allow(dead_code)]
    fn compare_id(value: &Value, id_value: &str) -> bool {
        if let Some(obj) = value.as_object() {
            if let Some(field_value) = obj.get("id") {
                if let Some(field_str) = field_value.as_str() {
                    return field_str == id_value;
                }
            }
        }
        false
    }

    #[allow(dead_code)]
    fn get_record_if_id_matches<T: for<'de> Deserialize<'de>>(
        &self,
        line: &str,
        id_value: &str,
    ) -> Option<T> {
        let value: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => return None,
        };

        if Self::compare_id(&value, id_value) {
            match serde_json::from_str::<T>(line) {
                Ok(record) => Some(record),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn fetch_one<T: for<'de> Deserialize<'de>>(&self, id_value: &str) -> io::Result<Option<T>> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                if let Some(record) = self.get_record_if_id_matches::<T>(&line, id_value) {
                    return Ok(Some(record));
                }
            }
        }

        Ok(None)
    }

    #[allow(dead_code)]
    pub fn update<T: Serialize + for<'de> Deserialize<'de>>(
        &self,
        id: &str,
        update_fn: impl Fn(&mut T),
    ) -> io::Result<bool> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        let mut updated = false;

        for line in reader.lines() {
            let line = line?;
            lines.push(line.clone());

            if !line.trim().is_empty() && !updated {
                if let Some(mut record) = self.get_record_if_id_matches::<T>(&line, id) {
                    update_fn(&mut record);

                    let updated_json = serde_json::to_string(&record)?;
                    *lines.last_mut().unwrap() = updated_json;
                    updated = true;
                }
            }
        }

        if updated {
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.file_path)?;

            let mut writer = BufWriter::new(file);

            for line in lines {
                writeln!(writer, "{}", line)?;
            }
        }
        Ok(updated)
    }

    #[allow(dead_code)]
    pub fn delete<T: for<'de> Deserialize<'de>>(&self, id: &str) -> io::Result<bool> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        let mut deleted = false;

        for line in reader.lines() {
            let line = line?;

            if !line.trim().is_empty() {
                if let Some(_) = self.get_record_if_id_matches::<T>(&line, id) {
                    deleted = true;
                    continue;
                }
            }
            lines.push(line);
        }

        if deleted {
            let file = OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&self.file_path)?;

            let mut writer = BufWriter::new(file);

            for line in lines {
                writeln!(writer, "{}", line)?;
            }
        }

        Ok(deleted)
    }
}
