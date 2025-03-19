use std::collections::HashMap;
use std::io;
use std::path::Path;

use crate::storage::storage::{CsvStorage, Storage, TableStorage};
use crate::table::table::{Row, Schema};

pub struct Database {
    name: String,
    storage: Box<dyn Storage>,
    tables: HashMap<String, Table>,
}

pub struct Table {
    name: String,
    storage: Box<dyn TableStorage>,
}

impl Table {
    pub fn new(name: String, storage: Box<dyn TableStorage>) -> Self {
        Table { name, storage }
    }

    pub fn insert(&mut self, values: Vec<String>) -> io::Result<()> {
        let row = Row::new(values);
        self.storage.insert(&row)
    }

    pub fn scan_all(&self) -> io::Result<Vec<Row>> {
        self.storage.scan()?.collect::<io::Result<Vec<_>>>()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema(&self) -> &Schema {
        self.storage.schema()
    }

    pub fn to_json(&self) -> io::Result<serde_json::Value> {
        let rows = self.scan_all()?;
        let schema_clone = self.schema().clone();

        let json_rows: Vec<serde_json::Value> =
            rows.iter().map(|row| row.to_json(&schema_clone)).collect();

        Ok(serde_json::Value::Array(json_rows))
    }

    pub fn insert_from_json(&mut self, json: &serde_json::Value) -> io::Result<usize> {
        if !json.is_array() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected JSON array",
            ));
        }

        let schema_clone = self.schema().clone();
        let json_array = json.as_array().unwrap();
        let mut count = 0;

        let rows_result: Result<Vec<_>, _> = json_array
            .iter()
            .map(|item| Row::from_json(item, &schema_clone))
            .collect();

        let rows = match rows_result {
            Ok(rows) => rows,
            Err(e) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Error converting JSON to row: {}", e),
                ));
            }
        };

        for row in rows {
            self.storage.insert(&row)?;
            count += 1;
        }

        Ok(count)
    }
}

impl Database {
    pub fn new<P: AsRef<Path>>(name: &str, path: P) -> io::Result<Self> {
        let storage = Box::new(CsvStorage::new(path)?);

        Ok(Database {
            name: name.to_string(),
            storage,
            tables: HashMap::new(),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create_table(&mut self, table_name: &str, columns: Vec<String>) -> io::Result<()> {
        let schema = Schema::new(columns);
        self.storage.create_table(table_name, &schema)
    }

    pub fn drop_table(&mut self, table_name: &str) -> io::Result<()> {
        self.tables.remove(table_name);
        self.storage.remove_table(table_name)
    }

    pub fn get_table(&mut self, table_name: &str) -> io::Result<&mut Table> {
        if !self.tables.contains_key(table_name) {
            let table_storage = self.storage.open_table(table_name)?;
            let table = Table::new(table_name.to_string(), table_storage);
            self.tables.insert(table_name.to_string(), table);
        }

        Ok(self.tables.get_mut(table_name).unwrap())
    }

    pub fn list_opened_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn insert(&mut self, table_name: &str, values: Vec<String>) -> io::Result<()> {
        let table = self.get_table(table_name)?;
        table.insert(values)
    }

    pub fn select_all(&mut self, table_name: &str) -> io::Result<Vec<Row>> {
        let table = self.get_table(table_name)?;
        table.scan_all()
    }

    pub fn export_table_json(&mut self, table_name: &str) -> io::Result<String> {
        let table = self.get_table(table_name)?;
        let json = table.to_json()?;
        Ok(serde_json::to_string_pretty(&json)?)
    }

    pub fn import_table_json(&mut self, table_name: &str, json_str: &str) -> io::Result<usize> {
        let json: serde_json::Value = serde_json::from_str(json_str)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let table = self.get_table(table_name)?;
        table.insert_from_json(&json)
    }

    pub fn to_json_schema(&mut self) -> io::Result<serde_json::Value> {
        let mut db_schema = serde_json::Map::new();

        for table_name in self.list_opened_tables() {
            let table = self.get_table(&table_name)?;
            let schema = table.schema();

            let columns = schema
                .columns()
                .iter()
                .map(|c| serde_json::Value::String(c.clone()))
                .collect();

            db_schema.insert(table_name, serde_json::Value::Array(columns));
        }

        Ok(serde_json::Value::Object(db_schema))
    }
}
