use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use crate::table::table::{Row, Schema};

pub trait Storage {
    fn create_table(&self, name: &str, schema: &Schema) -> io::Result<()>;
    fn remove_table(&self, name: &str) -> io::Result<()>;
    fn open_table(&self, name: &str) -> io::Result<Box<dyn TableStorage>>;
}

pub trait TableStorage {
    fn schema(&self) -> &Schema;
    fn insert(&mut self, row: &Row) -> io::Result<()>;
    fn scan(&self) -> io::Result<Box<dyn Iterator<Item = io::Result<Row>> + '_>>;
}

pub struct CsvStorage {
    base_path: PathBuf,
}

impl CsvStorage {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let base_path = PathBuf::from(path.as_ref());
        if !base_path.exists() {
            fs::create_dir_all(&base_path)?;
        }
        Ok(CsvStorage { base_path })
    }
}

impl Storage for CsvStorage {
    fn create_table(&self, name: &str, schema: &Schema) -> io::Result<()> {
        let file_path = self.base_path.join(format!("{}.csv", name));
        if file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Table '{}' already exists", name),
            ));
        }

        let mut file = File::create(&file_path)?;

        let schema_header = schema.to_csv_header();
        writeln!(file, "{}", schema_header)?;

        Ok(())
    }

    fn remove_table(&self, name: &str) -> io::Result<()> {
        let file_path = self.base_path.join(format!("{}.csv", name));
        if file_path.exists() {
            fs::remove_file(file_path)?;
        }
        Ok(())
    }

    fn open_table(&self, name: &str) -> io::Result<Box<dyn TableStorage>> {
        let file_path = self.base_path.join(format!("{}.csv", name));
        if !file_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Table '{}' not found", name),
            ));
        }

        let file = File::open(&file_path)?;
        let mut reader = BufReader::new(file);

        let mut header = String::new();
        reader.read_line(&mut header)?;
        header = header.trim().to_string();

        let schema = Schema::from_csv_header(&header);

        Ok(Box::new(CsvTableStorage { file_path, schema }))
    }
}

pub struct CsvTableStorage {
    file_path: PathBuf,
    schema: Schema,
}

impl TableStorage for CsvTableStorage {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn insert(&mut self, row: &Row) -> io::Result<()> {
        if let Err(err) = row.validate(&self.schema) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, err));
        }

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&self.file_path)?;

        writeln!(file, "{}", row.to_csv_string())?;

        Ok(())
    }

    fn scan(&self) -> io::Result<Box<dyn Iterator<Item = io::Result<Row>> + '_>> {
        let file = File::open(&self.file_path)?;
        let reader = BufReader::new(file);
        let lines = reader.lines();

        let schema = self.schema.clone();

        let iter = lines.enumerate().filter_map(move |(i, line_result)| {
            if i == 0 {
                None
            } else {
                Some(line_result.map(|line| Row::from_csv_string(&line)))
            }
        });

        Ok(Box::new(iter))
    }
}
