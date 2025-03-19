use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    columns: Vec<String>,
    #[serde(skip)]
    column_index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<String>) -> Self {
        let mut column_index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            column_index.insert(col.clone(), i);
        }

        Schema {
            columns,
            column_index,
        }
    }

    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.column_index.get(name).copied()
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn to_csv_header(&self) -> String {
        self.columns.join(",")
    }

    pub fn from_csv_header(header: &str) -> Self {
        let columns = header
            .split(",")
            .map(|s| s.trim().to_string())
            .collect::<Vec<String>>();

        Schema::new(columns)
    }

    pub fn rebuild_index(&mut self) {
        self.column_index.clear();
        for (i, col) in self.columns.iter().enumerate() {
            self.column_index.insert(col.clone(), i);
        }
    }
}

#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<String>,
}

impl Row {
    pub fn new(values: Vec<String>) -> Self {
        Row { values }
    }

    pub fn values(&self) -> &[String] {
        &self.values
    }

    pub fn get(&self, index: usize) -> Option<&String> {
        self.values.get(index)
    }

    pub fn get_by_name(&self, schema: &Schema, name: &str) -> Option<&String> {
        schema.column_index(name).and_then(|idx| self.get(idx))
    }

    pub fn to_csv_string(&self) -> String {
        let escaped_values: Vec<String> = self
            .values
            .iter()
            .map(|v| {
                if v.contains(",") || v.contains('"') || v.contains('\n') {
                    let escaped = v.replace('"', "\"\"");
                    format!("\"{}\"", escaped)
                } else {
                    v.clone()
                }
            })
            .collect();

        escaped_values.join(",")
    }

    pub fn from_csv_string(s: &str) -> Self {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;

        for c in s.chars() {
            match c {
                '"' => {
                    if in_quotes && c.is_ascii_punctuation() && current.chars().next() == Some('"')
                    {
                        current.push('"');
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                ',' if !in_quotes => {
                    let value =
                        if current.starts_with('"') && current.ends_with('"') && current.len() >= 2
                        {
                            let inner = &current[1..current.len() - 1];
                            inner.replace("\"\"", "\"")
                        } else {
                            current
                        };
                    values.push(value);
                    current = String::new();
                }
                _ => current.push(c),
            }
        }

        if !current.is_empty() {
            let value = if current.starts_with('"') && current.ends_with('"') && current.len() >= 2
            {
                let inner = &current[1..current.len() - 1];
                inner.replace("\"\"", "\"")
            } else {
                current
            };
            values.push(value);
        }

        Row::new(values)
    }

    pub fn to_json(&self, schema: &Schema) -> serde_json::Value {
        let mut obj = serde_json::Map::new();

        for (i, col) in schema.columns().iter().enumerate() {
            if i < self.values.len() {
                let value = &self.values[i];
                let json_value = if value == "NULL" {
                    serde_json::Value::Null
                } else if let Ok(num) = value.parse::<i64>() {
                    serde_json::Value::Number(num.into())
                } else if let Ok(num) = value.parse::<f64>() {
                    match serde_json::Number::from_f64(num) {
                        Some(n) => serde_json::Value::Number(n),
                        None => serde_json::Value::String(value.clone()),
                    }
                } else if value == "true" {
                    serde_json::Value::Bool(true)
                } else if value == "false" {
                    serde_json::Value::Bool(false)
                } else if value.starts_with('[') && value.ends_with(']') {
                    match serde_json::from_str::<serde_json::Value>(value) {
                        Ok(array) => array,
                        Err(_) => serde_json::Value::String(value.clone()),
                    }
                } else if value.starts_with('{') && value.ends_with('}') {
                    match serde_json::from_str::<serde_json::Value>(value) {
                        Ok(obj) => obj,
                        Err(_) => serde_json::Value::String(value.clone()),
                    }
                } else {
                    serde_json::Value::String(value.clone())
                };

                obj.insert(col.clone(), json_value);
            }
        }

        serde_json::Value::Object(obj)
    }

    pub fn from_json(json: &serde_json::Value, schema: &Schema) -> Result<Self, String> {
        if !json.is_object() {
            return Err("Expected JSON object".to_string());
        }

        let obj = json.as_object().unwrap();
        let mut values = vec!["".to_string(); schema.column_count()];

        for (name, value) in obj {
            if let Some(index) = schema.column_index(name) {
                let str_value = match value {
                    serde_json::Value::Null => "NULL".to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Array(_) | serde_json::Value::Object(_) => value.to_string(),
                };

                values[index] = str_value;
            }
        }

        Ok(Row::new(values))
    }

    pub fn validate(&self, schema: &Schema) -> Result<(), String> {
        if self.values.len() != schema.column_count() {
            return Err(format!(
                "Row has {} values but schema expects {}",
                self.values.len(),
                schema.column_count()
            ));
        }
        Ok(())
    }
}
