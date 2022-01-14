use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;

use anyhow::Context;
use cli_table::Style;

const CONFIG_FILE: &str = "config.toml";

fn main() -> anyhow::Result<()> {
    let config = Config::new(CONFIG_FILE).context("Error loading config")?;
    tracing_subscriber::fmt::init();
    process_file(config.input_file)?;
    Ok(())
}

fn process_file<P: AsRef<Path> + Clone>(path: P) -> anyhow::Result<LogStats> {
    // First step is opening the file and creating a reader.
    let file = File::open(path).context("Failed to open file")?;

    // While we are here, we also get the file size and create the instance of `LogStats`.
    let file_len_bytes = file.metadata().expect("Failed to read file metadata").len();
    let mut stats = LogStats::new(file_len_bytes);

    // Options to iterate the lines using the `BufReader`:
    //  - `lines()`: iterates each line allocating a new `String` each time. The string doesn't contain `\n`.
    //  - `read_line()`: allows us to reuse a single `String` instance, acting as a buffer. The string does contain `\n`.
    // Other approaches to potentially improve the performance would be to parallelize a `Vec<String>` with rayon.
    // The obvious problem with this approach is memory consumption as you have to read the whole file and store it in memory.
    // It would be probably better to split the input file in smaller files, processing them concurrently, and accumulate
    // the results as a final step (mapreduce approach).
    let mut reader = BufReader::new(file);
    let mut buffer = String::new();
    loop {
        let num_bytes = reader.read_line(&mut buffer).context("Failed to read line")?;

        // If num_bytes is 0, the current line is empty, so we assume this is the EOF.
        if num_bytes == 0 {
            break;
        }

        // Now we need to process the readline. The first thing we have to do is deserializing the line into a `LogLine` instance.
        // This step doesn't allocate new memory, since `LogLine`'s only holds a reference to the `str` from the `String` buffer.
        if let Ok(log_line) = serde_json::from_str::<LogLine>(&buffer) {
            // If the key exists in the hashmap, we get a mutable reference to its associated value.
            match stats.count_map.get_mut(log_line.object_type) {
                // If the key is in the hashmap, we just increase the counters. No allocations needed.
                Some(object_stats) => {
                    object_stats.count += 1;
                    object_stats.bytes += num_bytes;
                }
                // If the key is not in the hashmap, we add a new entry initializing a new instance of `ObjectStats`.
                // In this case, we need to own the `str` to use it later on, as the values it's pointing at will be erased
                // after the current iteration ends. In other words, we need an to perform an extra `String` allocation
                // everytime we need to add a new key so the hashmap can save the value of the current `type` value and
                // use it outside this iteration to build and output the stats table.
                None => {
                    stats.count_map
                        .insert(log_line.object_type.to_string(), ObjectStats::new(num_bytes));
                }
            }
        } else {
            // The current line couldn't be deserialized into a `LogLine` instance, so we do nothing with it.
        }

        // Clear the buffer to avoid accumulating data.
        buffer.clear();
    }
    stats.print()?;
    Ok(stats)
}

// The json object structure is dynamic and we are only interested in this field.
// Deserializing will work as long as the json object has a `type` field.
#[derive(serde::Deserialize, Debug, Default)]
struct LogLine<'a> {
    #[serde(rename = "type")]
    object_type: &'a str,
}

#[derive(Debug, PartialEq)]
pub struct LogStats {
    pub file_len_bytes: u64,
    pub start: Instant,
    pub count_map: HashMap<String, ObjectStats>,
}

impl LogStats {
    fn new(file_len_bytes: u64) -> Self {
        Self {
            file_len_bytes,
            ..Default::default()
        }
    }
}

impl Default for LogStats {
    fn default() -> Self {
        Self {
            file_len_bytes: 0,
            start: Instant::now(),
            count_map: Default::default(),
        }
    }
}

impl LogStats {
    fn print(&mut self) -> anyhow::Result<()> {
        // Performance stats
        {
            let time_elapsed = self.start.elapsed();
            let file_size_mb = self.file_len_bytes / 1_048_576;
            let throughput = file_size_mb as f64 / time_elapsed.as_secs_f64();
            let lines_processed = self.count_map.iter().map(|x| x.1.count).sum::<usize>();
            let unique_keys = self.count_map.keys().count();
            tracing::info!("[time={time_elapsed:?}][file_size={file_size_mb}MB][throughput={throughput:.2}MB/s][lines={lines_processed:?}][unique_types={unique_keys}]");
        }
        // Table with keys and counts
        {
            use cli_table::{format::Justify, Cell, Table, print_stdout};
            let mut rows = vec![];
            for data in self.count_map.iter() {
                rows.push(vec![
                    data.0.cell().justify(Justify::Right),
                    data.1.count.cell().justify(Justify::Right),
                    data.1.bytes.cell().justify(Justify::Right),
                ]);
            }
            let table = rows
                .table()
                .title(vec!["Type".cell().bold(true), "Count".cell().bold(true), "Size Bytes".cell().bold(true)]);
            print_stdout(table).context("Failed to print stats table")
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ObjectStats {
    pub count: usize,
    pub bytes: usize,
}

impl ObjectStats {
    fn new(bytes: usize) -> Self {
        Self {
            count: 1,
            bytes,
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(default)]
struct Config {
    log_level: String,
    input_file: String,
}

impl Config {
    fn new(path: &str) -> anyhow::Result<Self> {
        use config::Config as CConfig;
        let mut c = CConfig::new();
        let config: Config = {
            if std::path::Path::new(path).exists() {
                c.merge(config::File::with_name(path))?;
                c.try_into()?
            } else {
                Config::default()
            }
        };
        std::env::set_var("RUST_LOG", &config.log_level);
        Ok(config)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            log_level: "info".to_string(),
            input_file: "small.log".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_parsing() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data/small.log");
        let sut = process_file(path).unwrap();
        let expected = {
            let mut count_map = HashMap::new();
            count_map.insert("A".to_string(), ObjectStats { count: 3, bytes: 76 });
            count_map.insert("B".to_string(), ObjectStats { count: 4, bytes: 169 });
            LogStats { file_len_bytes: 0, start: Instant::now(), count_map }
        };
        assert_eq!(expected.count_map, sut.count_map);
    }
}
