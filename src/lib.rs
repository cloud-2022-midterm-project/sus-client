#![allow(let_underscore_lock)]

use std::{
    collections::VecDeque,
    fs::OpenOptions,
    io::{BufRead, BufReader, BufWriter},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
};

use dotenv::dotenv;
use pyo3::prelude::*;

use serde::{ser::Error, Deserialize, Deserializer, Serialize, Serializer};
use std::io::Write;
use threadpool::ThreadPool;

#[derive(Serialize, Deserialize)]
pub struct PaginationMetadata {
    total_pages: usize,
    kind: PaginationType,
}

#[derive(Serialize, Deserialize)]
enum PaginationType {
    Fresh,
    Cache,
}

impl FromStr for PaginationType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "fresh" => Ok(PaginationType::Fresh),
            "cached" => Ok(PaginationType::Cache),
            _ => Err("Unknown pagination kind"),
        }
    }
}

struct State {
    cache_number: Mutex<usize>,
    pages_fetched: Mutex<usize>,
    get_page_url: String,
    meta: PaginationMetadata,
    cache_number_list: Mutex<VecDeque<usize>>,
    writing_fresh: Mutex<()>,
    result_csv_name: &'static str,
    cached_posts_csv_name: &'static str,
}

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sync() -> PyResult<()> {
    dotenv().ok();
    let url = std::env::var("BASE_URL").expect("BASE_URL must be set");
    get_all_pagination(url);
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn app(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    Ok(())
}

fn get_all_pagination(base_url: String) {
    let meta = trigger_pagination(&base_url);

    let worker_threads_count = 8;
    let pool = ThreadPool::new(worker_threads_count);
    let total_pages = meta.total_pages;
    let state = Arc::new(State {
        cache_number: Mutex::new(0),
        pages_fetched: Mutex::new(0),
        get_page_url: format!("{base_url}/get-page"),
        meta,
        cache_number_list: Mutex::new(VecDeque::with_capacity(total_pages)),
        writing_fresh: Mutex::new(()),
        result_csv_name: "results.csv",
        cached_posts_csv_name: "cached_posts.csv",
    });

    for _ in 0..total_pages {
        let s = state.clone();
        pool.execute(move || {
            get_page_and_process(s);
        });
    }

    // wait for all threads to finish
    pool.join();
}

fn trigger_pagination(url: &str) -> PaginationMetadata {
    let r = reqwest::blocking::get(url).unwrap();
    r.json().unwrap()
}

#[derive(Serialize, Debug, Deserialize)]
pub struct CompleteMessage {
    pub uuid: String,
    pub author: String,
    pub message: String,
    pub likes: i32,
    pub image: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
/// The update that the client sees.
pub struct ClientPutUpdate {
    pub uuid: String,
    pub author: String,
    pub message: String,
    pub likes: i32,
    pub image: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct PutDeleteUpdate {
    put: Option<ClientPutUpdate>,
    delete: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct MutationResults {
    pub posts: Vec<CompleteMessage>,
    pub puts_deletes: Vec<PutDeleteUpdate>,
    pub done: bool,
}

fn get_page_and_process(state: Arc<State>) {
    let client = reqwest::blocking::Client::new();
    let res = client.get(&state.get_page_url).send().unwrap();
    drop(client);

    match state.meta.kind {
        PaginationType::Fresh => {
            let response_json: Vec<CompleteMessage> = res.json().unwrap();
            write_posts_csv(state.result_csv_name, response_json, &state);
        }
        PaginationType::Cache => {
            let res: MutationResults = res.json().unwrap();

            // create a new file called `cached_posts.csv` if it doesn't exist
            // check if the `results.csv` file exists
            let first_sync = !Path::new(state.result_csv_name).exists();
            let file_name = if first_sync {
                // first time syncing
                state.result_csv_name
            } else {
                state.cached_posts_csv_name
            };
            write_posts_csv(file_name, res.posts, &state);

            if first_sync {
                // rename the file to `results.csv`
                std::fs::rename(file_name, state.result_csv_name).unwrap();
                return;
            }

            if !res.puts_deletes.is_empty() {
                let cache_num;
                {
                    let mut cache_number = state.cache_number.lock().unwrap();
                    *cache_number += 1;
                    cache_num = *cache_number;
                }
                state.cache_number_list.lock().unwrap().push_back(cache_num);

                // create a new file called `cached_mutations_{}.csv`
                let file_name = put_delete_file_name(cache_num);
                // dump puts deletes to the file
                let encoded = bincode::serialize(&res.puts_deletes).unwrap();
                std::fs::write(file_name, encoded).unwrap();
            }

            let mut pages_fetched = state.pages_fetched.lock().unwrap();
            *pages_fetched += 1;
            if *pages_fetched == state.meta.total_pages {
                merge(&state);
            }
        }
    };
}

fn put_delete_file_name(num: usize) -> String {
    let file_name = format!("cached_mutations_{}", num);
    file_name
}

fn write_posts_csv(file_name: &str, posts: Vec<CompleteMessage>, state: &Arc<State>) {
    let _ = state.writing_fresh.lock().unwrap();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    // append to the file
    for post in posts {
        // each csv row is this format: uuid,message,author,likes,image
        let row = get_csv_row(post);
        // write the row to the file
        writeln!(file, "{}", row).unwrap();
    }
    // flush the file
    file.flush().unwrap();
}

fn get_csv_row(post: CompleteMessage) -> String {
    let row = format!(
        "{},{},{},{},{}",
        post.uuid,
        post.author,
        post.message,
        post.likes,
        post.image.unwrap_or("".to_string())
    );
    row
}

fn merge(state: &Arc<State>) {
    let merge_file_name = "merge.csv";

    // create a new filed called `final.csv` even if it exists
    let mut final_writer = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .write(true)
            .open(merge_file_name)
            .unwrap(),
    );
    // open the `results.csv` file
    let mut results_reader = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(state.result_csv_name)
            .unwrap(),
    )
    .lines();
    // open the `cached_posts.csv` file
    let mut cached_posts_reader = BufReader::new(
        OpenOptions::new()
            .read(true)
            .open(state.cached_posts_csv_name)
            .unwrap(),
    )
    .lines();

    let mut read_result: Option<String> = None;
    let mut read_cached_post: Option<String> = None;
    let mut puts_deletes = VecDeque::new();
    let mut should_update_results = !state.cache_number_list.lock().unwrap().is_empty();

    loop {
        // read a line from the `results.csv` file
        let result_line = match read_result.take() {
            Some(l) => Some(l),
            None => {
                let l = results_reader.next();
                l.map(|l| l.unwrap())
            }
        };
        let Some(mut result_line) = result_line else {
            break;
        };

        let mut mark_result_line_for_deletion = false;

        if should_update_results {
            if puts_deletes.is_empty() {
                // load puts and deletes file
                match state.cache_number_list.lock().unwrap().pop_front() {
                    Some(n) => {
                        let file_name = put_delete_file_name(n);
                        let file = std::fs::File::open(file_name).unwrap();
                        let content: Vec<PutDeleteUpdate> =
                            bincode::deserialize_from(file).unwrap();
                        puts_deletes.extend(content);
                    }
                    None => {
                        should_update_results = false;
                    }
                }
            }
            // apply put update here
            if let Some(update) = puts_deletes.pop_front() {
                let mut used_update = false;
                if let Some(put) = &update.put {
                    if put.uuid == result_line.split(',').next().unwrap() {
                        let mut line_splits: Vec<String> =
                            result_line.split(',').map(|s| s.to_string()).collect();
                        // TODO: refactor this, it's a lot of copies
                        line_splits[1] = put.author.to_string();
                        line_splits[2] = put.message.to_string();
                        let likes_binding = put.likes.to_string();
                        line_splits[3] = likes_binding;
                        if let Some(image) = &put.image {
                            line_splits[4] = image.to_string();
                        }
                        // rejoin the line
                        result_line = line_splits.join(",");
                        used_update = true;
                    }
                }
                if let Some(delete) = &update.delete {
                    if delete == result_line.split(',').next().unwrap() {
                        mark_result_line_for_deletion = true;
                        used_update = true;
                    }
                }
                if !used_update {
                    puts_deletes.push_front(update);
                }
            }
        }

        let cached_post_line = match read_cached_post.take() {
            Some(l) => Some(l),
            None => {
                let l = cached_posts_reader.next();
                l.map(|l| l.unwrap())
            }
        };
        let Some(cached_post_line) = cached_post_line else {
            writeln!(final_writer, "{}", result_line).unwrap();
            break;
        };

        // check to see what should be written to the final file first
        if result_line.split(',').next().unwrap() < cached_post_line.split(',').next().unwrap() {
            // should write the result line
            read_cached_post = Some(cached_post_line);
            if mark_result_line_for_deletion {
                continue;
            }
            writeln!(final_writer, "{}", result_line).unwrap();
        } else {
            // should write the cached post line
            writeln!(final_writer, "{}", cached_post_line).unwrap();
            if mark_result_line_for_deletion {
                continue;
            }
            read_result = Some(result_line);
        }
    }

    // write the remaining lines
    if let Some(l) = read_result {
        writeln!(final_writer, "{}", l).unwrap();
    }
    if let Some(l) = read_cached_post {
        writeln!(final_writer, "{}", l).unwrap();
    }
    for line in cached_posts_reader {
        writeln!(final_writer, "{}", line.unwrap()).unwrap();
    }
    for line in results_reader {
        writeln!(final_writer, "{}", line.unwrap()).unwrap();
    }

    // replace `results.csv` with `merged.csv`
    std::fs::rename(merge_file_name, state.result_csv_name).unwrap();
}

/// serde Value that can be Absent, Null, or Value(T)
#[derive(Debug, Default)]
pub enum Maybe<T> {
    #[default]
    Absent,
    Null,
    Value(T),
}

#[allow(dead_code)]
impl<T> Maybe<T> {
    pub fn is_absent(&self) -> bool {
        matches!(self, Maybe::Absent)
    }

    pub fn as_ref(&self) -> Maybe<&T> {
        match self {
            Maybe::Absent => Maybe::Absent,
            Maybe::Null => Maybe::Null,
            Maybe::Value(v) => Maybe::Value(v),
        }
    }
}

impl<T> From<Option<T>> for Maybe<T> {
    fn from(opt: Option<T>) -> Maybe<T> {
        match opt {
            Some(v) => Maybe::Value(v),
            None => Maybe::Null,
        }
    }
}

impl<'de, T> Deserialize<'de> for Maybe<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let d = Option::deserialize(deserializer).map(Into::into);
        d
    }
}

impl<T: Serialize> Serialize for Maybe<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            // this will be serialized as null
            Maybe::Null => serializer.serialize_none(),
            Maybe::Value(v) => v.serialize(serializer),
            // should have been skipped
            Maybe::Absent => Err(Error::custom(
                r#"Maybe fields need to be annotated with: #[serde(default, skip_serializing_if = "Maybe::is_Absent")]"#,
            )),
        }
    }
}
