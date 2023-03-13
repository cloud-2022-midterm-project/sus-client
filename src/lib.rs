#![allow(let_underscore_lock)]

use dotenv::dotenv;
use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

/// This is the main function that is called from the Python client.
#[pyfunction]
fn sync() -> PyResult<()> {
    dotenv().ok();
    let url = std::env::var("BASE_URL").expect("BASE_URL must be set");
    let num_workers = std::env::var("NUM_WORKERS")
        .expect("NUM_WORKERS must be set")
        .parse()
        .expect("NUM_WORKERS must be a number");
    get_all_pagination(url, num_workers);
    Ok(())
}

#[pymodule]
fn app(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PaginationMetadata {
    total_pages: usize,
    kind: PaginationType,
}

#[derive(Serialize, Deserialize, Debug)]
enum PaginationType {
    Cache,
    Fresh,
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

#[derive(Serialize, Deserialize)]
pub struct DbResults {
    pub page_number: usize,
    pub messages: Vec<CompleteMessage>,
}

struct State {
    cache_number: Mutex<usize>,
    pages_fetched: Mutex<usize>,
    get_page_url: String,
    meta: PaginationMetadata,
    cache_number_list: Mutex<VecDeque<usize>>,
    posts_file_names: Mutex<BTreeSet<String>>,
}

const RESULT_CSV_NAME: &str = "results.csv";

fn get_all_pagination(base_url: String, num_workers: usize) {
    let meta = trigger_pagination(&base_url);

    let total_pages = meta.total_pages;
    let state = Arc::new(State {
        cache_number: Mutex::new(0),
        pages_fetched: Mutex::new(0),
        get_page_url: format!("{base_url}/get-page"),
        meta,
        cache_number_list: Mutex::new(VecDeque::with_capacity(total_pages)),
        posts_file_names: Mutex::new(BTreeSet::new()),
    });
    let first_sync = !Path::new(RESULT_CSV_NAME).exists();

    let pool = ThreadPool::new(num_workers);
    for _ in 0..total_pages {
        let s = state.clone();
        pool.execute(move || {
            get_page_and_process(s, first_sync, total_pages);
        });
    }

    // wait for all threads to finish
    pool.join();
}

fn trigger_pagination(url: &str) -> PaginationMetadata {
    let r = reqwest::blocking::get(url).unwrap().bytes().unwrap();
    bincode::deserialize(&r).unwrap()
}

#[derive(Serialize, Debug, Deserialize)]
pub struct CompleteMessage {
    pub uuid: String,
    pub author: String,
    pub message: String,
    pub likes: i32,
    pub image: Option<String>,
}

impl CompleteMessage {
    fn into_csv_row(self) -> String {
        let row = format!(
            "{},{},{},{},{}",
            self.uuid,
            self.author,
            self.message,
            self.likes,
            self.image.unwrap_or("".to_string())
        );
        row
    }
}

#[derive(Serialize, Debug, Deserialize)]
/// The update that the client sees.
pub struct ClientPutUpdate {
    pub author: String,
    pub message: String,
    pub likes: i32,
    pub image: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct PutDeleteUpdate {
    uuid: String,
    put: Option<ClientPutUpdate>,
    delete: bool,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct MutationResults {
    pub posts: Vec<CompleteMessage>,
    pub puts_deletes: Vec<PutDeleteUpdate>,
    pub done: bool,
    pub page_number: usize,
}

fn post_file_name(n: usize) -> String {
    format!("posts_{n}.csv")
}

fn merge_posts(state: &Arc<State>, to: &str) {
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .open(to)
            .unwrap(),
    );
    let mut file_names = state.posts_file_names.lock().unwrap();
    while let Some(file_name) = file_names.pop_first() {
        let mut post = BufReader::new(File::open(file_name).unwrap()).lines();
        while let Some(line) = post.next().map(|l| l.unwrap()) {
            writeln!(writer, "{}", line).unwrap();
        }
    }
}

fn get_page_and_process(state: Arc<State>, first_sync: bool, total_pages: usize) {
    let client = reqwest::blocking::Client::new();
    let body_bytes = client
        .get(&state.get_page_url)
        .send()
        .unwrap()
        .bytes()
        .unwrap();
    drop(client);

    match state.meta.kind {
        PaginationType::Fresh => {
            let res: DbResults = bincode::deserialize(&body_bytes).unwrap();

            let post_file_name = post_file_name(res.page_number);
            state
                .posts_file_names
                .lock()
                .unwrap()
                .insert(post_file_name.clone());
            write_posts_csv(&post_file_name, res.messages);

            let mut pages_fetched = state.pages_fetched.lock().unwrap();
            *pages_fetched += 1;
            println!("{}/{}", *pages_fetched, total_pages);

            if *pages_fetched == total_pages {
                merge_posts(&state, RESULT_CSV_NAME);
            }
        }
        PaginationType::Cache => {
            let res: MutationResults = bincode::deserialize(&body_bytes).unwrap();

            let post_file_name = post_file_name(res.page_number);
            state
                .posts_file_names
                .lock()
                .unwrap()
                .insert(post_file_name.clone());
            write_posts_csv(&post_file_name, res.posts);

            let mut pages_fetched = state.pages_fetched.lock().unwrap();
            *pages_fetched += 1;
            println!("{}/{}", *pages_fetched, total_pages);

            if first_sync && *pages_fetched == total_pages {
                // first time syncing
                // from the flow of the demo, we are certain that there are only `post` cache updates
                merge_posts(&state, RESULT_CSV_NAME);
                return;
            }

            // drop the lock first
            drop(pages_fetched);

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

            if *state.pages_fetched.lock().unwrap() == state.meta.total_pages {
                merge(&state);
            }
        }
    };
}

fn put_delete_file_name(num: usize) -> String {
    let file_name = format!("cached_mutations_{}", num);
    file_name
}

fn write_posts_csv(file_name: &str, posts: Vec<CompleteMessage>) {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)
        .unwrap();
    // append to the file
    for post in posts {
        // each csv row is this format: uuid,message,author,likes,image
        let row = post.into_csv_row();
        // write the row to the file
        writeln!(file, "{}", row).unwrap();
    }
    // flush the file
    file.flush().unwrap();
}

fn merge(state: &Arc<State>) {
    println!("Merging results...");
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
    let mut results_reader =
        BufReader::new(OpenOptions::new().read(true).open(RESULT_CSV_NAME).unwrap()).lines();

    let mut read_result: Option<String> = None;
    let mut read_cached_post: Option<String> = None;
    let mut puts_deletes = VecDeque::new();
    let mut should_update_results = !state.cache_number_list.lock().unwrap().is_empty();

    // check if we we have any cached post updates that we need to merge with the old result lines
    // if not we can just skip the main merge loop entirely
    let mut cached_post_file_names = state.posts_file_names.lock().unwrap();
    let cached_post_file_name = cached_post_file_names.pop_first();
    if let Some(cached_file_name) = cached_post_file_name {
        // we have at least one cached post update
        let mut cached_posts_reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(cached_file_name)
                .unwrap(),
        )
        .lines();

        loop {
            // read a line from the `results.csv` file
            let mut result_line = match read_result.take() {
                Some(l) => l,
                None => match results_reader.next() {
                    Some(l) => l.unwrap(),
                    None => {
                        break;
                    }
                },
            };

            let mark_result_line_for_deletion = update_post_line_with_put_delete(
                &mut should_update_results,
                &mut puts_deletes,
                state,
                &mut result_line,
            );

            let cached_post_line = match read_cached_post.take() {
                Some(l) => l,
                None => match cached_posts_reader.next() {
                    Some(l) => l.unwrap(),
                    None => {
                        // we have reached the end of this current cached post file
                        // load the next post cached file
                        match cached_post_file_names.pop_first() {
                            Some(file_name) => {
                                // we still have more post cached file to load
                                cached_posts_reader = BufReader::new(
                                    OpenOptions::new().read(true).open(file_name).unwrap(),
                                )
                                .lines();
                                // read the first line of the new cached post file
                                match cached_posts_reader.next() {
                                    Some(l) => l.unwrap(),
                                    None => {
                                        // somehow this file doesn't have any lines
                                        break;
                                    }
                                }
                            }
                            None => {
                                // There is no more post cached file to read, so we should write the result line (if not marked for deletion)
                                // and break out of the loop to write the remaining `result_line` lines
                                if !mark_result_line_for_deletion {
                                    writeln!(final_writer, "{}", result_line).unwrap();
                                }
                                break;
                            }
                        }
                    }
                },
            };

            // check to see what should be written to the final file first in this iteration
            if result_line.split(',').next().unwrap() < cached_post_line.split(',').next().unwrap()
            {
                // we should write the result line
                if !mark_result_line_for_deletion {
                    writeln!(final_writer, "{}", result_line).unwrap();
                }
                // save the cached post line for the next iteration
                read_cached_post = Some(cached_post_line);
            } else {
                // we should write the cached post line
                writeln!(final_writer, "{}", cached_post_line).unwrap();
                // save the result line for the next iteration if it is not marked for deletion by the put update
                if !mark_result_line_for_deletion {
                    read_result = Some(result_line);
                }
            }
        }

        // write the remaining cached post lines if there are any
        if let Some(l) = read_cached_post {
            writeln!(final_writer, "{}", l).unwrap();
        }
        for line in cached_posts_reader {
            writeln!(final_writer, "{}", line.unwrap()).unwrap();
        }
    }

    // write the remaining old result lines if there are any
    if let Some(mut result_line) = read_result.take() {
        let mark_result_line_for_deletion = update_post_line_with_put_delete(
            &mut should_update_results,
            &mut puts_deletes,
            state,
            &mut result_line,
        );
        if !mark_result_line_for_deletion {
            writeln!(final_writer, "{}", result_line).unwrap();
        }
    }
    for result_line in results_reader {
        let mut result_line = result_line.unwrap();
        let mark_result_line_for_deletion = update_post_line_with_put_delete(
            &mut should_update_results,
            &mut puts_deletes,
            state,
            &mut result_line,
        );
        if !mark_result_line_for_deletion {
            writeln!(final_writer, "{}", result_line).unwrap();
        }
    }

    // replace `results.csv` with `merged.csv`
    std::fs::rename(merge_file_name, RESULT_CSV_NAME).unwrap();
}

/// Update the passed in `result_line` by mutating it if there is a put update for it.
/// Returns `true` if the `result_line` should be deleted.
fn update_post_line_with_put_delete(
    should_update_results: &mut bool,
    puts_deletes: &mut VecDeque<PutDeleteUpdate>,
    state: &Arc<State>,
    result_line: &mut String,
) -> bool {
    if *should_update_results {
        if puts_deletes.is_empty() {
            // load more put and delete updates
            match state.cache_number_list.lock().unwrap().pop_front() {
                Some(n) => {
                    let file_name = put_delete_file_name(n);
                    let file = std::fs::File::open(file_name).unwrap();
                    let content: Vec<PutDeleteUpdate> = bincode::deserialize_from(file).unwrap();
                    puts_deletes.extend(content);
                }
                None => {
                    // there is no more put or delete update
                    *should_update_results = false;
                }
            }
        }

        // apply update here if there is one for this result line
        if let Some(update) = puts_deletes.pop_front() {
            if update.uuid != result_line.split(',').next().unwrap() {
                // push it back to the front if it is not the update we want
                puts_deletes.push_front(update);
                return false;
            }

            if update.delete {
                // this is a delete update
                return true;
            }

            // this is a put update

            // there has to be a put update here so we can just unwrap
            let put = update.put.unwrap();

            // construct the new line
            let without_image: [String; 5] = [
                update.uuid,
                put.author,
                put.message,
                put.likes.to_string(),
                match put.image {
                    Some(new_image) => new_image,
                    None => result_line.split(',').last().unwrap().to_string(),
                },
            ];
            let updated_result_line = without_image.join(",");

            // replace the old result line with the new updated line
            *result_line = updated_result_line;
        }
    }

    false
}
