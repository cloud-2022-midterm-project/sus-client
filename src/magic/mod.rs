mod enums_structs;
mod state;

use enums_structs::*;
use state::State;
use std::{
    collections::{BTreeSet, VecDeque},
    fs::OpenOptions,
    io::Write,
    path::Path,
    sync::{Arc, Mutex},
};
use threadpool::ThreadPool;

const RESULT_CSV_NAME: &str = "results.csv";
const MERGE_FILE_NAME: &str = "merge.csv";

pub(crate) fn get_all_pagination(base_url: String, num_workers: usize) {
    // trigger the pagination process
    let meta = trigger_pagination(&base_url);
    let total_pages = meta.total_pages;

    // create a state object that will be shared between threads
    let state = Arc::new(State {
        cache_number: Mutex::new(0),
        pages_fetched: Mutex::new(0),
        get_page_url: format!("{base_url}/get-page"),
        meta,
        cache_number_list: Mutex::new(VecDeque::with_capacity(total_pages)),
        posts_file_names: Mutex::new(BTreeSet::new()),
    });

    // a special case flag for the first sync operation
    let first_sync = !Path::new(RESULT_CSV_NAME).exists();

    // create a thread pool
    let pool = ThreadPool::new(num_workers);

    // do the work
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
                state.merge_posts(RESULT_CSV_NAME);
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
                state.merge_posts(RESULT_CSV_NAME);
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
                state.merge();
            }
        }
    };
}

fn post_file_name(n: usize) -> String {
    format!("posts_{n}.csv")
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
