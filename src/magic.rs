use std::{
    collections::{BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
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
        pages_fetched: Mutex::new(0),
        get_page_url: format!("{base_url}/get-page"),
        meta,
        puts_deletes_file_numbers: Mutex::new(BTreeSet::new()),
        posts_file_numbers: Mutex::new(BTreeSet::new()),
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

            state
                .posts_file_numbers
                .lock()
                .unwrap()
                .insert(res.page_number);

            let post_file_name = post_file_name(res.page_number);
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

            state
                .posts_file_numbers
                .lock()
                .unwrap()
                .insert(res.page_number);

            let post_file_name = post_file_name(res.page_number);
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
                state
                    .puts_deletes_file_numbers
                    .lock()
                    .unwrap()
                    .insert(res.page_number);

                // create a new file for put/delete mutations of this page
                let file_name = put_delete_file_name(res.page_number);
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

use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Debug, Deserialize)]
/// The update that the client sees.
pub struct ClientPutUpdate {
    pub(crate) author: String,
    pub(crate) message: String,
    pub(crate) likes: i32,
    pub(crate) image: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
pub struct PutDeleteUpdate {
    pub uuid: String,
    pub put: Option<ClientPutUpdate>,
    pub delete: bool,
}

#[derive(Serialize, Debug, Deserialize)]
pub(crate) struct MutationResults {
    pub(crate) posts: Vec<CompleteMessage>,
    pub(crate) puts_deletes: Vec<PutDeleteUpdate>,
    pub(crate) done: bool,
    pub(crate) page_number: usize,
}

#[derive(Serialize, Debug, Deserialize)]
pub(crate) struct CompleteMessage {
    pub(crate) uuid: String,
    pub(crate) author: String,
    pub(crate) message: String,
    pub(crate) likes: i32,
    pub(crate) image: Option<String>,
}

impl CompleteMessage {
    pub(crate) fn into_csv_row(self) -> String {
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

#[derive(Serialize, Deserialize, Debug)]
pub struct PaginationMetadata {
    pub(crate) total_pages: usize,
    pub(crate) kind: PaginationType,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum PaginationType {
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
pub(crate) struct DbResults {
    pub(crate) page_number: usize,
    pub(crate) messages: Vec<CompleteMessage>,
}

pub struct ReadResultLine {
    /// the read result line
    line: String,
    /// A flag to indicate that this result line was updated by a put or delete update.
    /// This is used to skip updating the result line again
    updated: bool,
    mark_for_deletion: bool,
}

impl ReadResultLine {
    fn new(line: String) -> Self {
        Self {
            line,
            updated: false,
            mark_for_deletion: false,
        }
    }
}

pub(crate) struct State {
    pub(crate) pages_fetched: Mutex<usize>,
    pub(crate) get_page_url: String,
    pub(crate) meta: PaginationMetadata,
    pub(crate) puts_deletes_file_numbers: Mutex<BTreeSet<usize>>,
    pub(crate) posts_file_numbers: Mutex<BTreeSet<usize>>,
}

impl State {
    /// merge all files, saved by each thread, that contain complete messages into a single file called `to`
    pub(crate) fn merge_posts(&self, to: &str) {
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .open(to)
                .unwrap(),
        );
        let mut file_names = self.posts_file_numbers.lock().unwrap();
        while let Some(file_num) = file_names.pop_first() {
            let mut post = BufReader::new(File::open(post_file_name(file_num)).unwrap()).lines();
            while let Some(line) = post.next().map(|l| l.unwrap()) {
                writeln!(writer, "{}", line).unwrap();
            }
        }
    }

    /// Merge between a group of _posts_ cached files and the previous sync results while applying
    /// _put_ and _delete_ updates to the results.
    pub(crate) fn merge(&self) {
        println!("Merging results...");

        // create a new file for the merge result
        let mut final_writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(MERGE_FILE_NAME)
                .unwrap(),
        );

        // open the previous sync results file
        let mut results_reader =
            BufReader::new(OpenOptions::new().read(true).open(RESULT_CSV_NAME).unwrap()).lines();

        // a queue of put and delete updates
        // these updates are sorted by uuid already
        let mut puts_deletes = VecDeque::new();

        // a flag to indicate if we should look for a put or delete update for the current result line
        let mut should_update_results = !self.puts_deletes_file_numbers.lock().unwrap().is_empty();

        // check if we we have any cached post updates that we need to merge with the old result lines
        // if not we can just skip the main merge loop entirely
        let mut post_file_numbers = self.posts_file_numbers.lock().unwrap();
        let Some(post_file_num) = post_file_numbers.pop_first() else {
            // we don't have any cached post updates so we can just write the remaining old result lines
            // while applying any put or delete updates
            for mut result_line in results_reader.map(Result::unwrap).map(ReadResultLine::new) {
                // apply a put or delete update if there is one for this result line
                self.update_post_line_with_put_delete(
                    &mut should_update_results,
                    &mut puts_deletes,
                    &mut result_line,
                );

                // if the result line is not marked for deletion, write it to the final results file
                if !result_line.mark_for_deletion {
                    writeln!(final_writer, "{}", result_line.line).unwrap();
                }
            }

            // rename the merge file to the final results file
            std::fs::rename(MERGE_FILE_NAME, RESULT_CSV_NAME).unwrap();

            return;
        };

        // we have at least one cached post file to merge with the old results
        let mut cached_posts_reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(post_file_name(post_file_num))
                .unwrap(),
        )
        .lines();

        // prepare 2 buffers for already read lines that they are not selected to
        // be written to the final results file in the iteration of loop
        let mut read_cached_post: Option<String> = None;
        let mut read_result: Option<ReadResultLine> = None;

        loop {
            // get a line from the buffer first
            let mut read_result_line = match read_result.take() {
                Some(l) => l,
                // if the buffer is empty, read a line from the previous sync results file
                None => match results_reader.next() {
                    // Some(l) => l.unwrap(),
                    Some(l) => ReadResultLine::new(l.unwrap()),
                    None => {
                        // we have reached the end of the previous sync results file
                        // go write the remaining cached post lines outside the loop
                        break;
                    }
                },
            };

            // If this result line has not been marked as updated, try update it if uuid matches.
            if !read_result_line.updated {
                // apply a put or delete update to the current result line if needed
                self.update_post_line_with_put_delete(
                    &mut should_update_results,
                    &mut puts_deletes,
                    &mut read_result_line,
                );
            }

            // read a line from the current cached post file
            // if there is a break here, don't forget to write the current result line
            // to the final results file or else it will be lost
            let cached_post_line = match read_cached_post.take() {
                Some(l) => l,
                None => match cached_posts_reader.next() {
                    Some(l) => l.unwrap(),
                    None => {
                        // we have reached the end of this current cached post file
                        // load the next post cached file if there is more
                        match post_file_numbers.pop_first() {
                            Some(post_file_num) => {
                                // we still have more post cached file to load
                                cached_posts_reader = BufReader::new(
                                    OpenOptions::new()
                                        .read(true)
                                        .open(post_file_name(post_file_num))
                                        .unwrap(),
                                )
                                .lines();
                                // read the first line of the new cached post file
                                match cached_posts_reader.next() {
                                    Some(l) => l.unwrap(),
                                    None => {
                                        // somehow this file doesn't have any lines???
                                        if !read_result_line.mark_for_deletion {
                                            writeln!(final_writer, "{}", read_result_line.line)
                                                .unwrap();
                                        }
                                        break;
                                    }
                                }
                            }
                            None => {
                                // There is no more post cached file to read, so we should write the result line (if not marked for deletion)
                                // and break out of the loop to write the remaining `result_line` lines
                                if !read_result_line.mark_for_deletion {
                                    writeln!(final_writer, "{}", read_result_line.line).unwrap();
                                }
                                break;
                            }
                        }
                    }
                },
            };

            // check to see what should be written to the final file in this iteration
            if read_result_line.line.split(',').next().unwrap()
                < cached_post_line.split(',').next().unwrap()
            {
                // we should write the result line
                if !read_result_line.mark_for_deletion {
                    writeln!(final_writer, "{}", read_result_line.line).unwrap();
                }
                // save the cached post line for the next iteration
                read_cached_post = Some(cached_post_line);
            } else {
                // we should write the cached post line
                writeln!(final_writer, "{}", cached_post_line).unwrap();
                // save the result line for the next iteration if it is not marked for deletion by the put update
                if !read_result_line.mark_for_deletion {
                    read_result = Some(read_result_line);
                }
            }
        } // end of loop

        // write the remaining cached post lines if there are any
        if let Some(l) = read_cached_post.take() {
            writeln!(final_writer, "{}", l).unwrap();
        }
        for line in cached_posts_reader {
            writeln!(final_writer, "{}", line.unwrap()).unwrap();
        }

        // write the remaining old result lines if there are any
        if let Some(mut result_line) = read_result.take() {
            self.update_post_line_with_put_delete(
                &mut should_update_results,
                &mut puts_deletes,
                &mut result_line,
            );
            if !result_line.mark_for_deletion {
                writeln!(final_writer, "{}", result_line.line).unwrap();
            }
        }
        for mut result_line in results_reader.map(Result::unwrap).map(ReadResultLine::new) {
            self.update_post_line_with_put_delete(
                &mut should_update_results,
                &mut puts_deletes,
                &mut result_line,
            );
            if !result_line.mark_for_deletion {
                writeln!(final_writer, "{}", result_line.line).unwrap();
            }
        }

        // rename the merge file to the final results file
        std::fs::rename(MERGE_FILE_NAME, RESULT_CSV_NAME).unwrap();
    }

    /// Update if there is a put update for it.
    pub(crate) fn update_post_line_with_put_delete(
        &self,
        should_update_results: &mut bool,
        puts_deletes: &mut VecDeque<PutDeleteUpdate>,
        result_line: &mut ReadResultLine,
    ) {
        if *should_update_results {
            if puts_deletes.is_empty() {
                // load more put and delete updates
                match self.puts_deletes_file_numbers.lock().unwrap().pop_first() {
                    Some(n) => {
                        let file_name = put_delete_file_name(n);
                        let file = std::fs::File::open(file_name).unwrap();
                        let content: Vec<PutDeleteUpdate> =
                            bincode::deserialize_from(file).unwrap();
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
                if update.uuid != result_line.line.split(',').next().unwrap() {
                    // push it back to the front if it is not the update we want
                    puts_deletes.push_front(update);
                    return;
                }

                // we have found the update for this read result line
                result_line.updated = true;

                if update.delete {
                    // this is a delete update
                    result_line.mark_for_deletion = true;
                    return;
                }

                // this is a put update

                // there has to be a put update here so we can just unwrap
                let put = update.put.unwrap();

                // construct the parts of the new line
                let parts: [String; 5] = [
                    update.uuid,
                    put.author,
                    put.message,
                    put.likes.to_string(),
                    match put.image {
                        Some(new_image) => new_image,
                        // there is no update for image, so we should just use the old image
                        None => result_line.line.split(',').last().unwrap().to_string(),
                    },
                ];

                // replace the old result line with the new updated line
                result_line.line = parts.join(",");
            }
        }
    }
}
