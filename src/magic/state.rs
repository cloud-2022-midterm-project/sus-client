use super::{
    enums_structs::PaginationMetadata, put_delete_file_name, PutDeleteUpdate, MERGE_FILE_NAME,
    RESULT_CSV_NAME,
};
use std::io::BufRead;
use std::sync::Mutex;
use std::{
    self,
    collections::{BTreeSet, VecDeque},
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Write},
};

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
    pub(crate) cache_number: Mutex<usize>,
    pub(crate) pages_fetched: Mutex<usize>,
    pub(crate) get_page_url: String,
    pub(crate) meta: PaginationMetadata,
    pub(crate) cache_number_list: Mutex<VecDeque<usize>>,
    pub(crate) posts_file_names: Mutex<BTreeSet<String>>,
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
        let mut file_names = self.posts_file_names.lock().unwrap();
        while let Some(file_name) = file_names.pop_first() {
            let mut post = BufReader::new(File::open(file_name).unwrap()).lines();
            while let Some(line) = post.next().map(|l| l.unwrap()) {
                writeln!(writer, "{}", line).unwrap();
            }
        }
    }

    /// Merge between a group of _posts_ cached files and the previous sync results while applying
    /// _put_ and _delete_ updates to the results.
    pub(crate) fn merge(&self) {
        println!("Merging results...");

        // create a new filed called `final.csv` even if it exists
        let mut final_writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .open(MERGE_FILE_NAME)
                .unwrap(),
        );
        // open the `results.csv` file
        let mut results_reader =
            BufReader::new(OpenOptions::new().read(true).open(RESULT_CSV_NAME).unwrap()).lines();

        // a queue of put and delete updates
        // these updates are sorted by uuid already
        let mut puts_deletes = VecDeque::new();

        // a flag to indicate if we should look for a put or delete update for the current result line
        let mut should_update_results = !self.cache_number_list.lock().unwrap().is_empty();

        // check if we we have any cached post updates that we need to merge with the old result lines
        // if not we can just skip the main merge loop entirely
        let mut cached_post_file_names = self.posts_file_names.lock().unwrap();
        let Some(cached_file_name) = cached_post_file_names.pop_first() else {
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
                .open(cached_file_name)
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
                match self.cache_number_list.lock().unwrap().pop_front() {
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
