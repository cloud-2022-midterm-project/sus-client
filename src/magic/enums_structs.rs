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
