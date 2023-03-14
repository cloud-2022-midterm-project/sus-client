#![allow(let_underscore_lock)]

use dotenv::dotenv;
use pyo3::prelude::*;

mod magic;

/// This is the main function that is called from the Python client.
#[pyfunction]
fn sync() -> PyResult<()> {
    // load the environment variables from the .env file
    dotenv().ok();
    let url = std::env::var("BASE_URL").expect("BASE_URL must be set");
    let num_workers = std::env::var("NUM_WORKERS")
        .expect("NUM_WORKERS must be set")
        .parse()
        .expect("NUM_WORKERS must be a number");

    // call the rust function that does all the work
    magic::get_all_pagination(url, num_workers);

    // return back to the Python client
    Ok(())
}

#[pymodule]
fn app(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sync, m)?)?;
    Ok(())
}
