use cmd_lib::*;
use rayon::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;
use uuid::Uuid;

// Support GenUuids only for now
pub fn gen_uuids(num: usize, file: &str) -> CmdResult {
    info!("Generating {num} uuids into file {file}");
    let num_threads = num_cpus::get();
    let num_uuids = num / num_threads;
    let last_num_uuids = num - num_uuids * (num_threads - 1);

    let uuids = Arc::new(Mutex::new(Vec::new()));
    (0..num_threads).into_par_iter().for_each(|i| {
        let mut uuids_str = String::new();
        let n = if i == num_threads - 1 {
            last_num_uuids
        } else {
            num_uuids
        };
        for _ in 0..n {
            uuids_str += &Uuid::now_v7().to_string();
            uuids_str += "\n";
        }
        uuids.lock().unwrap().push(uuids_str);
    });

    let dir = run_fun!(dirname $file)?;
    run_cmd! {
        mkdir -p $dir;
        echo -n > $file;
    }?;
    for uuid in uuids.lock().unwrap().iter() {
        run_cmd!(echo -n $uuid >> $file)?;
    }
    info!("File {file} is ready");
    Ok(())
}
