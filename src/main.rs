// Based on the example from https://github.com/dropbox/dropbox-sdk-rust/blob/master/examples/large-file-upload.rs

//! This example illustrates advanced usage of Dropbox's chunked file upload API to upload large
//! files that would not fit in a single HTTP request, including allowing the user to resume
//! interrupted uploads, and uploading blocks in parallel.

use dropbox_sdk::{files, default_client::UserAuthDefaultClient};
use std::{collections::HashMap, fs::File, path::PathBuf, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering::SeqCst}}, time::{SystemTime, Instant}, thread::sleep, time::Duration};
use clap::crate_name;
use jdt;
use serde::{Serialize, Deserialize};
use dirs::{home_dir, config_dir};
use sys_info::hostname;
use anyhow::Result;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Dropbox error: {0}")]
    DropboxError(#[from] dropbox_sdk::Error),
    #[error("Parallel reader error: {0}")]
    ParallelReaderError(parallel_reader::Error<anyhow::Error>),
    #[error("Upload failed: session ID {0}, complete up to {1}")]
    UploadFailed(String, u64),
}

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    backup_paths: Vec<PathBuf>,
    dst_dir: String,
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl Default for Config {
    fn default() -> Self {
        let home_dir = home_dir().expect("Default configuration uses system home directory");
        let config_dir = config_dir().expect("Default configuration uses system config directory");
        let pc_name = hostname().expect("Default configuration uses system hostname");
        let pc_name = pc_name.to_lowercase();
        let pc_name = pc_name.replace(" ", "_");

        Self {
            backup_paths: vec![
                home_dir.join(".bashrc"),
                home_dir.join(".bash_profile"),
                home_dir.join(".profile"),
                home_dir.join(".zshrc"),
                home_dir.join(".zsh_profile"),
                home_dir.join(".vimrc"),
                home_dir.join(".tmux.conf"),
                home_dir.join(".gitconfig"),
                config_dir,
            ],
            dst_dir: format!("/local_backup_{}", pc_name),
            client_id: "".to_string(),
            client_secret: "".to_string(),
            refresh_token: "".to_string(),
        }
    }
}


/// How many blocks to upload in parallel.
const PARALLELISM: usize = 20;

/// The size of a block. This is a Dropbox constant, not adjustable.
const BLOCK_SIZE: usize = 4 * 1024 * 1024;

/// We can upload an integer multiple of BLOCK_SIZE in a single request. This reduces the number of
/// requests needed to do the upload and can help avoid running into rate limits.
const BLOCKS_PER_REQUEST: usize = 2;

/// Keep track of some shared state accessed / updated by various parts of the uploading process.
struct UploadSession {
    session_id: String,
    start_offset: u64,
    file_size: u64,
    bytes_transferred: AtomicU64,
    completion: Mutex<CompletionTracker>,
}

impl UploadSession {
    /// Make a new upload session.
    pub fn new(client: &UserAuthDefaultClient, file_size: u64) -> Result<Self> {
        let session_id = match files::upload_session_start(
            client,
            &files::UploadSessionStartArg::default()
                .with_session_type(files::UploadSessionType::Concurrent),
            &[],
        ) {
            Ok(Ok(result)) => result.session_id,
            Ok(Err(error)) => return Err(error.into()),
            Err(error) => return Err(error.into()),
        };

        Ok(Self {
            session_id,
            start_offset: 0,
            file_size,
            bytes_transferred: AtomicU64::new(0),
            completion: Mutex::new(CompletionTracker::default()),
        })
    }

    /// Generate the argument to append a block at the given offset.
    pub fn append_arg(&self, block_offset: u64) -> files::UploadSessionAppendArg {
        files::UploadSessionAppendArg::new(
            files::UploadSessionCursor::new(
                self.session_id.clone(),
                self.start_offset + block_offset))
    }

    /// Generate the argument to commit the upload at the given path with the given modification
    /// time.
    pub fn commit_arg(&self, dest_path: String, source_mtime: SystemTime)
        -> files::UploadSessionFinishArg
    {
        files::UploadSessionFinishArg::new(
            files::UploadSessionCursor::new(
                self.session_id.clone(),
                self.file_size),
            files::CommitInfo::new(dest_path)
                .with_client_modified(iso8601(source_mtime)))
    }

    /// Mark a block as uploaded.
    pub fn mark_block_uploaded(&self, block_offset: u64, block_len: u64) {
        let mut completion = self.completion.lock().unwrap();
        completion.complete_block(self.start_offset + block_offset, block_len);
    }

    /// Return the offset up to which the file is completely uploaded. It can be resumed from this
    /// position if something goes wrong.
    pub fn complete_up_to(&self) -> u64 {
        let completion = self.completion.lock().unwrap();
        completion.complete_up_to
    }
}

/// Because blocks can be uploaded out of order, if an error is encountered when uploading a given
/// block, that is not necessarily the correct place to resume uploading from next time: there may
/// be gaps before that block.
///
/// This struct is for keeping track of what offset the file has been completely uploaded to.
///
/// When a block is finished uploading, call `complete_block` with the offset and length.
#[derive(Default)]
struct CompletionTracker {
    complete_up_to: u64,
    uploaded_blocks: HashMap<u64, u64>,
}

impl CompletionTracker {

    /// Mark a block as completely uploaded.
    pub fn complete_block(&mut self, block_offset: u64, block_len: u64) {
        if block_offset == self.complete_up_to {
            // Advance the cursor.
            self.complete_up_to += block_len;

            // Also look if we can advance it further still.
            while let Some(len) = self.uploaded_blocks.remove(&self.complete_up_to) {
                self.complete_up_to += len;
            }
        } else {
            // This block isn't at the low-water mark; there's a gap behind it. Save it for later.
            self.uploaded_blocks.insert(block_offset, block_len);
        }
    }
}

fn get_file_mtime_and_size(f: &File) -> Result<(SystemTime, u64)> {
    let meta = f.metadata()?;
    let mtime = meta.modified()?;
    Ok((mtime, meta.len()))
}

/// This function does it all.
fn upload_file(
    client: Arc<UserAuthDefaultClient>,
    mut source_file: File,
    dest_path: String,
) -> Result<()> {

    let (source_mtime, source_len) = get_file_mtime_and_size(&source_file)?;

    let session = Arc::new(UploadSession::new(client.as_ref(), source_len)?);

    eprintln!("upload session ID is {}", session.session_id);

    // Initially set to the end of the file and an empty block; if the file is an exact multiple of
    // BLOCK_SIZE, we'll need to upload an empty buffer when closing the session.
    let last_block = Arc::new(Mutex::new((source_len, vec![])));

    let start_time = Instant::now();
    let upload_result = {
        let client = client.clone();
        let session = session.clone();
        let last_block = last_block.clone();
        parallel_reader::read_stream_and_process_chunks_in_parallel(
            &mut source_file,
            BLOCK_SIZE * BLOCKS_PER_REQUEST,
            PARALLELISM,
            Arc::new(move |block_offset, data: &[u8]| -> Result<()> {
                let append_arg = session.append_arg(block_offset);
                if data.len() != BLOCK_SIZE * BLOCKS_PER_REQUEST {
                    // This must be the last block. Only the last one is allowed to be not 4 MiB
                    // exactly. Save the block and offset so it can be uploaded after all the
                    // parallel uploads are done. This is because once the session is closed, we
                    // can't resume it.
                    let mut last_block = last_block.lock().unwrap();
                    last_block.0 = block_offset + session.start_offset;
                    last_block.1 = data.to_vec();
                    return Ok(());
                }
                let result = upload_block_with_retry(
                    client.as_ref(),
                    &append_arg,
                    data,
                    start_time,
                    session.as_ref(),
                );
                if result.is_ok() {
                    session.mark_block_uploaded(block_offset, data.len() as u64);
                }
                result
            }))
    };

    if let Err(error) = upload_result {
        return Err(Error::ParallelReaderError(error).into());
    }

    let (last_block_offset, last_block_data) = unwrap_arcmutex(last_block);
    eprintln!("closing session at {} with {}-byte block",
        last_block_offset, last_block_data.len());
    let mut arg = session.append_arg(last_block_offset);
    arg.close = true;
    if let Err(e) = upload_block_with_retry(
        client.as_ref(), &arg, &last_block_data, start_time, session.as_ref())
    {
        eprintln!("failed to close session: {}", e);
        // But don't error out; try committing anyway. It could be we're resuming a file where we
        // already closed it out but failed to commit.
    }

    eprintln!("committing...");
    let finish = session.commit_arg(dest_path, source_mtime);

    let mut retry = 0;
    while retry < 3 {
        match files::upload_session_finish(client.as_ref(), &finish, &[]) {
            Ok(Ok(file_metadata)) => {
                println!("Upload succeeded!");
                println!("{:#?}", file_metadata);
                return Ok(());
            }
            error => {
                eprintln!("Error finishing upload: {:?}", error);
                retry += 1;
                sleep(Duration::from_secs(1));
            }
        }
    }

    Err(Error::UploadFailed(session.session_id.clone(), session.complete_up_to()).into())
}

/// Upload a single block, retrying a few times if an error occurs.
///
/// Prints progress and upload speed, and updates the UploadSession if successful.
fn upload_block_with_retry(
    client: &UserAuthDefaultClient,
    arg: &files::UploadSessionAppendArg,
    buf: &[u8],
    start_time: Instant,
    session: &UploadSession,
) -> Result<()> {
    let block_start_time = Instant::now();
    let mut errors = 0;
    loop {
        match files::upload_session_append_v2(client, arg, buf) {
            Ok(Ok(())) => { break; }
            Err(dropbox_sdk::Error::RateLimited { reason, retry_after_seconds }) => {
                eprintln!("rate-limited ({}), waiting {} seconds", reason, retry_after_seconds);
                if retry_after_seconds > 0 {
                    sleep(Duration::from_secs(u64::from(retry_after_seconds)));
                }
            },
            error => {
                errors += 1;
                if errors == 3 {
                    match error {
                        Ok(Err(error)) => return Err(error.into()),
                        Err(error) => return Err(error.into()),
                        _ => unreachable!(),
                    }
                } else {
                    eprintln!("{:?}; retrying...", error);
                }
            },
        }
    }

    let now = Instant::now();
    let block_dur = now.duration_since(block_start_time);
    let overall_dur = now.duration_since(start_time);

    let block_bytes = buf.len() as u64;
    let bytes_sofar = session.bytes_transferred.fetch_add(block_bytes, SeqCst) + block_bytes;

    let percent = bytes_sofar as f64 / session.file_size as f64 * 100.;

    // This assumes that we have `PARALLELISM` uploads going at the same time and at roughly the
    // same upload speed:
    let block_rate = block_bytes as f64 / block_dur.as_secs_f64() * PARALLELISM as f64;

    let overall_rate = bytes_sofar as f64 / overall_dur.as_secs_f64();

    eprintln!("{:.01}%: {}Bytes uploaded, {}Bytes per second, {}Bytes per second average",
        percent,
        human_number(bytes_sofar),
        human_number(block_rate as u64),
        human_number(overall_rate as u64),
        );

    Ok(())
}

fn human_number(n: u64) -> String {
    let mut f = n as f64;
    let prefixes = ['k','M','G','T','E'];
    let mut mag = 0;
    while mag < prefixes.len() {
        if f < 1000. {
            break;
        }
        f /= 1000.;
        mag += 1;
    }
    if mag == 0 {
        format!("{} ", n)
    } else {
        format!("{:.02} {}", f, prefixes[mag - 1])
    }
}

fn iso8601(t: SystemTime) -> String {
    let timestamp: i64 = match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_secs() as i64,
        Err(e) => -(e.duration().as_secs() as i64),
    };

    chrono::DateTime::from_timestamp(timestamp, 0 /* nsecs */)
        .expect("invalid or out-of-range timestamp")
        .format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn unwrap_arcmutex<T: std::fmt::Debug>(x: Arc<Mutex<T>>) -> T {
    Arc::try_unwrap(x)
        .expect("failed to unwrap Arc")
        .into_inner()
        .expect("failed to unwrap Mutex")
}

fn main() -> Result<()> {
    env_logger::init();

    let config = jdt::project(crate_name!()).config::<Config>();
    let dst_dir = config.dst_dir.clone();
    let client_id = config.client_id.clone();
    if client_id.is_empty() {
        eprintln!("Please set the client ID (a.k.a. app key) in the configuration file");
        return Ok(());
    }

    let client_secret = config.client_secret.clone();
    if client_secret.is_empty() {
        eprintln!("Please set the client secret (a.k.a. app secret) in the configuration file");
        return Ok(());
    }

    let refresh_token = config.refresh_token.clone();
    if refresh_token.is_empty() {
        eprintln!("Please set the refresh token in the configuration file");
        return Ok(());
    }

    let auth = dropbox_sdk::oauth2::Authorization::from_client_secret_refresh_token(client_id, client_secret, refresh_token);
    let client = Arc::new(UserAuthDefaultClient::new(auth));

    let mut path_stack = config.backup_paths.clone();
    while let Some(path) = path_stack.pop() {
        if path.exists() {
            if path.is_dir() {
                for entry in path.read_dir()? {
                    let entry = entry.unwrap();
                    let path = entry.path();
                    path_stack.push(path);
                }
            } else {
                let path = path.canonicalize()?;
                let file = File::open(&path)?;
                let dst_path = dst_dir.clone() + path.to_string_lossy().as_ref();
                upload_file(client.clone(), file, dst_path)?;
                eprintln!("uploaded {}", path.to_string_lossy());
            }
        }
    }
    Ok(())
}
