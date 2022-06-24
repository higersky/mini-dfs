use crate::helper::create_dir_all_exists_ok;
use anyhow::{Context, Result};
use fs2::FileExt;

use path_absolutize::Absolutize;
use rand::prelude::SliceRandom;
use std::collections::HashMap;
use std::io::{Write, Read};
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::time::Duration;
use std::{fs::File, path::PathBuf};
use uuid::Uuid;

#[derive(PartialEq, Eq, Debug)]
#[allow(dead_code)]
pub enum ClientType {
    Client = 0,
    Dataserver = 1,
}

#[derive(Debug)]
pub struct HelloParams {
    pub client_type: ClientType,
    pub address: usize,
}

#[derive(Debug)]
pub struct ReadParams {
    pub path: String,
    pub offset: i64,
    pub blocks: i64,
}

#[derive(Debug)]
pub struct LocateParams {
    pub id: String,
}

#[derive(Debug)]
pub struct WriteParams {
    pub path: String,
    pub amount: i64,
}

#[derive(Debug)]
pub struct SaveParams {
    pub path: String,
    // file_id: String,
    pub block_metadata: BlockMetadata,
    // pub checksum: [u8; 16],
}

#[derive(Debug)]
pub struct ListParams {
    pub path: String,
}

#[derive(Debug)]
pub struct CreateParams {
    pub path: String,
}

#[derive(Debug)]
pub enum NameserverRequest {
    Hello(HelloParams),
    List(ListParams),
    Create(CreateParams),
    Read(ReadParams),
    Locate(LocateParams),
    Write(WriteParams),
    Save(SaveParams),
}

#[derive(Debug)]
pub struct HelloReply {
    pub address: usize,
}

#[derive(Debug)]
pub struct ListReply {
    pub list: Option<Vec<String>>,
}
#[derive(Debug)]
pub struct CreateReply {
    pub status: bool,
}

#[derive(Default, Debug)]
pub struct ReadReply {
    pub valid: bool,
    pub file_id: String,
    pub metadatas: Vec<BlockMetadata>,
    // pub first_offset: i64,
    // pub last_amount: i64,
}

#[derive(Default, Debug)]
pub struct WriteReply {
    pub valid: bool,
    pub addresses: Vec<usize>,
    pub id: String,
}

#[derive(Debug)]
pub struct LocateReply {
    pub path: Option<String>,
}

#[derive(Debug)]
pub struct SaveReply {
    pub status: bool,
}

#[derive(Debug)]
pub enum NameserverResponse {
    Hello(HelloReply),
    List(ListReply),
    Create(CreateReply),
    Read(ReadReply),
    Locate(LocateReply),
    Write(WriteReply),
    Save(SaveReply),
}

#[derive(Debug, Clone)]
pub struct BlockMetadata {
    pub address: usize,
    pub block_id: String,
    pub offset: i64,
    pub amount: i64,
    pub checksum: [u8; 16],
}

// pub type NameserverSender = Sender<(usize, NameserverRequest)>;
// pub type NameserverResponder = Sender<NameserverResponse>;

pub struct Nameserver {
    request_receiver: Receiver<(usize, NameserverRequest)>,
    request_sender: Sender<(usize, NameserverRequest)>,
    response_senders: HashMap<usize, Sender<NameserverResponse>>,
    storage_folder: PathBuf,
    _storage_lock: File,
    file_id_db: rusqlite::Connection, // dataservers: HashMap<u64, DataserverInfo>,
}

const LOCK_FILE: &str = "mini_dfs.lock";
const STORAGE_FOLDER: &str = "metadata";
// const ROOT_PATH: & str = "/";
const FILE_METADATA_FILE: &str = "file_id";
const FILE_ID_DB: &str = "file_id.db";
const BLOCKS_METADATA_FILE: &str = "blocks.db";
const TIMEOUT_SECONDS: u64 = 1;

impl Nameserver {
    pub fn new(storage_path: &Path) -> Result<Nameserver> {
        create_dir_all_exists_ok(storage_path)?;
        create_dir_all_exists_ok(&storage_path.join(STORAGE_FOLDER))?;

        let lock_path = storage_path.join(LOCK_FILE);
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .with_context(|| "Cannot create nameserver folder")?;
        lock_file
            .try_lock_exclusive()
            .with_context(|| format!("Cannot lock {}", lock_path.to_string_lossy()))?;
        let (tx, rx) = channel();
        let file_id_db = rusqlite::Connection::open(storage_path.join(FILE_ID_DB))?;
        file_id_db.execute(
            "create table if not exists file (
            id TEXT PRIMARY KEY unique,
            path TEXT
        )",
            [],
        )?;
        Ok(Nameserver {
            request_receiver: rx,
            request_sender: tx,
            response_senders: Default::default(),
            storage_folder: storage_path.join(STORAGE_FOLDER),
            _storage_lock: lock_file,
            file_id_db,
            // dataservers: Default::default(),
        })
    }

    pub fn add_client_tx(&mut self, address: usize, client_tx: Sender<NameserverResponse>) {
        self.response_senders.insert(address, client_tx);
    }

    pub fn channel(&self) -> Sender<(usize, NameserverRequest)> {
        self.request_sender.clone()
    }

    pub fn run(&mut self, stop_token: &AtomicBool) -> Result<()> {
        let mut rng = rand::thread_rng();
        scopeguard::defer! {
            stop_token.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        loop {
            let msg = self
                .request_receiver
                .recv_timeout(Duration::from_secs(TIMEOUT_SECONDS));
            match msg {
                Ok(msg) => {
                    let (client_tx_id, req) = msg;
                    let client_tx = &self.response_senders[&client_tx_id];
                    match req {
                        NameserverRequest::Hello(params) => {
                            Self::hello(params, client_tx)?;
                        }
                        NameserverRequest::List(params) => {
                            self.list(params, client_tx)?;
                        }
                        NameserverRequest::Create(params) => {
                            self.create(params, client_tx)?;
                        }
                        NameserverRequest::Read(params) => {
                            self.read(params, client_tx)?;
                        }
                        NameserverRequest::Locate(params) => {
                            self.locate(params, client_tx)?;
                        }
                        NameserverRequest::Write(params) => {
                            self.write(params, &mut rng, client_tx)?;
                        }
                        NameserverRequest::Save(params) => {
                            self.save(params, client_tx)?;
                        }
                    }
                }
                Err(_e) => {
                    if stop_token.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    fn locate(&self, params: LocateParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        if let Ok(path) = self.find_file_id(params.id) {
            client_tx.send(NameserverResponse::Locate(LocateReply { path: Some(path) }))?;
        } else {
            client_tx.send(NameserverResponse::Locate(LocateReply { path: None }))?;
        }
        Ok(())
    }

    fn find_file_id(&self, id: String) -> Result<String> {
        let mut stmt = self
            .file_id_db
            .prepare("select path from file where id == ?")?;
        let result = stmt
            .query_map(rusqlite::params![id], |x| x.get::<usize, String>(0))?
            .collect::<Vec<_>>();
        assert!(result.len() <= 1);
        if result.is_empty() {
            Err(anyhow::anyhow!("file not found"))
        } else if let Ok(s) = &result[0] {
            Ok(s.clone())
        } else {
            panic!("file table might corrpted");
        }
    }

    fn save(&self, params: SaveParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        assert!(self
            .validate_path(Path::new(&params.path).join(FILE_METADATA_FILE))
            .is_some());
        let file_folder = self.storage_folder.join(&params.path);
        let conn = rusqlite::Connection::open(file_folder.join(BLOCKS_METADATA_FILE))?;
        conn.execute(
            "create table if not exists blocks (
                                address INTEGER,
                                block_id TEXT,
                                offset INTEGER,
                                amount INTEGER,
                                checksum BLOB
                            )",
            [],
        )?;
        conn.execute(
            "insert into blocks values (?, ?, ?, ?, ?)",
            rusqlite::params![
                params.block_metadata.address,
                params.block_metadata.block_id,
                params.block_metadata.offset,
                params.block_metadata.amount,
                params.block_metadata.checksum
            ],
        )?;
        client_tx
            .send(NameserverResponse::Save(SaveReply { status: true }))
            .with_context(|| "save error")
    }

    fn create(&self, params: CreateParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        let r = if std::fs::create_dir_all(self.storage_folder.join(params.path)).is_ok() {
            client_tx.send(NameserverResponse::Create(CreateReply { status: true }))
        } else {
            client_tx.send(NameserverResponse::Create(CreateReply { status: false }))
        };
        r.with_context(|| "create error")
    }

    fn list(&self, params: ListParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        let read_dir = std::fs::read_dir(self.storage_folder.join(params.path));
        let r = if let Ok(dir) = read_dir {
            let list = dir
                .filter_map(|x| x.ok())
                .map(|x| x.file_name().to_string_lossy().to_string())
                .filter(|x| x != FILE_METADATA_FILE && x != BLOCKS_METADATA_FILE)
                .collect::<Vec<_>>();
            client_tx.send(NameserverResponse::List(ListReply { list: Some(list) }))
        } else {
            client_tx.send(NameserverResponse::List(ListReply { list: None }))
        };
        r.with_context(|| "list error")
    }

    fn write(
        &self,
        params: WriteParams,
        rng: &mut rand::prelude::ThreadRng,
        client_tx: &Sender<NameserverResponse>,
    ) -> Result<()> {
        // dbg!(&params);
        // dbg!(&self.storage_folder);
        let valid = self.validate_path(&params.path).is_some();
        let file_folder = self.storage_folder.join(&params.path);
        let r = if valid {
            let mut addresses = (1..self.response_senders.len()).collect::<Vec<_>>();
            addresses.shuffle(rng);
            if file_folder.exists() && file_folder.is_dir() {
                std::fs::remove_file(file_folder.join(FILE_METADATA_FILE))?;
                std::fs::remove_file(file_folder.join(BLOCKS_METADATA_FILE))?;
                std::fs::remove_dir(&file_folder)?;
            }
            create_dir_all_exists_ok(&file_folder)?;
            let mut f = File::create(file_folder.join(FILE_METADATA_FILE))?;
            let file_id = Self::generate_id();
            self.file_id_db.execute(
                "insert into file values(?, ?)",
                rusqlite::params![file_id, params.path],
            )?;
            f.write_all(file_id.as_bytes())?;
            client_tx.send(NameserverResponse::Write(WriteReply {
                valid,
                addresses,
                id: file_id,
            }))
        } else {
            client_tx.send(NameserverResponse::Write(WriteReply::default()))
        };
        r.with_context(|| "write error")
    }

    fn read(&self, params: ReadParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        let path = self.validate_path(&params.path);
        let r;
        // dbg!(&path);
        if let Some(file_folder) = path {
            if file_folder.is_dir() && !file_folder.join(FILE_METADATA_FILE).exists() {
                r = client_tx.send(NameserverResponse::Read(ReadReply {
                    valid: true,
                    file_id: Default::default(),
                    metadatas: Default::default(),
                }));
            } else if file_folder.is_file() {
                r = client_tx.send(NameserverResponse::Read(ReadReply::default()));
                println!("is unknown file");
            } else {
                let conn = rusqlite::Connection::open(file_folder.join(BLOCKS_METADATA_FILE));
                if let Ok(conn) = conn {
                    let end = params.offset + params.blocks;
                    let mut stmt = conn.prepare(
                        "select address, block_id, offset, amount, checksum from blocks where offset >= ? and offset < ?",
                    )?;
                    let mut rows = stmt
                        .query_map(rusqlite::params![params.offset, end], |row| {
                            Ok(BlockMetadata {
                                address: row.get(0)?,
                                block_id: row.get(1)?,
                                offset: row.get(2)?,
                                amount: row.get(3)?,
                                checksum: row.get(4)?,
                            })
                        })?
                        .filter_map(|s| s.ok())
                        .collect::<Vec<_>>();
                    let file_id = {
                        let f= File::open(file_folder.join(FILE_METADATA_FILE));
                        if let Ok(mut f) = f {
                            let mut buf = String::new();
                            let _  = f.read_to_string(&mut buf);
                            buf
                        } else {
                            Default::default()
                        }
                    };
                    if rows.is_empty() {
                        r = client_tx.send(NameserverResponse::Read(ReadReply::default()));
                        // println!("unknown rows");
                    } else {
                        rows.sort_by_key(|x| x.offset);
                        r = client_tx.send(NameserverResponse::Read(ReadReply {
                            valid: true,
                            file_id,
                            metadatas: rows.clone(),
                        }));
                    }
                } else {
                    r = client_tx.send(NameserverResponse::Read(ReadReply::default()));
                    // println!("connection fail");
                }
            }
        } else {
            r = client_tx.send(NameserverResponse::Read(ReadReply::default()));
        }
        r.with_context(|| "read error")
    }

    fn generate_id() -> String {
        Uuid::new_v4().to_string()
    }

    fn validate_path<P>(&self, path: P) -> Option<PathBuf>
    where
        P: AsRef<Path>,
    {
        // let a = ;
        self.storage_folder
            .join(path)
            .absolutize()
            .map(|x| x.to_path_buf())
            .ok()
            .filter(|x| x.starts_with(&self.storage_folder))
        // dbg!(&a);
        // let b = a;

        // dbg!(&b);
        // b
    }

    fn hello(params: HelloParams, client_tx: &Sender<NameserverResponse>) -> Result<()> {
        let r = if params.client_type == ClientType::Dataserver {
            assert!(params.address != 0);
            client_tx.send(NameserverResponse::Hello(HelloReply {
                address: params.address,
            }))
        } else {
            client_tx.send(NameserverResponse::Hello(HelloReply { address: 0 }))
        };
        r.with_context(|| "hello error")
    }
}
