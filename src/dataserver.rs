use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::AtomicBool,
        mpsc::{channel, Receiver, Sender},
    },
    time::Duration,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    helper::create_dir_all_exists_ok,
    nameserver::{
        BlockMetadata, ClientType, HelloParams, HelloReply, NameserverRequest, NameserverResponse,
        SaveParams, SaveReply,
    },
};

pub enum DataserverRequest {
    Read(String),
    Write(Data),
    Replica(Data, String),
}

#[derive(Default, Serialize, Deserialize, Clone, Debug)]
pub struct Metadata {
    pub path: String,
    pub file_id: String,
    pub offset: i64,
    pub amount: i64,
}

#[derive(Default, Clone, Debug)]
pub struct Data {
    pub metadata: Metadata,
    pub blob: Vec<u8>,
}

const BLOB_FILE: &str = "blob";
const METADATA_FILE: &str = "metadata.json";

impl Data {
    pub fn new(path: String, file_id: String, offset: i64, blob: Vec<u8>) -> Data {
        Data {
            metadata: Metadata {
                path,
                file_id,
                offset,
                amount: blob.len() as i64,
            },
            blob,
        }
    }

    pub fn load(block_folder: &Path) -> Result<Data> {
        // dbg!(block_folder);
        let file = File::open(block_folder.join(BLOB_FILE))?;
        let mut reader = BufReader::new(file);
        let mut blob = Vec::new();
        reader.read_to_end(&mut blob)?;
        let file = File::open(block_folder.join(METADATA_FILE))?;
        let reader = BufReader::new(file);
        let metadata: Metadata = serde_json::from_reader(reader)?;
        Ok(Data { metadata, blob })
    }

    pub fn save(&self, block_id: &String, storage_folder: &Path) -> Result<()> {
        let block_folder = storage_folder.join(block_id);
        std::fs::create_dir_all(&block_folder)?;
        let file = File::create(block_folder.join(BLOB_FILE))?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&self.blob)?;
        let file = File::create(block_folder.join(METADATA_FILE))?;
        let metadata = serde_json::to_string(&self.metadata)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(metadata.as_bytes())?;
        Ok(())
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum DataserverResponse {
    Read(bool, Data),
    Write(bool),
    Replica(bool),
}

pub struct Dataserver {
    storage_folder: PathBuf,
    // Send request to nameserver
    nameserver_request_sender: Sender<(usize, NameserverRequest)>,
    // Saved for clone to nameserver
    nameserver_response_sender: Sender<NameserverResponse>,
    // Receive response from nameserver
    nameserver_response_receiver: Receiver<NameserverResponse>,
    // Saved for clone to client
    tx: Sender<(usize, DataserverRequest)>,
    // Receive request from client
    rx: Receiver<(usize, DataserverRequest)>,

    dataserver_request_senders: HashMap<usize, Sender<(usize, DataserverRequest)>>,
    dataserver_response_senders: HashMap<usize, Sender<DataserverResponse>>,
}

const TIMEOUT_SECONDS: u64 = 1;

impl Dataserver {
    pub fn new(
        storage_folder: &Path,
        nameserver_request_sender: Sender<(usize, NameserverRequest)>,
    ) -> Result<Dataserver> {
        create_dir_all_exists_ok(storage_folder)?;

        let (nameserver_response_tx, nameserver_response_rx) = channel();
        let (tx, rx) = channel();

        Ok(Dataserver {
            storage_folder: storage_folder.to_owned(),
            nameserver_request_sender,
            nameserver_response_sender: nameserver_response_tx,
            nameserver_response_receiver: nameserver_response_rx,
            tx,
            rx,
            dataserver_request_senders: Default::default(),
            dataserver_response_senders: Default::default(),
        })
    }

    pub fn nameserver_channel(&self) -> Sender<NameserverResponse> {
        self.nameserver_response_sender.clone()
    }

    pub fn channel(&self) -> Sender<(usize, DataserverRequest)> {
        self.tx.clone()
    }

    pub fn set_senders(
        &mut self,
        dataserver_request_senders: HashMap<usize, Sender<(usize, DataserverRequest)>>,
        dataserver_response_senders: HashMap<usize, Sender<DataserverResponse>>,
    ) {
        self.dataserver_request_senders = dataserver_request_senders;
        self.dataserver_response_senders = dataserver_response_senders;
    }

    pub fn run(&mut self, stop_token: &AtomicBool, address: usize) -> Result<()> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Hello(HelloParams {
                client_type: ClientType::Dataserver,
                address,
            }),
        ))?;
        let r = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Hello(HelloReply { address: id_r }) = r {
            assert!(address == id_r);
        } else {
            panic!("unknown response: {:#?}", r);
        }
        loop {
            let msg = self.rx.recv_timeout(Duration::from_secs(TIMEOUT_SECONDS));
            if let Ok(msg) = msg {
                let (_client_tx_id, req) = msg;
                let client_tx = &self.dataserver_response_senders[&address];
                match req {
                    DataserverRequest::Read(block_id) => {
                        let data = Data::load(&self.storage_folder.join(block_id));
                        if let Ok(data) = data {
                            client_tx.send(DataserverResponse::Read(true, data))?;
                        } else {
                            client_tx.send(DataserverResponse::Read(false, Data::default()))?;
                        }
                    }
                    DataserverRequest::Write(data) => {
                        let block_id = Self::assign_id();
                        self.save_block(&data, &block_id, address)?;
                        for (addr, replica_tx) in self.dataserver_request_senders.iter() {
                            if *addr != address {
                                replica_tx.send((
                                    address,
                                    DataserverRequest::Replica(data.clone(), block_id.clone()),
                                ))?;
                            }
                        }
                        client_tx.send(DataserverResponse::Write(true))?;
                    }
                    DataserverRequest::Replica(data, block_id) => {
                        self.save_block(&data, &block_id, address)?;
                        // dbg!(client_tx_id);
                    }
                }
            }
            if stop_token.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
        }
        Ok(())
    }

    fn save_block(&self, data: &Data, block_id: &String, address: usize) -> Result<()> {
        data.save(block_id, &self.storage_folder)?;
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Save(SaveParams {
                path: data.metadata.path.clone(),
                block_metadata: BlockMetadata {
                    address,
                    block_id: block_id.to_owned(),
                    offset: data.metadata.offset,
                    amount: data.metadata.amount,
                    checksum: md5::compute(&data.blob).0,
                },
            }),
        ))?;

        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Save(SaveReply { status }) = resp {
            if !status {
                panic!("nameserver response failed");
            }
        } else {
            panic!("nameserver response type error : {:#?}", resp);
        }
        Ok(())
    }

    fn assign_id() -> String {
        Uuid::new_v4().to_string()
    }
}
