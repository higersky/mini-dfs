use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::Path,
    sync::{
        atomic::AtomicBool,
        mpsc::{Receiver, Sender},
    },
};

use crate::{
    dataserver::{Data, DataserverRequest, DataserverResponse},
    nameserver::{
        BlockMetadata, CreateParams, CreateReply, ListParams, ListReply, LocateParams, LocateReply,
        NameserverRequest, NameserverResponse, ReadParams, ReadReply, WriteParams, WriteReply,
    },
};

use anyhow::Result;
use itertools::Itertools;
use rustyline::{error::ReadlineError, Editor};
use tabled::Tabled;
use thiserror::private::PathAsDisplay;

#[derive(Debug)]
pub struct Client {
    nameserver_request_sender: Sender<(usize, NameserverRequest)>,
    dataserver_request_senders: HashMap<usize, Sender<(usize, DataserverRequest)>>,
    nameserver_response_receiver: Receiver<NameserverResponse>,
    dataserver_response_receivers: HashMap<usize, Receiver<DataserverResponse>>,
}

#[derive(Tabled)]
struct DisplayMetadata {
    pub address: usize,
    pub block_id: String,
    pub offset: i64,
    pub amount: i64,
    pub checksum: String,
}

impl From<&BlockMetadata> for DisplayMetadata {
    fn from(x: &BlockMetadata) -> Self {
        DisplayMetadata {
            address: x.address,
            block_id: x.block_id.clone(),
            offset: x.offset,
            amount: x.amount,
            checksum: hex::encode(x.checksum),
        }
    }
}

const BLOCK_SIZE: usize = 2 * 1024 * 1024;

impl Client {
    pub fn new(
        nameserver_request_sender: Sender<(usize, NameserverRequest)>,
        nameserver_response_receiver: Receiver<NameserverResponse>,
        dataserver_request_senders: HashMap<usize, Sender<(usize, DataserverRequest)>>,
        dataserver_response_receivers: HashMap<usize, Receiver<DataserverResponse>>,
    ) -> Client {
        Client {
            nameserver_request_sender,
            nameserver_response_receiver,
            dataserver_request_senders,
            dataserver_response_receivers,
        }
    }

    pub fn run(&mut self, stop_token: &AtomicBool, cwd: &Path, address: usize) -> Result<()> {
        let mut rl = Editor::<()>::new();
        let _ = rl.load_history(&cwd.join("history.txt"));
        println!("Welcome to MiniDFS, type 'help' to show avilable commands");
        loop {
            if stop_token.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            let readline = rl.readline("MiniDFS >> ");
            match readline {
                Ok(line) => {
                    if line.trim().is_empty() {
                        continue;
                    }
                    rl.add_history_entry(line.as_str());
                    let commands = shellwords::split(&line);
                    let r;
                    match commands {
                        Ok(commands) => match commands[0].as_str() {
                            "help" => {
                                println!(
                                    "Available commands:\n\tlist [directory]\n\tcreate <directory>\n\tfile <path>\n\tlocate <id>\n\twrite <local path> <path>\n\tread <path> <local path> [offset]"
                                );
                                r = Ok(());
                            }
                            "exit" => {
                                break;
                            }
                            "list" => {
                                r = if commands.len() == 1 {
                                    self.list("./".into(), address)
                                } else if commands.len() == 2 {
                                    self.list(format!("./{}", commands[1]), address)
                                } else {
                                    Err(anyhow::anyhow!(
                                        "list: too many parameters\n syntax: list [directory]"
                                    ))
                                };
                            }
                            "create" => {
                                r = if commands.len() == 2 {
                                    self.create(format!("./{}", commands[1]), address)
                                } else {
                                    Err(anyhow::anyhow!("syntax: create <directory>"))
                                };
                            }
                            "file" => {
                                r = if commands.len() == 2 {
                                    self.file(format!("./{}", commands[1]), address)
                                } else {
                                    Err(anyhow::anyhow!("syntax: file <path>"))
                                };
                            }
                            "locate" => {
                                r = if commands.len() == 2 {
                                    self.locate(&commands[1], address)
                                } else {
                                    Err(anyhow::anyhow!("syntax: locate <id>"))
                                };
                            }
                            "write" => {
                                r = if commands.len() == 3 {
                                    let local_path = commands[1].clone();
                                    let local_path = Path::new(&local_path);
                                    if !local_path.exists() {
                                        Err(anyhow::anyhow!(
                                            "error: file not exists on local path {}",
                                            local_path.as_display()
                                        ))
                                    } else {
                                        let f = File::open(local_path);
                                        if let Ok(f) = f {
                                            let mut reader = BufReader::new(f);
                                            let mut buf = Vec::new();
                                            reader.read_to_end(&mut buf)?;
                                            self.write(format!("./{}", commands[2]), &buf, address)
                                        } else {
                                            Err(anyhow::anyhow!(
                                                "error: failed to create file on path {}",
                                                commands[2]
                                            ))
                                        }
                                    }
                                } else {
                                    Err(anyhow::anyhow!("syntax: write <local path> <path>"))
                                }
                            }
                            "read" => {
                                r = if commands.len() == 3 {
                                    self.download(
                                        format!("./{}", commands[1]),
                                        &commands[2],
                                        0,
                                        i64::max_value(),
                                        address,
                                    )
                                } else if commands.len() == 4 {
                                    let offset = commands[3].parse();
                                    if let Ok(offset) = offset {
                                        self.download(
                                            format!("./{}", commands[1]),
                                            &commands[2],
                                            offset,
                                            1,
                                            address,
                                        )
                                    } else {
                                        Err(anyhow::anyhow!(
                                            "syntax: read <path> <local path> [offset]"
                                        ))
                                    }
                                } else {
                                    Err(anyhow::anyhow!(
                                        "syntax: read <path> <local path> [offset]"
                                    ))
                                }
                            }
                            _ => {
                                println!("error: unrecognized command {}", commands[0]);
                                r = Ok(());
                            }
                        },
                        Err(_e) => {
                            println!("syntax error");
                            r = Ok(());
                        }
                    }
                    if let Err(e) = r {
                        println!("{}", e);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("Interrupted, exiting...");
                    break;
                }
                Err(ReadlineError::Eof) => {
                    println!("EOF detected, exiting...");
                    break;
                }
                Err(err) => {
                    println!("Error: {:?}", err);
                }
            }
        }
        stop_token.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = rl.save_history(&cwd.join("history.txt"));
        Ok(())
    }

    fn download(
        &self,
        path: String,
        local_path: &String,
        offset: i64,
        blocks: i64,
        address: usize,
    ) -> Result<()> {
        let f = File::create(local_path);
        if let Ok(f) = f {
            let mut writer = BufWriter::new(f);
            let data = self.read(path, offset, blocks, address)?;
            if writer.write_all(&data).is_ok() {
                println!("file saved on {}", local_path);
                Ok(())
            } else {
                std::fs::remove_file(local_path)?;
                Err(anyhow::anyhow!(
                    "error: failed to save file on local path {}",
                    local_path
                ))
            }
        } else {
            Err(anyhow::anyhow!(
                "error: failed to create file on local path {}",
                local_path
            ))
        }
    }

    fn locate(&self, id: &str, address: usize) -> Result<()> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Locate(LocateParams { id: id.to_string() }),
        ))?;
        let resp = self.nameserver_response_receiver.recv();
        if let Ok(NameserverResponse::Locate(LocateReply {
            path: Some(mut path),
        })) = resp
        {
            if path.len() >= 2 {
                path.remove(0);
                path.remove(0);
            }
            println!("{}", path);
            Ok(())
        } else {
            Err(anyhow::anyhow!("error: unknown file id"))
        }
    }

    fn read(&self, path: String, offset: i64, blocks: i64, address: usize) -> Result<Vec<u8>> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Read(ReadParams {
                path,
                offset,
                blocks,
            }),
        ))?;
        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Read(ReadReply {
            valid,
            file_id: _,
            mut metadatas,
        }) = resp
        {
            if valid && !metadatas.is_empty() {
                metadatas.sort_by_key(|x| x.offset);
                let mut datas = Vec::new();
                // dbg!(&metadatas);
                for (_offset, group) in &metadatas.into_iter().group_by(|x| x.offset) {
                    let mut d = None;
                    for metadata in group {
                        // dbg!(&metadata);
                        let dataserver_tx = &self.dataserver_request_senders[&metadata.address];
                        dataserver_tx
                            .send((address, DataserverRequest::Read(metadata.block_id)))?;
                        let dataserver_rx = &self.dataserver_response_receivers[&metadata.address];
                        let resp = dataserver_rx.recv();
                        // dbg!(&resp);
                        if let Ok(DataserverResponse::Read(status, data)) = resp {
                            if status {
                                d = Some(data);
                                break;
                            } else {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    }
                    if let Some(data) = d {
                        datas.push(data);
                    } else {
                        panic!("data corruption");
                    }
                }
                // metadatas.dedup_by(|x, y| x.start_at == y.start_at);
                assert!(!datas.is_empty());
                let content = datas.into_iter().flat_map(|x| x.blob).collect();
                Ok(content)
            } else {
                Err(anyhow::anyhow!(
                    "error: not a valid file, or offset out of range"
                ))
            }
        } else {
            panic!("error: unknown response {:#?} from nameserver", resp);
        }
    }

    fn write(&self, path: String, data: &[u8], address: usize) -> Result<()> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Write(WriteParams {
                path: path.clone(),
                amount: data.len() as i64,
            }),
        ))?;
        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Write(WriteReply {
            valid,
            addresses,
            id,
        }) = resp
        {
            if valid {
                let slices = Self::split_data(data);
                let mut dataserver_addr_index = 0;
                // dbg!(&addresses);
                for (offset, slice) in slices.into_iter().enumerate() {
                    let dataserver_addr = addresses[dataserver_addr_index];
                    // dbg!(dataserver_addr);
                    let dataserver_tx = &self.dataserver_request_senders[&dataserver_addr];
                    dataserver_tx.send((
                        address,
                        DataserverRequest::Write(Data::new(
                            path.clone(),
                            id.clone(),
                            offset as i64,
                            slice.to_vec(),
                        )),
                    ))?;
                    let dataserver_rx = &self.dataserver_response_receivers[&dataserver_addr];
                    let r = dataserver_rx.recv();
                    if let Ok(DataserverResponse::Write(status)) = r {
                        if !status {
                            panic!("failed to write");
                        } else {
                            // println!("path: {} id: {}", path, id);
                        }
                    }
                    dataserver_addr_index = (dataserver_addr_index + 1) % addresses.len();
                }
                let mut path = path;
                path.remove(0);
                path.remove(0);
                println!("sucessfully write {} @ {}", id, path);
            } else {
                anyhow::bail!("invalid path");
            }
        }
        Ok(())
    }

    fn split_data(data: &[u8]) -> Vec<&[u8]> {
        let mut slices = Vec::new();
        let parts = data.len() / BLOCK_SIZE;
        let reminder = data.len() % BLOCK_SIZE;
        for i in 0..parts {
            slices.push(&data[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE]);
        }
        if reminder > 0 {
            slices.push(&data[parts * BLOCK_SIZE..])
        }
        slices
    }

    fn file(&self, path: String, address: usize) -> Result<()> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Read(ReadParams {
                path: path.clone(),
                offset: 0,
                blocks: i64::max_value(),
            }),
        ))?;
        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Read(ReadReply {
            valid,
            file_id,
            metadatas,
        }) = resp
        {
            if valid {
                if metadatas.is_empty() {
                    let mut path = path;
                    path.remove(0);
                    path.remove(0);
                    println!("{}: directory", &path);
                    Ok(())
                } else {
                    let mut path = path;
                    path.remove(0);
                    path.remove(0);
                    let size = metadatas.iter().map(|x| x.amount).sum::<i64>();
                    let pretty_size = byte_unit::Byte::from_bytes(size as u128)
                        .get_appropriate_unit(true)
                        .to_string();
                    println!("{}: file (size = {}, id = {})", &path, pretty_size, file_id);
                    let table = tabled::Table::new(metadatas.iter().map(DisplayMetadata::from));
                    println!("{}", table);
                    Ok(())
                }
            } else {
                Err(anyhow::anyhow!("error: file not found"))
            }
        } else {
            panic!("error: unknown response {:#?} from nameserver", resp);
        }
    }

    fn create(&self, path: String, address: usize) -> Result<()> {
        self.nameserver_request_sender.send((
            address,
            NameserverRequest::Create(CreateParams { path: path.clone() }),
        ))?;
        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::Create(CreateReply { status }) = resp {
            if status {
                Ok(())
            } else {
                Err(anyhow::anyhow!("error: failed to make directory {}", path))
            }
        } else {
            panic!("error: unknown response {:#?} from nameserver", resp);
        }
    }

    fn list(&self, path: String, address: usize) -> Result<()> {
        self.nameserver_request_sender
            .send((address, NameserverRequest::List(ListParams { path })))?;
        let resp = self.nameserver_response_receiver.recv()?;
        if let NameserverResponse::List(ListReply { list }) = resp {
            if let Some(list) = list {
                let s = list.join("\t");
                println!("{}", s);
                Ok(())
            } else {
                Err(anyhow::anyhow!("error: invalid directory path"))
            }
        } else {
            panic!("error: unknown response {:#?} from nameserver", resp);
        }
    }
}
