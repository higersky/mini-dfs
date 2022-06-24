mod client;
mod dataserver;
mod helper;
mod nameserver;

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::AtomicBool,
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
};

use anyhow::Result;
use client::Client;
use dataserver::{Dataserver, DataserverRequest, DataserverResponse};
use nameserver::Nameserver;
use path_absolutize::Absolutize;

fn main() -> Result<()> {
    let wd = Path::new("./minidfs").absolutize()?;
    let mut nameserver = Nameserver::new(&wd)?;

    let dataserver_ids = [1, 2, 3, 4];

    let mut d1 = Dataserver::new(&wd.join("d1"), nameserver.channel())?;
    let mut d2 = Dataserver::new(&wd.join("d2"), nameserver.channel())?;
    let mut d3 = Dataserver::new(&wd.join("d3"), nameserver.channel())?;
    let mut d4 = Dataserver::new(&wd.join("d4"), nameserver.channel())?;

    // Bootstrap network

    let mut dataserver_request_senders: HashMap<usize, Sender<(usize, DataserverRequest)>> =
        HashMap::new();
    let mut dataserver_response_receivers: HashMap<usize, Receiver<DataserverResponse>> =
        HashMap::new();
    let mut dataserver_response_senders: HashMap<usize, Sender<DataserverResponse>> =
        HashMap::new();

    for (id, dataserver) in dataserver_ids.into_iter().zip(&[&d1, &d2, &d3, &d4]) {
        let (tx, rx) = channel();
        dataserver_response_senders.insert(id, tx);
        dataserver_response_receivers.insert(id, rx);
        dataserver_request_senders.insert(id, dataserver.channel());
        nameserver.add_client_tx(id, dataserver.nameserver_channel())
    }

    let (nameserver_response_tx, nameserver_response_rx) = channel();
    nameserver.add_client_tx(0, nameserver_response_tx);

    let mut client = Client::new(
        nameserver.channel(),
        nameserver_response_rx,
        dataserver_request_senders.clone(),
        dataserver_response_receivers,
    );

    d1.set_senders(
        dataserver_request_senders.clone(),
        dataserver_response_senders.clone(),
    );
    d2.set_senders(
        dataserver_request_senders.clone(),
        dataserver_response_senders.clone(),
    );
    d3.set_senders(
        dataserver_request_senders.clone(),
        dataserver_response_senders.clone(),
    );
    d4.set_senders(
        dataserver_request_senders.clone(),
        dataserver_response_senders.clone(),
    );

    let stop_token = Arc::new(AtomicBool::new(false));
    let cloned_token = stop_token.clone();
    let tn = std::thread::spawn(move || (nameserver.run(&cloned_token)));
    let cloned_token = stop_token.clone();
    let t1 = std::thread::spawn(move || (d1.run(&cloned_token, 1)));
    let cloned_token = stop_token.clone();
    let t2 = std::thread::spawn(move || (d2.run(&cloned_token, 2)));
    let cloned_token = stop_token.clone();
    let t3 = std::thread::spawn(move || (d3.run(&cloned_token, 3)));
    let cloned_token = stop_token.clone();
    let t4 = std::thread::spawn(move || (d4.run(&cloned_token, 4)));

    let cloned_token = stop_token;
    (client.run(&cloned_token, &wd, 0))?;

    let _ = t1.join();
    let _ = t2.join();
    let _ = t3.join();
    let _ = t4.join();
    let _ = tn.join();
    Ok(())
}
