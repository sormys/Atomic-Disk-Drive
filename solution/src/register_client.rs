use std::alloc::System;

use async_channel::{unbounded, Receiver, Recv, Sender};
use hmac::digest::consts::P1013;

use crate::{register_client_public::*, RegisterCommand, SectorIdx, SystemRegisterCommand, SystemRegisterCommandContent};
use crate::register_process::ClientCallback;

#[derive(Clone)]
pub(crate) struct BasicRegisterClient {
    tcp_locations: Vec<(String, u16)>,
    senders: Vec<Sender<Box<SystemRegisterCommand>>>,
    receivers: Vec<Receiver<Box<SystemRegisterCommand>>>,
    self_rank: u8,
    local_tx: Sender<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>,
}


mod tcp {
    use async_channel::{Sender, Receiver};
    use crate::{transfer_lib::serialize_command, SystemRegisterCommand};
    use crate::RegisterCommand;

    static RETRY_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(300);
    static CONN_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(500);

    pub(crate) async fn writing_task(host: String, port: u16, hmac_system_key: [u8; 64], forwarding_rx: Receiver<Box<SystemRegisterCommand>>) {
        loop {
            let connection_attempt =
                tokio::time::timeout(CONN_TIMEOUT, 
                    tokio::net::TcpStream::connect(format!("{}:{}", host, port))).await;
            match connection_attempt {
                Err(_) => {
                    log::info!("Failed to connect before timeout to the {}:{}.", host, port);
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                    continue;
                }
                Ok(Err(_)) => {
                    log::info!("Connection to the {}:{} refused", host, port);
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                    continue;
                }
                Ok(Ok(stream)) => {
                    log::info!("Connected to the {}:{}.", host, port);
                    handle_out_connection(host, port, stream, &hmac_system_key, &forwarding_rx).await;
                    return;
                }
            };
        }
    }

    pub(crate) async fn handle_out_connection(
            host: String,
            port: u16,
            mut stream: tokio::net::TcpStream,
            hmac_system_key: &[u8; 64],
            forwarding_rx: &Receiver<Box<SystemRegisterCommand>>) {
        loop {
            let command = forwarding_rx.recv().await;
            match command {
                Ok(command) => {
                    let command = RegisterCommand::System(*command);
                    let sent = serialize_command(&command, &mut stream, &hmac_system_key[..]).await;
                    if sent.is_err() {
                        log::warn!("Failed to send command to the {}:{} (serialization error?).", host, port);
                        return;
                    }
                }
                Err(_) => {
                    log::warn!("Forwarding channel closed.");
                    return;
                }
            }
        }
    }
}

// #Derive clone
impl BasicRegisterClient {
    pub(crate) fn new(self_rank: u8, tcp_locations: Vec<(String, u16)>,
        local_tx: Sender<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>, hmac_system_key: [u8; 64]) -> Self {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        // Create channels to store all commands before tcp communication.
        for _location in tcp_locations.iter() {
            let (tx, rx) = unbounded::<Box<SystemRegisterCommand>>();
            senders.push(tx);
            receivers.push(rx);
        }

        for (i, (ip, port)) in tcp_locations.iter().enumerate() {
            if i == self_rank as usize {
                // Do not listen for commands from self using tcp.
                continue;
            }
            tokio::spawn(tcp::writing_task(ip.clone(), *port, hmac_system_key, receivers[i].clone()));
        }

        BasicRegisterClient {
            tcp_locations,
            senders,
            receivers,
            self_rank,
            local_tx
        }
    }

    async fn send_local(&self, msg: Send) {
        let register_msg = RegisterCommand::System((*msg.cmd).clone());
        let is_write = match msg.cmd.content {
            SystemRegisterCommandContent::WriteProc {..} | SystemRegisterCommandContent::Value {..} => true,
            SystemRegisterCommandContent::ReadProc | SystemRegisterCommandContent::Ack => false,
        };
        self.local_tx.send((msg.cmd.header.sector_idx, register_msg, is_write, None)).await.unwrap();
    }

}

#[async_trait::async_trait]
impl RegisterClient for BasicRegisterClient {

    async fn send(&self, msg: Send) {
        let dest = msg.target;
        if msg.target == self.self_rank {
            self.send_local(msg).await;
            return;
        }
        let cmd = Box::new((*msg.cmd).clone());
        self.senders[dest as usize].send(cmd).await.unwrap();
    }

    async fn broadcast(&self, msg: Broadcast) {
        let cmd = Box::new((*msg.cmd).clone());
        let mut tcp_sent = Vec::new();
        for (i, s) in self.senders.iter().enumerate() {
            if i + 1 == self.self_rank as usize {
                continue;
            }
            tcp_sent.push(s.send(cmd.clone()));
        }
        self.send_local(Send { cmd: msg.cmd, target: self.self_rank }).await;
        for s in tcp_sent {
            s.await.unwrap();
        }
    }
}