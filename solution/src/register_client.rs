
use async_channel::{unbounded, Sender};

use crate::{register_client_public::*, RegisterCommand, SystemRegisterCommand, SystemRegisterCommandContent};
use crate::common::InternalCommand;

#[derive(Clone)]
pub(crate) struct BasicRegisterClient {
    senders: Vec<Sender<Box<SystemRegisterCommand>>>,
    self_rank: u8,
    local_tx: Sender<InternalCommand>,
}

mod tcp {
    use async_channel::Receiver;
    use crate::{transfer_lib::serialize_command, SystemRegisterCommand};
    use crate::{common, RegisterCommand};

    static RETRY_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(150);
    static CONN_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_millis(500);

    pub(crate) async fn connect_to_process(host: String, port: u16, hmac_system_key: [u8; 64], forwarding_rx: Receiver<Box<SystemRegisterCommand>>) {
        let mut current_command = None;
        let _semaphore_permit = common::FD_SEMAPHORE.acquire().await.unwrap();
        loop {
            let connection_attempt =
                tokio::time::timeout(CONN_TIMEOUT, 
                    tokio::net::TcpStream::connect(format!("{}:{}", host, port))).await;
            match connection_attempt {
                Err(_) | Ok(Err(_)) => {
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                    continue;
                }
                Ok(Ok(stream)) => {
                    forward_commands(stream, &hmac_system_key, &mut current_command, &forwarding_rx).await;
                }
            };
        }
    }

    pub(crate) async fn forward_commands(
            mut stream: tokio::net::TcpStream,
            hmac_system_key: &[u8; 64],
            current_command: &mut Option<RegisterCommand>, // Placeholder for failed command to be retried.
            forwarding_rx: &Receiver<Box<SystemRegisterCommand>>) {
        
        loop {
            if current_command.is_none() {
                match forwarding_rx.try_recv() {
                    Ok(command) => { current_command.replace(RegisterCommand::System(*command)); },
                    Err(_) => {return;}
                }
            }
            
            if !serialize_command(current_command.as_ref().unwrap(), &mut stream, &hmac_system_key[..]).await.is_err() {
                // No error, no need to retry sending the command.
                *current_command = None;
            } else {
                // Error, current_command should be retried.
                return;
            }
        }
    }
}

impl BasicRegisterClient {
    pub(crate) fn new(self_rank: u8, tcp_locations: Vec<(String, u16)>,
        local_tx: Sender<InternalCommand>, hmac_system_key: [u8; 64]) -> Self {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();

        // Create channels to store all commands before tcp communication.
        // Channel for self is just a placeholder - it will not be used.
        for _location in tcp_locations.iter() {
            let (tx, rx) = unbounded::<Box<SystemRegisterCommand>>();
            senders.push(tx);
            receivers.push(rx);
        }

        for (i, (ip, port)) in tcp_locations.iter().enumerate() {
            if i + 1 == self_rank as usize {
                // Do not listen for commands from self using tcp.
                continue;
            }
            tokio::spawn(tcp::connect_to_process(ip.clone(), *port, hmac_system_key, receivers[i].clone()));
        }

        BasicRegisterClient {
            senders,
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
        if msg.target == self.self_rank {
            self.send_local(msg).await;
            return;
        }
        let cmd = Box::new((*msg.cmd).clone());
        self.senders[(msg.target - 1) as usize].send(cmd).await.unwrap();
    }

    async fn broadcast(&self, msg: Broadcast) {
        let cmd = Box::new((*msg.cmd).clone());
        let mut tcp_sent = Vec::new();
        // Trigger tcp send first to reduce latency.
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