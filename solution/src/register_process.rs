use std::collections::HashMap;
use std::future::Future;
use std::os::linux::raw::stat;
use std::os::unix::net::SocketAddr;
use std::pin::Pin;
use async_channel::Receiver;
use async_channel::Sender;
use tokio::task::JoinHandle;

use crate::domain::*;
use crate::transfer_lib;
use crate::transfer_public;
use crate::AtomicRegister;
use async_channel::unbounded;
use tokio::io::AsyncWrite;
use crate::register_client;
use crate::atomic_register;
use crate::sectors_manager;
use crate::transfer_public::*;
use tokio::net::TcpListener;


use std::sync::Arc;
use tokio::sync::Mutex;

pub(crate) type ClientCallback = Box<
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        >;


struct RegisterProcess {
    config: Configuration,
}

async fn handle_command(
    writer: Arc<Mutex<dyn AsyncWrite + Send + Unpin>>,
    command: RegisterCommand,
    hmac_valid: bool,
    hmac_client_key: [u8; 32],
    n_sectors: u64,
    atomic_tx: Sender<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>
    ) {
    if !hmac_valid {
        if let RegisterCommand::Client(ClientRegisterCommand { header, content}) = command{
            let status_code = StatusCode::AuthFailure;
            let msg_type = match content {
                ClientRegisterCommandContent::Read => transfer_lib::ClientMessageType::Read,
                ClientRegisterCommandContent::Write { data: _data } => transfer_lib::ClientMessageType::Write,
            };
            transfer_lib::send_response(
                writer,
                status_code,
                msg_type as u8,
                header.request_identifier,
                None,
                hmac_client_key).await;
        }
        return;
    }

    match command {
        RegisterCommand::Client(ClientRegisterCommand { header, content }) => {
            if header.sector_idx > n_sectors {
                let msg_type = match content {
                    ClientRegisterCommandContent::Read => transfer_lib::ClientMessageType::Read,
                    ClientRegisterCommandContent::Write { data: _data } => transfer_lib::ClientMessageType::Write,
                };
                let status_code = StatusCode::InvalidSectorIndex;
                transfer_lib::send_response(
                    writer,
                    status_code,
                    msg_type as u8,
                    header.request_identifier,
                    None,
                    hmac_client_key,
                ).await;
                return;
            }
            let callback = Some(client_callback(writer, hmac_client_key));
            let is_write = if let ClientRegisterCommandContent::Write {..} = content {
                true
            } else {
                false
            };
            atomic_tx.send((header.sector_idx,
                RegisterCommand::Client(ClientRegisterCommand { header, content }),
                is_write, callback)).await.unwrap();
        }
        RegisterCommand::System(SystemRegisterCommand { header, content }) => {
            let is_write = match content {
                SystemRegisterCommandContent::WriteProc {..} | SystemRegisterCommandContent::Value {..} => true,
                SystemRegisterCommandContent::ReadProc | SystemRegisterCommandContent::Ack => false,
            };
            atomic_tx.send((header.sector_idx,
                RegisterCommand::System(SystemRegisterCommand { header, content }),
                is_write, None)).await.unwrap();
        } 
    }
}

fn client_callback(
    writer: Arc<Mutex<dyn AsyncWrite + Send + Unpin>>,
    hmac_client_key: [u8; 32]) -> ClientCallback {
    Box::new(move |response: OperationSuccess| {
        let writer = Arc::clone(&writer);
        Box::pin(async move {
            let mut msg_type = transfer_lib::ClientMessageType::Read;
            let content: Option<SectorVec> = match response.op_return {
                OperationReturn::Read(data) => {
                    Some(data.read_data.clone())
                }
                OperationReturn::Write => {
                    msg_type = transfer_lib::ClientMessageType::Write;
                    None
                }
            };
            transfer_lib::send_response(
                writer,
                StatusCode::Ok,
                msg_type as u8,
                response.request_identifier,
                content.as_ref(),
                hmac_client_key,
            ).await;
        })
    })
}

async fn handle_tcp_connection(stream: tokio::net::TcpStream, n_sectors: u64, hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], atomic_tx: Sender<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>) {
    let (mut read, write) = stream.into_split();

    let write_arc = Arc::new(Mutex::new(write));
    while let Ok((command, hmac_valid)) =
        transfer_public::deserialize_register_command(&mut read, &hmac_system_key, &hmac_client_key).await {
        handle_command(write_arc.clone(), command, hmac_valid, hmac_client_key, n_sectors, atomic_tx.clone()).await;
    }

}

async fn listen_tcp(listener: TcpListener, n_sectors: u64, hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], atomic_tx: Sender<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>) {
    while let Ok((stream, _socket)) = listener.accept().await {
        tokio::spawn(handle_tcp_connection(stream,
            n_sectors,
            hmac_system_key,
            hmac_client_key,
            atomic_tx.clone()));
    }
}

async fn process_command(aregister: Arc<Mutex<dyn AtomicRegister + Send>>, command: RegisterCommand, callback: Option<ClientCallback>) {
    let mut aregister = aregister.lock().await;
    match command {
        RegisterCommand::Client(client_command) => {
            aregister.client_command(client_command, callback.unwrap()).await;
        }
        RegisterCommand::System(system_command) => {
            aregister.system_command(system_command).await;
        }
    }
}


async fn manage_atomic_registers_task(
    process_count: u8,
    process_rank: u8,
    register_client: Arc<register_client::BasicRegisterClient>,
    sectors_manager: Arc<sectors_manager::BasicSectorsManager>,
    rx: Receiver<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>) {
    let mut modified_atomic_registers: HashMap<SectorIdx, Arc<Mutex<dyn AtomicRegister + Send>>> = HashMap::new();
    let mut tmp_atomic_registers: HashMap<SectorIdx, Arc<Mutex<dyn AtomicRegister + Send>>> = HashMap::new();
    let mut tmp_atomic_registers_count: u64 = 0;

    while let Ok((sector_idx, command, is_write, callback)) = rx.recv().await {
        let aregister: Arc<Mutex<dyn AtomicRegister + Send>> = 
            if modified_atomic_registers.contains_key(&sector_idx) {
                modified_atomic_registers.get(&sector_idx).unwrap().clone()
            } else if is_write {
                if tmp_atomic_registers.contains_key(&sector_idx) {
                    let atomic_register = tmp_atomic_registers.remove(&sector_idx).unwrap();
                    tmp_atomic_registers_count -= 1;
                    modified_atomic_registers.insert(sector_idx, atomic_register.clone());
                    atomic_register
                } else {
                    let atomic_register = Arc::new(
                        Mutex::new(
                            atomic_register::BasicAtomicRegister::new(
                                process_rank,
                                sector_idx,
                                register_client.clone(),
                                sectors_manager.clone(),
                                process_count,
                            )));
                    modified_atomic_registers.insert(sector_idx, atomic_register.clone());
                    atomic_register
                }
            } else {
                // TODO: limit the number of atomic registers
                tmp_atomic_registers.entry(sector_idx).or_insert_with(|| {
                    tmp_atomic_registers_count += 1;
                    Arc::new(Mutex::new(atomic_register::BasicAtomicRegister::new(
                        process_rank,
                        sector_idx,
                        register_client.clone(),
                        sectors_manager.clone(),
                        process_count,
                    )))
                }).clone()
            };
        tokio::spawn(async move {process_command(aregister, command, callback).await;});
    }         
}


pub(crate) async fn begin_register_process(config: Configuration, listener: TcpListener) {
    let (tx, rx) = unbounded::<(SectorIdx, RegisterCommand, bool, Option<ClientCallback>)>();
    let rclient = Arc::new(register_client::BasicRegisterClient::new(
        config.public.self_rank,
        config.public.tcp_locations.clone(),
        tx.clone(),
        config.hmac_system_key,
    ));
    let process_count = config.public.tcp_locations.len() as u8;
    let smanager = Arc::new(sectors_manager::BasicSectorsManager::new(&config.public.storage_dir).await);
    tokio::spawn(manage_atomic_registers_task(process_count, config.public.self_rank, rclient.clone(), smanager.clone(), rx));
    listen_tcp(listener,
        config.public.n_sectors, config.hmac_system_key,
        config.hmac_client_key, tx).await;
}
