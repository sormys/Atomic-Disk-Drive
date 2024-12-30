use std::collections::HashMap;
use async_channel::Receiver;
use std::collections::VecDeque;
use async_channel::Sender;
use tokio::sync::OwnedSemaphorePermit;

use crate::common;
use crate::common::InternalCommand;
use crate::domain::*;
use crate::transfer_lib;
use crate::transfer_public;
use crate::AtomicRegister;
use async_channel::unbounded;
use tokio::io::AsyncWrite;
use crate::register_client;
use crate::atomic_register;
use crate::sectors_manager;
use tokio::net::TcpListener;
use crate::common::ClientCallback;

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;

async fn handle_command(
    writer: Arc<Mutex<dyn AsyncWrite + Send + Unpin>>,
    command: RegisterCommand,
    hmac_valid: bool,
    hmac_client_key: [u8; 32],
    n_sectors: u64,
    atomic_tx: Sender<InternalCommand>
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
            if header.sector_idx >= n_sectors {
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

async fn handle_tcp_connection(stream: tokio::net::TcpStream, n_sectors: u64, hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], atomic_tx: Sender<InternalCommand>) {
    let (mut read, write) = stream.into_split();

    let write_arc = Arc::new(Mutex::new(write));
    while let Ok((command, hmac_valid)) =
        transfer_public::deserialize_register_command(&mut read, &hmac_system_key, &hmac_client_key).await {        
        handle_command(write_arc.clone(), command, hmac_valid, hmac_client_key, n_sectors, atomic_tx.clone()).await;
    }

}

async fn listen_tcp(listener: TcpListener, n_sectors: u64, hmac_system_key: [u8; 64], hmac_client_key: [u8; 32], atomic_tx: Sender<InternalCommand>) {
    let _semaphore_permit = common::FD_SEMAPHORE.acquire().await.unwrap();
    while let Ok((stream, _socket)) = listener.accept().await {
        tokio::spawn(handle_tcp_connection(stream,
            n_sectors,
            hmac_system_key,
            hmac_client_key,
            atomic_tx.clone()));
    }
}

fn wrap_permit_relase(callback: ClientCallback, semaphore_permit: OwnedSemaphorePermit) -> ClientCallback {
    Box::new(move |response: OperationSuccess| {
        let semaphore_permit = semaphore_permit;
        Box::pin(async move {
            callback(response).await;
            drop(semaphore_permit);
        })
    })
}

async fn process_command(aregister: Arc<Mutex<dyn AtomicRegister + Send>>, command: RegisterCommand, callback: Option<ClientCallback>, semaphore: Arc<Semaphore>) {
    match command {
        RegisterCommand::Client(client_command) => {
            let semaphore_permit = semaphore.clone().acquire_owned().await.unwrap();
            let callback = callback.map(|callback| wrap_permit_relase(callback, semaphore_permit));
            let mut aregister = aregister.lock().await;
            // Only one client command can be processed at a time for single sector.
            aregister.client_command(client_command, callback.unwrap()).await;
        }
        RegisterCommand::System(system_command) => {
            let mut aregister = aregister.lock().await;
            aregister.system_command(system_command).await;
        }
    }
}

struct AtomicRegisterManager {
    tmp_atomic_registers: HashMap<SectorIdx, Arc<Mutex<atomic_register::BasicAtomicRegister>>>,
    tmp_atomic_registers_order: VecDeque<SectorIdx>,
    tmp_atomic_registers_count: usize,
    process_rank: u8,
    register_client: Arc<register_client::BasicRegisterClient>,
    sectors_manager: Arc<sectors_manager::BasicSectorsManager>,
    process_count: u8,
    command_rx: Receiver<InternalCommand>,
}

impl AtomicRegisterManager {
    const MAX_TMP_ATOMIC_REGISTERS: usize = 400;
    fn new(
        process_rank: u8,
        register_client: Arc<register_client::BasicRegisterClient>,
        sectors_manager: Arc<sectors_manager::BasicSectorsManager>,
        process_count: u8,
        rx: Receiver<InternalCommand>,
    ) -> Self {
        Self {
            tmp_atomic_registers: HashMap::new(),
            tmp_atomic_registers_order: VecDeque::new(),
            tmp_atomic_registers_count: 0,
            process_rank,
            register_client,
            sectors_manager,
            process_count,
            command_rx: rx,
        }
    }

    fn get_or_insert_tmp_atomic_register(&mut self, sector_idx: SectorIdx) -> Arc<Mutex<atomic_register::BasicAtomicRegister>> {
        if self.tmp_atomic_registers.contains_key(&sector_idx) {
            return self.tmp_atomic_registers.get(&sector_idx).unwrap().clone();
        }
        // No register for sector available. 

        // Remove the oldest register to limit amount of registers without writes.
        if self.tmp_atomic_registers_count >= Self::MAX_TMP_ATOMIC_REGISTERS {
             if let Some(oldest_sector_idx) = self.tmp_atomic_registers_order.pop_front() {
                self.tmp_atomic_registers.remove(&oldest_sector_idx);
                self.tmp_atomic_registers_count -= 1;
            }
        }

        let new_register = Arc::new(Mutex::new(atomic_register::BasicAtomicRegister::new(
            self.process_rank,
            sector_idx,
            self.register_client.clone(),
            self.sectors_manager.clone(),
            self.process_count,
        )));
        self.tmp_atomic_registers.insert(sector_idx, new_register.clone());
        self.tmp_atomic_registers_order.push_back(sector_idx);
        self.tmp_atomic_registers_count += 1;
    
        new_register
    }

    async fn run(&mut self) {
        let mut modified_atomic_registers: HashMap<SectorIdx, Arc<Mutex<dyn AtomicRegister + Send>>> = HashMap::new();
        let mut semaphores: HashMap<SectorIdx, Arc<Semaphore>> = HashMap::new();
    
        while let Ok((sector_idx, command, is_write, callback)) = self.command_rx.recv().await {
            let aregister: Arc<Mutex<dyn AtomicRegister + Send>> = 
                if modified_atomic_registers.contains_key(&sector_idx) {
                    modified_atomic_registers.get(&sector_idx).unwrap().clone()
                } else if is_write {
                    if self.tmp_atomic_registers.contains_key(&sector_idx) {
                        let atomic_register = self.tmp_atomic_registers.remove(&sector_idx).unwrap();
                        self.tmp_atomic_registers_count -= 1;
                        modified_atomic_registers.insert(sector_idx, atomic_register.clone());
                        atomic_register
                    } else {
                        let atomic_register = Arc::new(
                            Mutex::new(
                                atomic_register::BasicAtomicRegister::new(
                                    self.process_rank,
                                    sector_idx,
                                    self.register_client.clone(),
                                    self.sectors_manager.clone(),
                                    self.process_count,
                                )));
                        modified_atomic_registers.insert(sector_idx, atomic_register.clone());
                        atomic_register
                    }
                } else {
                    self.get_or_insert_tmp_atomic_register(sector_idx)
                };
            let semaphore = semaphores.entry(sector_idx).or_insert_with(|| Arc::new(Semaphore::new(1))).clone();
            tokio::spawn(async move {process_command(aregister, command, callback, semaphore).await;});
        }         
    }
}

pub(crate) async fn begin_register_process(config: Configuration, listener: TcpListener) {
    let (tx, rx) = unbounded::<InternalCommand>();
    let rclient = Arc::new(register_client::BasicRegisterClient::new(
        config.public.self_rank,
        config.public.tcp_locations.clone(),
        tx.clone(),
        config.hmac_system_key,
    ));
    let process_count = config.public.tcp_locations.len() as u8;
    let smanager = Arc::new(sectors_manager::BasicSectorsManager::new(&config.public.storage_dir).await);
    let mut atomic_register_manager = AtomicRegisterManager::new(
        config.public.self_rank,
        rclient.clone(),
        smanager.clone(),
        process_count,
        rx,
    );
    tokio::spawn(async move {
        atomic_register_manager.run().await;
    });
    listen_tcp(listener,
        config.public.n_sectors, config.hmac_system_key,
        config.hmac_client_key, tx).await;
}
