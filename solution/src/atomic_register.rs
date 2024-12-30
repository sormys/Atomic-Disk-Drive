use crate::atomic_register_public::AtomicRegister;
use crate::register_client_public;
use crate::register_client_public::RegisterClient;
use crate::sectors_manager_public::SectorsManager;
use std::sync::Arc;
use std::pin::Pin;
use crate::SectorIdx;
use std::future::Future;
use crate::domain::*;

fn generate_unique_id() -> uuid::Uuid {
    uuid::Uuid::new_v4()
}

pub(crate) struct BasicAtomicRegister {
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
    reading: bool,
    writing: bool,
    writing_data: Option<SectorVec>,
    write_phase: bool,
    read_list: Vec<Option<(u64, u8, SectorVec)>>,
    read_list_num: u8,
    read_data: Option<SectorVec>,
    ack_list: Vec<bool>,
    ack_list_num: u8,
    op_id: uuid::Uuid,
    request_identifier: u64,
    callback: Option<Box<
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        >
        >,
    data: Option<SectorVec>,
    metadata: Option<(u64, u8)>,
}

impl BasicAtomicRegister {
    pub(crate) fn new( self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8) -> Self {
        BasicAtomicRegister {
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
            reading: false,
            writing: false,
            writing_data: None,
            write_phase: false,
            read_list: vec![None; processes_count as usize],
            read_list_num: 0,
            read_data: None,
            ack_list: vec![false; processes_count as usize],
            ack_list_num: 0,
            op_id: generate_unique_id(),
            request_identifier: 0,
            callback: None,
            data: None,
            metadata: None,
        }
    }

    async fn get_data(&mut self) -> &SectorVec {
        if self.data.is_none() {
            self.data = Some(self.sectors_manager.read_data(self.sector_idx).await);
        }
        self.data.as_ref().unwrap()
    }

    async fn get_metadata(&mut self) -> (u64, u8) {
        if self.metadata.is_none() {
            self.metadata = Some(self.sectors_manager.read_metadata(self.sector_idx).await);
        }
        self.metadata.unwrap()
    }

    async fn store(&mut self, data: SectorVec, ts: u64, wr: u8) {
        self.sectors_manager.write(self.sector_idx, &(data.clone(), ts, wr)).await;
        self.data = Some(data);
        self.metadata = Some((ts, wr));
    }

    fn reset_read_list(&mut self) {
        for i in 0..self.processes_count {
            self.read_list[i as usize] = None;
        }
        self.read_list_num = 0;
    }

    fn reset_ack_list(&mut self) {
        for i in 0..self.processes_count {
            self.ack_list[i as usize] = false;
        }
        self.ack_list_num = 0;
    }
}

fn highest(data: &Vec<Option<(u64, u8, SectorVec)>>) -> u8 {
    let mut best_idx = 0;
    let mut best_metadata: Option<(u64, u8)> = None;
    let length = data.len() as u8;
    for i in 0..length {
        if data[i as usize].is_none() {
            continue;
        }
        let new_metadata = (data[i as usize].as_ref().unwrap().0, data[i as usize].as_ref().unwrap().1);
        if best_metadata.is_none() || new_metadata > best_metadata.unwrap() {
            best_idx = i;
            best_metadata = Some(new_metadata);
            continue;
        }
    }
    best_idx
}

#[async_trait::async_trait]
impl AtomicRegister for BasicAtomicRegister {
    
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
        >,
    ) {
        self.request_identifier = cmd.header.request_identifier;
        self.op_id = generate_unique_id();
        self.reset_read_list();
        self.reset_ack_list();
        let msg = match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.reading = true;
                register_client_public::Broadcast {
                    cmd: Arc::new(SystemRegisterCommand{
                        header: SystemCommandHeader{
                            process_identifier: self.self_ident,
                            msg_ident: self.op_id,
                            sector_idx: self.sector_idx,
                        },
                        content: SystemRegisterCommandContent::ReadProc,
                    }),
                }
            }
            ClientRegisterCommandContent::Write { data } => {
                self.writing = true;
                self.writing_data = Some(data);
                register_client_public::Broadcast {
                    cmd: Arc::new(SystemRegisterCommand{
                        header: SystemCommandHeader{
                            process_identifier: self.self_ident,
                            msg_ident: self.op_id.clone(),
                            sector_idx: self.sector_idx,
                        },
                        content: SystemRegisterCommandContent::ReadProc,
                    }),
                }
            }
            
        };
        self.callback = Some(success_callback);
        self.register_client.broadcast(msg).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let process_id = cmd.header.process_identifier;
        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                let data = self.get_data().await.clone();
                let (ts, wr) = self.get_metadata().await;
                let msg = register_client_public::Send {
                    cmd: Arc::new(SystemRegisterCommand{
                        header: SystemCommandHeader{
                            process_identifier: self.self_ident,
                            msg_ident: cmd.header.msg_ident,
                            sector_idx: self.sector_idx,
                        },
                        content: SystemRegisterCommandContent::Value {
                            timestamp: ts,
                            write_rank: wr,
                            sector_data: data,
                        },
                    }),
                    target: process_id,
                };
                self.register_client.send(msg).await;
            }
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                if self.op_id != cmd.header.msg_ident || self.write_phase {
                    return;
                }
                let read_list_value = Some((timestamp, write_rank, sector_data));
                if read_list_value == self.read_list[(process_id - 1) as usize] {
                    // Trying to handle doubled messages
                    return;
                }
                self.read_list[(process_id - 1) as usize] = read_list_value;
                self.read_list_num += 1;
                if self.read_list_num > self.processes_count / 2 && (self.reading || self.writing) {
                    let (ts, wr) = self.get_metadata().await;
                    let data = self.get_data().await.clone();
                    self.read_list[(self.self_ident - 1) as usize] = Some((ts, wr, data));

                    let highest_idx = highest(&self.read_list);
                    let (mut maxts, mut rr, mut readval) = self.read_list[highest_idx as usize].take().unwrap();
                    self.read_data = Some(readval.clone());
                    
                    self.reset_read_list();
                    self.reset_ack_list();
                    self.write_phase = true;
                    if !self.reading {
                        (maxts, rr, readval) = (maxts + 1, self.self_ident, self.writing_data.take().unwrap());

                        self.store(readval.clone(), maxts, rr).await;
                        readval = readval;
                    }
                    let msg = register_client_public::Broadcast {
                        cmd: Arc::new(SystemRegisterCommand{
                            header: SystemCommandHeader{
                                process_identifier: self.self_ident,
                                msg_ident: cmd.header.msg_ident,
                                sector_idx: self.sector_idx,
                            },
                            content: SystemRegisterCommandContent::WriteProc { 
                                timestamp: maxts,
                                write_rank: rr,
                                data_to_write: readval, },
                        }),
                    };
                    self.register_client.broadcast(msg).await;
                }
            }
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                let (ts, wr) = self.get_metadata().await;
                if (timestamp, write_rank) > (ts, wr) {
                    self.store(data_to_write, timestamp, write_rank).await;
                }
                self.register_client.send(register_client_public::Send {
                    cmd: Arc::new(SystemRegisterCommand{
                        header: SystemCommandHeader{
                            process_identifier: self.self_ident,
                            msg_ident: cmd.header.msg_ident,
                            sector_idx: self.sector_idx,
                        },
                        content: SystemRegisterCommandContent::Ack,
                    }),
                    target: process_id,
                }).await;
            }
            SystemRegisterCommandContent::Ack => {
                if cmd.header.msg_ident != self.op_id || !self.write_phase {
                    return;
                }

                if self.ack_list[(process_id - 1) as usize] {
                    // Trying to handle doubled messages
                    return;
                }
                self.ack_list[(process_id - 1) as usize] = true;
                self.ack_list_num += 1;
                if self.ack_list_num > self.processes_count / 2 && (self.writing || self.reading) {
                    self.reset_ack_list();
                    self.write_phase = false;
                    let op_ret = if self.reading {
                        self.reading = false;
                        OperationReturn::Read(ReadReturn{
                                read_data: self.read_data.take().unwrap(),}
                            )
                    } else {
                        self.writing = false;
                        OperationReturn::Write
                    };
                    let success = OperationSuccess{
                        request_identifier: self.request_identifier,
                        op_return: op_ret,
                    };
                    self.callback.take().unwrap()(success).await;
                }
            }
        }
    }
}
