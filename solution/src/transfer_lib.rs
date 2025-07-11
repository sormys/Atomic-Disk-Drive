use crate::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SectorVec, StatusCode, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, MAGIC_NUMBER};
use std::{io::Error, sync::Arc, vec};
use tokio::{io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt}, sync::Mutex};
use hmac::{Hmac, Mac};
use async_trait::async_trait;

const DATA_SIZE: usize = 4096;
const MAGIC_NUMBER_LEN: usize = 4;

enum MessageType {
    Client(ClientMessageType),
    System(SystemMessageType),
}

#[repr(u8)]
pub(crate) enum ClientMessageType {
    Read = 1,
    Write = 2,
}

enum SystemMessageType {
    ReadProc = 3,
    Value = 4,
    WriteProc = 5,
    Ack = 6,
}


#[repr(usize)]
enum Padding {
    ClientCommand = 3,
    SystemCommand = 2,
    SystemCommandContent = 4,
}

// DESERIALIZATION


pub(crate) async fn read_until_magic_number(
    data: &mut (dyn AsyncRead + Send + Unpin),
) -> Result<(), Error> {
    let mut buf = vec![0u8; MAGIC_NUMBER_LEN];
    let mut pos = 0;

    loop {
        data.read_exact(&mut buf[pos..pos + 1]).await?;
        pos = (pos + 1) % MAGIC_NUMBER_LEN;

        if buf[pos..].iter().chain(&buf[..pos]).eq(MAGIC_NUMBER.iter()) {
            return Ok(());
        }
    }
}

async fn deserialize_header(
        data: &mut (dyn AsyncRead + Send + Unpin),
        received: &mut Vec<u8>,
    ) -> Result<MessageType, Error> {
    loop {
        received.clear();
        read_until_magic_number(data).await?;
        let mut padding = [0u8; Padding::SystemCommand as usize];
        data.read_exact(&mut padding).await?;
        // This may but does not have to be process rank (might just be padding)
        let mut process_rank = [0u8; 1];
        let mut message_type_raw = [0u8; 1];
        data.read_exact(&mut process_rank).await?;
        data.read_exact(&mut message_type_raw).await?;

        let message_type: Option<MessageType> = match u8::from_be_bytes(message_type_raw) {
            1 => Some(MessageType::Client(ClientMessageType::Read)),
            2 => Some(MessageType::Client(ClientMessageType::Write)),
            3 => Some(MessageType::System(SystemMessageType::ReadProc)),
            4 => Some(MessageType::System(SystemMessageType::Value)),
            5 => Some(MessageType::System(SystemMessageType::WriteProc)),
            6 => Some(MessageType::System(SystemMessageType::Ack)),
            _ => None
        };
        if message_type.is_none() {
            continue;
        }

        // Store received data for HMAC
        received.extend_from_slice(&MAGIC_NUMBER);
        received.extend_from_slice(&padding);
        received.extend_from_slice(&process_rank);
        received.extend_from_slice(&message_type_raw);
        return Ok(message_type.unwrap());
    }
}

async fn deserialize_client_message(
    message_type: ClientMessageType,
    data: &mut (dyn AsyncRead + Send + Unpin),
    received: &mut Vec<u8>) -> Result<RegisterCommand, Error> {
        let mut request_identifier = [0u8; 8];
        data.read_exact(&mut request_identifier).await?;
        let mut sector_idx = [0u8; 8];
        data.read_exact(&mut sector_idx).await?;

        received.extend_from_slice(&request_identifier);
        received.extend_from_slice(&sector_idx);

        let client_header = ClientCommandHeader {
            request_identifier: u64::from_be_bytes(request_identifier),
            sector_idx: u64::from_be_bytes(sector_idx),
        };

        let content: ClientRegisterCommandContent = match message_type {
            ClientMessageType::Read => ClientRegisterCommandContent::Read,
            ClientMessageType::Write => {
                let mut content = vec![0u8; DATA_SIZE];
                data.read_exact(&mut content).await?;
                received.extend_from_slice(&content);
                ClientRegisterCommandContent::Write {
                    data: SectorVec(content),
                }
            }
        };
        
        Ok(RegisterCommand::Client(
            ClientRegisterCommand {
                header: client_header,
                content: content
            })
        )
}

async fn deserialize_system_message_content(
    data: &mut (dyn AsyncRead + Send + Unpin),
    received: &mut Vec<u8>
) -> Result<(u64, u8, SectorVec), Error> {
    let mut timestamp = [0u8; 8];
    data.read_exact(&mut timestamp).await?;
    let mut padding = [0u8; Padding::SystemCommandContent as usize];
    data.read_exact(&mut padding).await?;
    let mut write_rank = [0u8; 1];
    data.read_exact(&mut write_rank).await?;
    let mut sector_data = vec![0u8; DATA_SIZE];
    data.read_exact(&mut sector_data).await?;
    received.extend_from_slice(&timestamp);
    received.extend_from_slice(&padding);
    received.extend_from_slice(&write_rank);
    received.extend_from_slice(&sector_data);
    let timestamp_ref = u64::from_be_bytes(timestamp);
    let write_rank_ref = write_rank[0];
    let sector_data_ref = SectorVec(sector_data);
    Ok((timestamp_ref, write_rank_ref, sector_data_ref))
}

async fn deserialize_system_message(
    message_type: SystemMessageType,
    data: &mut (dyn AsyncRead + Send + Unpin),
    received: &mut Vec<u8>
) -> Result<RegisterCommand, Error> {
    let mut uuid_raw = [0u8; 16];
    data.read_exact(&mut uuid_raw).await?;
    received.extend_from_slice(&uuid_raw);
    uuid_raw.reverse();
    let mut sector_idx = [0u8; 8];
    data.read_exact(&mut sector_idx).await?;
    received.extend_from_slice(&sector_idx);
    
    let header = SystemCommandHeader {
        // Process identifier is in the message header.
        process_identifier: received[6],
        msg_ident: uuid::Uuid::from_bytes(uuid_raw),
        sector_idx: u64::from_be_bytes(sector_idx),
    };

    let content = match message_type {
        SystemMessageType::ReadProc => SystemRegisterCommandContent::ReadProc,
        SystemMessageType::Value => {
            let (timestamp, write_rank, sector_data) = 
                deserialize_system_message_content(data, received).await?;
            SystemRegisterCommandContent::Value {
                timestamp: timestamp,
                write_rank: write_rank,
                sector_data: sector_data,
            }
        }
        SystemMessageType::WriteProc => {
            let (timestamp, write_rank, data_to_write) = 
                deserialize_system_message_content(data, received).await?;
            SystemRegisterCommandContent::WriteProc {
                timestamp: timestamp,
                write_rank: write_rank,
                data_to_write: data_to_write,
            }
        }
        SystemMessageType::Ack => SystemRegisterCommandContent::Ack,
    };

    return Ok(RegisterCommand::System(
        SystemRegisterCommand {
            header: header,
            content: content,
        }
    ));
}

async fn verify_hmac(
    data: &mut (dyn AsyncRead + Send + Unpin),
    received: &Vec<u8>,
    hmac_key: &Vec<u8>,
) -> Result<bool, Error> {
    let mut hmac_bytes = [0u8; 32];
    data.read_exact(&mut hmac_bytes).await?;
    if let Ok(mut hmac) = Hmac::<sha2::Sha256>::new_from_slice(hmac_key) {
        hmac.update(&received);
        return Ok(hmac.verify_slice(&hmac_bytes).is_ok());
    }
    Ok(false)
}

pub(crate) async fn deserialize_data(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    let mut received = Vec::new();
    let message_type = deserialize_header(data, &mut received).await?;
    let (hmac_key, cmd) = match message_type {
        MessageType::Client(client_message_type) => {
            (hmac_client_key.to_vec(), deserialize_client_message(client_message_type, data, &mut received).await?)
        }
        MessageType::System(system_message_type) => {
            (hmac_system_key.to_vec(), deserialize_system_message(system_message_type, data, &mut received).await?)
        }
    };

    let hmac_valid = verify_hmac(data, &received, &hmac_key).await?;
    Ok((cmd, hmac_valid))
}

// SERIALIZATION

#[async_trait]
trait SerializeCommand {
    async fn serialize(
        &self,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error>;
}

pub(crate) async fn serialize_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    match cmd {
        RegisterCommand::Client(client_cmd) => {
            client_cmd.serialize(writer, hmac_key).await
        }
        RegisterCommand::System(system_cmd) => {
            system_cmd.serialize(writer, hmac_key).await
        }
    }
}

#[async_trait]
impl SerializeCommand for ClientRegisterCommand {
    async fn serialize(
        &self,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
    let mut serialized: Vec<u8> = Vec::new();
    // Magic number
    serialized.extend_from_slice(&MAGIC_NUMBER);
    // Padding
    serialized.extend_from_slice(&[0u8; Padding::ClientCommand as usize]);
    // Message type + store content
    let mut content = Vec::new();
    match self.content {
        ClientRegisterCommandContent::Read => {
            serialized.push(1);
        }
        ClientRegisterCommandContent::Write { ref data } => {
            serialized.push(2);
            if data.0.len() != DATA_SIZE {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other, "Data is not 4096 bytes"));
            }
            content.extend_from_slice(data.0.as_slice());
        }
    }
    // Identifier
    serialized.extend_from_slice(
        &self.header.request_identifier.to_be_bytes());
    // Sector index
    serialized.extend_from_slice(&self.header.sector_idx.to_be_bytes());
    // Store content
    serialized.extend_from_slice(content.as_slice());
    // Hmac
    let mut hmac = Hmac::<sha2::Sha256>::new_from_slice(hmac_key)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Sending message HMAC error"))?;
    hmac.update(&serialized);
    let hmac_bytes = hmac.finalize().into_bytes();
    serialized.extend_from_slice(&hmac_bytes);

    writer.write_all(serialized.as_slice()).await?;
    return Ok(());
    }
}

pub(crate) fn append_system_command_content(
    store_vector: &mut Vec<u8>, timestamp: &u64, write_rank: &u8, content: &SectorVec
) -> Result<(), Error> {
    // Timestamp
    store_vector.extend_from_slice(&timestamp.to_be_bytes());
    // Padding
    store_vector.extend_from_slice(&[0u8; Padding::SystemCommandContent as usize]);
    // Value wr
    store_vector.push(*write_rank);
    // Sector data
    if content.0.len() != DATA_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other, "Sector data is not 4096 bytes"));
    }
    store_vector.extend_from_slice(content.0.as_slice());
    return Ok(());
}

#[async_trait]
impl SerializeCommand for SystemRegisterCommand {
    async fn serialize(
        &self,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        let mut serialized: Vec<u8> = Vec::new();
        // Magic number
        serialized.extend_from_slice(&MAGIC_NUMBER);
        // Padding
        serialized.extend_from_slice(&[0u8; Padding::SystemCommand as usize]);
        // Unpack header
        let SystemCommandHeader {
            process_identifier,
            msg_ident,
            sector_idx,
        } = self.header;
        // Unpack content
        let mut content: Vec<u8> = Vec::new();
        let message_type: u8 = match &self.content {
            SystemRegisterCommandContent::ReadProc => {
                3
            }
            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                append_system_command_content(
                    &mut content, timestamp, write_rank, sector_data)?;
                4
            }
            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                append_system_command_content(
                    &mut content,timestamp, write_rank, data_to_write)?;
                5
            }
            SystemRegisterCommandContent::Ack => {
                6
            }
        };
        // Process rank
        serialized.extend_from_slice(&process_identifier.to_be_bytes());
        // Message Type
        serialized.extend_from_slice(&message_type.to_be_bytes());
        // UUID
        let mut uuid_bytes = msg_ident.as_bytes().clone();
        uuid_bytes.reverse();
        serialized.extend_from_slice(&uuid_bytes);
        // Sector index
        serialized.extend_from_slice(&sector_idx.to_be_bytes());
        // Store content
        serialized.extend_from_slice(content.as_slice());
        // Hmac
        let mut hmac = Hmac::<sha2::Sha256>::new_from_slice(hmac_key)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Sending message HMAC error"))?;
        hmac.update(&serialized);
        let hmac_bytes = hmac.finalize().into_bytes();
        serialized.extend_from_slice(&hmac_bytes);

        writer.write_all(serialized.as_slice()).await?;

        return Ok(());
    }
}

// RESPONSE

pub(crate) async fn send_response(
    writer: Arc<Mutex<dyn AsyncWrite + Send + Unpin>>,
    status_code: StatusCode,
    msg_type:  u8,
    request_number: u64,
    response_content: Option<&SectorVec>,
    hmac_client_key: [u8; 32],
) {
    let mut response = Vec::new();
    response.extend_from_slice(&MAGIC_NUMBER);
    response.extend_from_slice(&[0u8; Padding::SystemCommand as usize]);
    let status_code_raw: u8 = match status_code {
        StatusCode::Ok => 0,
        StatusCode::AuthFailure => 1,
        StatusCode::InvalidSectorIndex => 2,
    };
    response.extend_from_slice(&status_code_raw.to_be_bytes());
    response.push(0x40u8 + msg_type);
    response.extend_from_slice(&request_number.to_be_bytes());
    if let Some(content) = response_content {
        response.extend_from_slice(content.0.as_slice());
    }
    let mut hmac = Hmac::<sha2::Sha256>::new_from_slice(&hmac_client_key)
        .expect("HMAC creation error");
    hmac.update(&response);
    let hmac_bytes = hmac.finalize().into_bytes();
    response.extend_from_slice(&hmac_bytes);
    let mut writer = writer.lock().await;
    writer.write_all(&response).await.expect("Response sending error");
}