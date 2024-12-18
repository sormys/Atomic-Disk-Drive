use crate::{
    SectorVec,
    ClientRegisterCommand, ClientRegisterCommandContent,
    SystemRegisterCommand, SystemCommandHeader,
        SystemRegisterCommandContent,
    MAGIC_NUMBER};
use std::io::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use hmac::{Hmac, Mac};

const DATA_SIZE: usize = 4096;

#[repr(usize)]
enum Padding {
    ClientCommand = 3,
    SystemCommand = 2,
    SystemCommandContent = 4,
}

// DESERIALIZATION



// SERIALIZATION

pub async fn serialize_client_command(
    cmd: &ClientRegisterCommand,
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
    match cmd.content {
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
        &cmd.header.request_identifier.to_be_bytes());
    // Sector index
    serialized.extend_from_slice(&cmd.header.sector_idx.to_be_bytes());
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

pub fn append_system_command_content(
    store_vector: &mut Vec<u8>, timestamp: &u64, write_rank: &u8, content: &SectorVec
) -> Result<(), Error> {
    // Timestamp
    store_vector.extend_from_slice(&timestamp.to_be_bytes());
    // Padding
    store_vector.extend_from_slice(&[0u8; Padding::SystemCommandContent as usize]);
    // Value wr
    store_vector.push(*write_rank);
    // Sector data
    // Return error if sector data is not 4096 bytes
    if content.0.len() != DATA_SIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other, "Sector data is not 4096 bytes"));
    }
    store_vector.extend_from_slice(content.0.as_slice());
    return Ok(());
}

pub async fn serialize_system_command(
    cmd: &SystemRegisterCommand,
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
    } = cmd.header;
    // Unpack content
    let mut content: Vec<u8> = Vec::new();
    let message_type: u8 = match &cmd.content {
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
    serialized.extend_from_slice(msg_ident.as_bytes());
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
