mod domain;

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

pub async fn run_register_process(config: Configuration) {
    unimplemented!()
}

pub mod atomic_register_public {
    use crate::{
        ClientRegisterCommand, OperationSuccess, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
        /// Handle a client command. After the command is completed, we expect
        /// callback to be called. Note that completion of client command happens after
        /// delivery of multiple system commands to the register, as the algorithm specifies.
        ///
        /// This function corresponds to the handlers of Read and Write events in the
        /// (N,N)-AtomicRegister algorithm.
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
                    + Sync,
            >,
        );

        /// Handle a system command.
        ///
        /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
        /// and ACK messages in the (N,N)-AtomicRegister algorithm.
        async fn system_command(&mut self, cmd: SystemRegisterCommand);
    }

    /// Idents are numbered starting at 1 (up to the number of processes in the system).
    /// Communication with other processes of the system is to be done by register_client.
    /// And sectors must be stored in the sectors_manager instance.
    ///
    /// This function corresponds to the handlers of Init and Recovery events in the
    /// (N,N)-AtomicRegister algorithm.
    pub async fn build_atomic_register(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    ) -> Box<dyn AtomicRegister> {
        unimplemented!()
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        unimplemented!()
    }
}

pub mod transfer_public {
    use crate::{RegisterCommand,
        ClientRegisterCommand, ClientRegisterCommandContent,
        SystemRegisterCommand,
        MAGIC_NUMBER};
    use std::io::Error;
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
    use hmac::{Hmac, Mac};

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), Error> {
        unimplemented!()
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        match cmd {
            RegisterCommand::Client(client_cmd) => {
                serialize_client_command(client_cmd, writer, hmac_key).await
            }
            RegisterCommand::System(system_cmd) => {
                serialize_system_command(system_cmd, writer, hmac_key).await
            }
        }
    }

    async fn serialize_client_command(
        cmd: &ClientRegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        let mut serialized: Vec<u8> = Vec::new();
        serialized.extend_from_slice(&MAGIC_NUMBER);
        serialized.extend_from_slice(&[0u8; 3]);
        let mut content = Vec::new();
        match cmd.content {
            ClientRegisterCommandContent::Read => {
                serialized.push(1);
            }
            ClientRegisterCommandContent::Write { ref data } => {
                serialized.push(2);
                content.extend_from_slice(data.0.as_slice());
            }
        }
        // HEADER END
        serialized.extend_from_slice(
            cmd.header.request_identifier.to_be_bytes().as_ref());
        // IDENTIFIER END
        serialized.extend_from_slice(
            cmd.header.sector_idx.to_be_bytes().as_ref());
        // SECTOR IDX END
        serialized.extend_from_slice(content.as_slice());
        // CONTENT END
        let mut hmac = Hmac::<sha2::Sha256>::new_from_slice(hmac_key)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "HMAC error"))?;
        hmac.update(&serialized);
        let hmac_bytes = hmac.finalize().into_bytes();
        serialized.extend_from_slice(&hmac_bytes);
        // HMAC END
        // MESSAGE END

        writer.write_all(serialized.as_slice()).await?;
        return Ok(());
    }
    
    async fn serialize_system_command(
        cmd: &SystemRegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), Error> {
        unimplemented!()
    }
}

pub mod register_client_public {
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

    #[async_trait::async_trait]
    /// We do not need any public implementation of this trait. It is there for use
    /// in AtomicRegister. In our opinion it is a safe bet to say some structure of
    /// this kind must appear in your solution.
    pub trait RegisterClient: core::marker::Send + core::marker::Sync {
        /// Sends a system message to a single process.
        async fn send(&self, msg: Send);

        /// Broadcasts a system message to all processes in the system, including self.
        async fn broadcast(&self, msg: Broadcast);
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
