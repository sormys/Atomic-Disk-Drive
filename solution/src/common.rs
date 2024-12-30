use std::{future::Future, pin::Pin};
use tokio::sync::Semaphore;

use crate::domain::*;

pub(crate) type ClientCallback = Box<
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        >;


pub(crate) type InternalCommand = (SectorIdx, RegisterCommand, bool, Option<ClientCallback>);

// No more than 1024 file descriptors are allowed per process.
pub(crate) static FD_SEMAPHORE: Semaphore = Semaphore::const_new(1024);
