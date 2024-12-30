use std::{future::Future, pin::Pin};

use crate::domain::*;

pub(crate) type ClientCallback = Box<
    dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync
        >;


pub(crate) type InternalCommand = (SectorIdx, RegisterCommand, bool, Option<ClientCallback>);

