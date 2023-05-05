use super::Service;
use crate::{actors::Sender, maelstrom_protocol};
use serde::{Deserialize, Serialize};

pub type SeqKv = Service<SeqKvPayload>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum SeqKvPayload {
    Read {
        key: String,
    },
    ReadOk {
        value: String,
    },
    Write {
        key: String,
        value: String,
    },
    WriteOk,
    Cas {
        key: String,
        from: String,
        to: String,
    },
    CasOk,
    #[serde(other)]
    Uknown,
}

impl maelstrom_protocol::Payload for SeqKvPayload {}

impl SeqKv {
    pub fn new(sender: xtra::WeakAddress<Sender>) -> Self {
        Self {
            name: "seq-kv",
            sender,
            pending_request: Default::default(),
        }
    }
}
