use std::collections::{HashMap, HashSet};

use gossip_glomers::{actors, maelstrom_protocol};
use serde::{Deserialize, Serialize};
use xtra::Actor;

type Msg = (usize, usize);

/// last_offset, committed, msgs
type Queue = (usize, usize, Vec<Msg>);

struct SingleNodeKafkaNode {
    queues: HashMap<String, Queue>,
}

impl xtra::Actor for SingleNodeKafkaNode {}

impl SingleNodeKafkaNode {
    pub fn new() -> Self {
        Self {
            queues: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init(maelstrom_protocol::InitPayload),
    InitOk,
    Send {
        key: String,
        msg: usize,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<Msg>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: HashSet<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },

    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for SingleNodeKafkaNode {
    async fn handle(
        &mut self,
        message: maelstrom_protocol::Message<Payload>,
        _ctx: &mut xtra::Context<Self>,
    ) -> Option<maelstrom_protocol::Message<Payload>> {
        match &message.body.payload {
            Payload::Init(_) => Some(message.make_response(Payload::InitOk)),
            Payload::Send { key, msg } => {
                let (last_offset, _, msgs) = self.queues.entry(key.clone()).or_default();
                *last_offset += 1;
                let offset = *last_offset;

                msgs.push((offset, *msg));
                msgs.sort_by_key(|(o, _)| *o);

                Some(message.make_response(Payload::SendOk { offset }))
            }
            Payload::Poll { offsets } => {
                let msgs = offsets
                    .iter()
                    .map(|(key, offset)| {
                        let (_, _, msgs) = self.queues.get(key).cloned().unwrap_or_default();

                        let mut msgs: Vec<_> = msgs
                            .iter()
                            .filter(|(o, _)| *o >= *offset)
                            .cloned()
                            .collect();
                        msgs.sort_by_key(|(o, _)| *o);

                        (key.clone(), msgs)
                    })
                    .collect();
                Some(message.make_response(Payload::PollOk { msgs }))
            }
            Payload::CommitOffsets { offsets } => {
                for (k, o) in offsets.iter() {
                    let (_, committed_offset, _) = self.queues.entry(k.clone()).or_default();
                    *committed_offset = *o;
                }
                Some(message.make_response(Payload::CommitOffsetsOk))
            }
            Payload::ListCommittedOffsets { keys } => {
                let offsets = keys
                    .iter()
                    .map(|k| (k, self.queues.get(k).cloned().unwrap_or_default().1))
                    .map(|(k, o)| (k.clone(), o.clone()))
                    .collect();

                Some(message.make_response(Payload::ListCommittedOffsetsOk { offsets }))
            }
            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk
            | Payload::ListCommittedOffsetsOk { .. } => None,
            Payload::InitOk | Payload::Unknown => None,
        }
    }
}

#[tokio::main]
async fn main() {
    let actors = actors::spawn_actors();
    let node = SingleNodeKafkaNode::new()
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(node, actors).await;
}
