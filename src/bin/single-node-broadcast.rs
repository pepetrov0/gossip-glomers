use std::collections::{HashMap, HashSet};

use gossip_glomers::{actors, maelstrom_protocol};
use serde::{Deserialize, Serialize};
use xtra::Actor;

#[derive(Default)]
struct SingleNodeBroadcastNode {
    messages: HashSet<usize>,
}

impl xtra::Actor for SingleNodeBroadcastNode {}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init(maelstrom_protocol::InitPayload),
    InitOk,
    Broadcast {
        message: usize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<usize>,
    },
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
    TopologyOk,
    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for SingleNodeBroadcastNode {
    async fn handle(
        &mut self,
        message: maelstrom_protocol::Message<Payload>,
        _ctx: &mut xtra::Context<Self>,
    ) -> Option<maelstrom_protocol::Message<Payload>> {
        match &message.body.payload {
            Payload::Init(_) => Some(message.make_response(Payload::InitOk)),
            Payload::Broadcast { message: m } => {
                self.messages.insert(*m);

                Some(message.make_response(Payload::BroadcastOk))
            }
            Payload::Read => Some(message.make_response(Payload::ReadOk {
                messages: self.messages.clone(),
            })),
            Payload::Topology { .. } => Some(message.make_response(Payload::TopologyOk)),

            Payload::InitOk
            | Payload::BroadcastOk
            | Payload::ReadOk { .. }
            | Payload::TopologyOk
            | Payload::Unknown => None,
        }
    }
}

#[tokio::main]
async fn main() {
    let actors = actors::spawn_actors();
    let node = SingleNodeBroadcastNode::default()
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(node, actors).await;
}
