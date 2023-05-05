use std::collections::{HashMap, HashSet};

use gossip_glomers::{actors, maelstrom_protocol};
use serde::{Deserialize, Serialize};
use xtra::Actor;

struct FaultTolerantBroadcastNode {
    init: Option<maelstrom_protocol::InitPayload>,
    messages: HashSet<usize>,
    neighbours: HashSet<String>,
    sender: xtra::WeakAddress<actors::Sender>,
}

impl FaultTolerantBroadcastNode {
    pub fn new(sender: xtra::WeakAddress<actors::Sender>) -> Self {
        Self {
            init: None,
            messages: HashSet::new(),
            neighbours: HashSet::new(),
            sender,
        }
    }
}

#[async_trait::async_trait]
impl xtra::Actor for FaultTolerantBroadcastNode {
    async fn started(&mut self, ctx: &mut xtra::Context<Self>) {
        tokio::spawn(
            ctx.notify_interval(std::time::Duration::from_millis(1000), || Gossip)
                .expect("notify_interval failed"),
        );
    }
}

struct Gossip;

impl xtra::Message for Gossip {
    type Result = ();
}

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
    Gossip {
        messages: HashSet<usize>,
    },
    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {}

#[async_trait::async_trait]
impl xtra::Handler<Gossip> for FaultTolerantBroadcastNode {
    async fn handle(&mut self, _: Gossip, _ctx: &mut xtra::Context<Self>) {
        let Some(id) = self.init.as_ref().map(|v| v.node_id.clone()) else {
            return;
        };

        for n in &self.neighbours {
            let message = maelstrom_protocol::Message::new(
                id.clone(),
                n.clone(),
                Payload::Gossip {
                    messages: self.messages.clone(),
                },
            );

            self.sender
                .do_send(actors::Output(message))
                .expect("could not send output to sender");
        }
    }
}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for FaultTolerantBroadcastNode {
    async fn handle(
        &mut self,
        message: maelstrom_protocol::Message<Payload>,
        _ctx: &mut xtra::Context<Self>,
    ) -> Option<maelstrom_protocol::Message<Payload>> {
        match &message.body.payload {
            Payload::Init(init) => {
                self.init.replace(init.clone());
                Some(message.make_response(Payload::InitOk))
            }
            Payload::Broadcast { message: m } => {
                self.messages.insert(*m);

                Some(message.make_response(Payload::BroadcastOk))
            }
            Payload::Read => Some(message.make_response(Payload::ReadOk {
                messages: self.messages.clone(),
            })),
            Payload::Topology { topology } => {
                if let Some(id) = self.init.as_ref().map(|v| v.node_id.clone()) {
                    self.neighbours.extend(topology[&id].clone());
                }

                Some(message.make_response(Payload::TopologyOk))
            }

            Payload::Gossip { messages } => {
                self.messages.extend(messages.clone());

                None
            }

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
    let sender = actors::Sender::new()
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    let node = FaultTolerantBroadcastNode::new(sender.downgrade())
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(node, sender).await;
}
