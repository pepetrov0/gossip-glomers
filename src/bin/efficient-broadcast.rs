use std::collections::{HashMap, HashSet};

use gossip_glomers::{actors, maelstrom_protocol};
use serde::{Deserialize, Serialize};
use xtra::Actor;

struct EfficientBroadcastNode {
    sender: xtra::WeakAddress<actors::Sender>,
    init: Option<maelstrom_protocol::InitPayload>,
    neighbours: HashSet<String>,
    messages: HashSet<usize>,
    unknown_messages: HashMap<String, HashSet<usize>>,
}

impl EfficientBroadcastNode {
    pub fn new(sender: xtra::WeakAddress<actors::Sender>) -> Self {
        Self {
            sender,
            init: None,
            neighbours: HashSet::new(),
            messages: HashSet::new(),
            unknown_messages: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl xtra::Actor for EfficientBroadcastNode {
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
        recipients: HashSet<String>,
        messages: HashSet<usize>,
    },
    GossipOk {
        messages: HashSet<usize>,
    },
    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {}

#[async_trait::async_trait]
impl xtra::Handler<Gossip> for EfficientBroadcastNode {
    async fn handle(&mut self, _: Gossip, _ctx: &mut xtra::Context<Self>) {
        let Some(id) = self.init.as_ref().map(|v| v.node_id.clone()) else {
            return;
        };

        let mut unknown_messages = HashSet::new();

        self.neighbours.iter().for_each(|n| {
            unknown_messages.extend(self.unknown_messages.get(n).cloned().unwrap_or_default())
        });

        if unknown_messages.is_empty() {
            return;
        }

        for n in &self.neighbours {
            let message = maelstrom_protocol::Message::new(
                id.clone(),
                n.clone(),
                Payload::Gossip {
                    recipients: self.neighbours.clone(),
                    messages: unknown_messages.clone(),
                },
            );

            self.sender
                .do_send(actors::Output(message))
                .expect("could not send output to sender");
        }
    }
}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for EfficientBroadcastNode {
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

                for n in &self.neighbours {
                    self.unknown_messages
                        .entry(n.clone())
                        .or_default()
                        .insert(*m);
                }

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
            Payload::Gossip {
                recipients,
                messages,
            } => match messages.is_empty() {
                true => None,
                false => {
                    self.messages.extend(messages.clone());

                    for n in self
                        .neighbours
                        .iter()
                        .filter(|n| !recipients.contains(*n) && **n != message.src)
                    {
                        self.unknown_messages
                            .entry(n.clone())
                            .or_default()
                            .extend(messages.clone());
                    }

                    Some(message.make_response(Payload::GossipOk {
                        messages: messages.clone(),
                    }))
                }
            },
            Payload::GossipOk { messages } => {
                if let Some(unknown_messages) = self.unknown_messages.get_mut(&message.src) {
                    unknown_messages.retain(|m| !messages.contains(m));
                }
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
    let node = EfficientBroadcastNode::new(sender.downgrade())
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(node, sender).await;
}
