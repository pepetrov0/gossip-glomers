use std::time::Duration;

use gossip_glomers::{
    actors::{
        self,
        service::{Request, SeqKv, SeqKvPayload},
    },
    maelstrom_protocol,
};
use serde::{Deserialize, Serialize};
use xtra::Actor;

struct GrowOnlyCounterNode {
    counter: usize,
    other_counters: usize,
    init: Option<maelstrom_protocol::InitPayload>,
    seq_kv: xtra::WeakAddress<SeqKv>,
}

#[async_trait::async_trait]
impl xtra::Actor for GrowOnlyCounterNode {
    async fn started(&mut self, ctx: &mut xtra::Context<Self>) {
        tokio::spawn(
            ctx.notify_interval(Duration::from_millis(1000), || FetchCounters)
                .expect("notify_interval failed"),
        );
    }
}

impl GrowOnlyCounterNode {
    pub fn new(seq_kv: xtra::WeakAddress<SeqKv>) -> Self {
        Self {
            counter: 0,
            other_counters: 0,
            init: None,
            seq_kv,
        }
    }
}

struct FetchCounters;

impl xtra::Message for FetchCounters {
    type Result = ();
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init(maelstrom_protocol::InitPayload),
    InitOk,
    Add {
        delta: usize,
    },
    AddOk,
    Read,
    ReadOk {
        value: usize,
    },
    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {}

#[async_trait::async_trait]
impl xtra::Handler<FetchCounters> for GrowOnlyCounterNode {
    async fn handle(&mut self, _: FetchCounters, _ctx: &mut xtra::Context<Self>) {
        let (node_id, others) = match self.init.as_ref().map(|v| {
            (
                v.node_id.clone(),
                v.node_ids
                    .iter()
                    .filter(|i| **i != v.node_id)
                    .map(|v| v.clone())
                    .collect::<Vec<_>>(),
            )
        }) {
            Some(v) => v,
            None => return,
        };

        // write
        let _ = self.seq_kv.do_send(Request(
            node_id.clone(),
            SeqKvPayload::Write {
                key: node_id.clone(),
                value: self.counter.to_string(),
            },
        ));

        // read others
        self.other_counters = 0;
        for o in &others {
            let Ok(Ok(resp)) = self
                .seq_kv
                .send(Request(
                    node_id.clone(),
                    actors::service::SeqKvPayload::Read { key: o.clone() },
                ))
                .await else {
                continue;
            };

            let Ok(SeqKvPayload::ReadOk { value }) = resp.await.map(|v| v.body.payload) else {
                continue;
            };

            self.other_counters += value.parse::<usize>().unwrap_or_default();
        }
    }
}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for GrowOnlyCounterNode {
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
            Payload::Add { delta } => {
                self.counter += delta;
                Some(message.make_response(Payload::AddOk))
            }
            Payload::Read => Some(message.make_response(Payload::ReadOk {
                value: self.counter + self.other_counters,
            })),
            Payload::AddOk | Payload::ReadOk { .. } | Payload::InitOk | Payload::Unknown => None,
        }
    }
}

#[tokio::main]
async fn main() {
    let actors = actors::spawn_actors();
    let node = GrowOnlyCounterNode::new(actors.1.downgrade())
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(node, actors).await;
}
