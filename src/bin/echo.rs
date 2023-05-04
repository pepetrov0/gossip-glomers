use gossip_glomers::{actors, maelstrom_protocol};
use serde::{Deserialize, Serialize};
use xtra::Actor;

struct EchoNode;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
enum Payload {
    Init(maelstrom_protocol::InitPayload),
    InitOk,
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
    #[serde(other)]
    Unknown,
}

impl maelstrom_protocol::Payload for Payload {
    fn into_init(self) -> Option<maelstrom_protocol::InitPayload> {
        match self {
            Payload::Init(v) => Some(v),
            _ => None,
        }
    }

    fn make_init_ok() -> Self {
        Self::InitOk
    }
}

impl xtra::Actor for EchoNode {}

#[async_trait::async_trait]
impl xtra::Handler<maelstrom_protocol::Message<Payload>> for EchoNode {
    async fn handle(
        &mut self,
        message: maelstrom_protocol::Message<Payload>,
        _ctx: &mut xtra::Context<Self>,
    ) -> Option<maelstrom_protocol::Message<Payload>> {
        match &message.body.payload {
            Payload::Init(_) => Some(message.make_response(Payload::InitOk)),
            Payload::Echo { echo } => {
                Some(message.make_response(Payload::EchoOk { echo: echo.clone() }))
            }
            Payload::EchoOk { echo: _ } | Payload::InitOk | Payload::Unknown => None,
        }
    }
}

#[tokio::main]
async fn main() {
    let addr = EchoNode.create(None).spawn(&mut xtra::spawn::Tokio::Global);
    actors::run_io(addr).await;
}
