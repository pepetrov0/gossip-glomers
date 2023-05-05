use crate::maelstrom_protocol;
use tokio::io::{self, AsyncBufReadExt, BufReader};

mod sender;
pub mod service;

pub use sender::Output;
pub use sender::Sender;
use xtra::Actor;

type Actors = (xtra::Address<Sender>, xtra::Address<service::SeqKv>);

pub fn spawn_actors() -> Actors {
    let sender = Sender::new()
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);
    let seq_kv = service::SeqKv::new(sender.downgrade())
        .create(None)
        .spawn(&mut xtra::spawn::Tokio::Global);

    (sender, seq_kv)
}

pub async fn run_io<A, P>(node: xtra::Address<A>, (sender, seq_kv): Actors)
where
    A: xtra::Actor + xtra::Handler<maelstrom_protocol::Message<P>>,
    P: maelstrom_protocol::Payload + 'static,
{
    let mut input = BufReader::new(io::stdin()).lines();

    while let Ok(Some(line)) = input.next_line().await {
        // seq-kv
        if let Ok(message) =
            serde_json::from_str::<maelstrom_protocol::Message<service::SeqKvPayload>>(&line)
        {
            if message.src == "seq-kv" {
                seq_kv
                    .do_send(service::Response(message))
                    .expect("failed to send response to seq-kv service");
                continue;
            }
        }

        // default payload
        let message = serde_json::from_str::<maelstrom_protocol::Message<P>>(&line)
            .expect("failed to deserialize message");

        let node = node.downgrade();
        let sender = sender.downgrade();
        tokio::spawn(async move {
            if let Some(message) = node.send(message).await.ok().flatten() {
                sender
                    .do_send(Output(message))
                    .expect("could not send output to writer");
            }
        });
    }
}
