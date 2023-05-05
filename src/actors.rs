use tokio::io::{self, AsyncBufReadExt, BufReader};

use crate::maelstrom_protocol;

mod sender;

pub use sender::Output;
pub use sender::Sender;

pub async fn run_io<A, P>(node: xtra::Address<A>, sender: xtra::Address<Sender>)
where
    A: xtra::Actor + xtra::Handler<maelstrom_protocol::Message<P>>,
    P: maelstrom_protocol::Payload + 'static,
{
    let mut input = BufReader::new(io::stdin()).lines();

    while let Ok(Some(line)) = input.next_line().await {
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
