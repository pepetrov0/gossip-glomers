use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};

use crate::maelstrom_protocol;

impl<P> xtra::Message for maelstrom_protocol::Message<P>
where
    P: maelstrom_protocol::Payload + 'static,
{
    type Result = Option<maelstrom_protocol::Message<P>>;
}

pub async fn run_io<A, P>(addr: xtra::Address<A>)
where
    A: xtra::Actor + xtra::Handler<maelstrom_protocol::Message<P>>,
    P: maelstrom_protocol::Payload + 'static,
{
    let mut input = BufReader::new(io::stdin()).lines();
    let mut output = BufWriter::new(io::stdout());

    let mut id = 0;

    while let Ok(Some(line)) = input.next_line().await {
        let Ok(message) = serde_json::from_str::<maelstrom_protocol::Message<P>>(&line) else {
                continue;
            };

        if let Some(mut message) = addr.send(message).await.ok().flatten() {
            message.body.id = Some(id);
            id += 1;

            let Ok(message) = serde_json::to_vec(&message) else {
                    continue;
                };

            output
                .write_all(&message)
                .await
                .expect("failed to write to stdout");
            output
                .write_all(&[b'\n'])
                .await
                .expect("failed to write newline to stdout");
            output.flush().await.expect("failed to flush stdout");
        }
    }
}
