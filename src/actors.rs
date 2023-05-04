use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter, Stdout};

use crate::maelstrom_protocol;

pub struct Writer {
    id: usize,
    inner: BufWriter<Stdout>,
}

impl xtra::Actor for Writer {}

impl Writer {
    pub fn new() -> Self {
        Self {
            id: 0,
            inner: BufWriter::new(io::stdout()),
        }
    }
}

pub struct Output<P>(pub maelstrom_protocol::Message<P>);

impl<P> xtra::Message for Output<P>
where
    P: Send + 'static,
{
    type Result = ();
}

#[async_trait::async_trait]
impl<P> xtra::Handler<Output<P>> for Writer
where
    P: maelstrom_protocol::Payload + 'static,
{
    async fn handle(&mut self, Output(mut message): Output<P>, _ctx: &mut xtra::Context<Self>) {
        message.body.id = Some(self.id);
        self.id += 1;

        let buf = serde_json::to_vec(&message).expect("failed to serialize message");

        self.inner
            .write_all(&buf)
            .await
            .expect("failed to write to stdout");
        self.inner
            .write_all(&[b'\n'])
            .await
            .expect("failed to write newline to stdout");
        self.inner.flush().await.expect("failed to flush stdout");

        eprintln!("sent: {:?}", message);
    }
}

pub async fn run_io<A, P>(addr: xtra::Address<A>, writer: xtra::Address<Writer>)
where
    A: xtra::Actor + xtra::Handler<maelstrom_protocol::Message<P>>,
    P: maelstrom_protocol::Payload + 'static,
{
    let mut input = BufReader::new(io::stdin()).lines();

    while let Ok(Some(line)) = input.next_line().await {
        let message = serde_json::from_str::<maelstrom_protocol::Message<P>>(&line)
            .expect("failed to deserialize message");

        eprintln!("received: {:?}", message);

        if let Some(message) = addr.send(message).await.ok().flatten() {
            writer
                .do_send(Output(message))
                .expect("could not send output to writer");
        }
    }
}
