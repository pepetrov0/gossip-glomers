use crate::maelstrom_protocol;
use tokio::io::{self, AsyncWriteExt, BufWriter, Stdout};

pub struct Sender {
    id: usize,
    inner: BufWriter<Stdout>,
}

impl xtra::Actor for Sender {}

impl Sender {
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
    type Result = usize;
}

#[async_trait::async_trait]
impl<P> xtra::Handler<Output<P>> for Sender
where
    P: maelstrom_protocol::Payload + 'static,
{
    async fn handle(
        &mut self,
        Output(mut message): Output<P>,
        _ctx: &mut xtra::Context<Self>,
    ) -> usize {
        let id = self.id;
        message.body.id = Some(id);
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

        id
    }
}
