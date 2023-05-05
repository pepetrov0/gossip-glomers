use super::{Output, Sender};
use crate::maelstrom_protocol;
use std::collections::HashMap;
use tokio::sync;

mod seq_kv;

pub use seq_kv::*;

pub struct Service<P> {
    pub name: &'static str,
    sender: xtra::WeakAddress<Sender>,
    pending_request: HashMap<usize, sync::oneshot::Sender<maelstrom_protocol::Message<P>>>,
}

impl<P: maelstrom_protocol::Payload + 'static> xtra::Actor for Service<P> {}

pub struct Request<P>(pub String, pub P);
pub struct Response<P>(pub maelstrom_protocol::Message<P>);

impl<P: maelstrom_protocol::Payload + 'static> xtra::Message for Request<P> {
    type Result =
        Result<sync::oneshot::Receiver<maelstrom_protocol::Message<P>>, xtra::Disconnected>;
}

impl<P: maelstrom_protocol::Payload + 'static> xtra::Message for Response<P> {
    type Result = ();
}

#[async_trait::async_trait]
impl<P: maelstrom_protocol::Payload + 'static> xtra::Handler<Request<P>> for Service<P> {
    async fn handle(
        &mut self,
        Request(from, payload): Request<P>,
        _ctx: &mut xtra::Context<Self>,
    ) -> Result<sync::oneshot::Receiver<maelstrom_protocol::Message<P>>, xtra::Disconnected> {
        let (tx, rx) = sync::oneshot::channel();

        let message = maelstrom_protocol::Message::new(from, self.name.to_string(), payload);
        let id = self.sender.send(Output(message)).await?;
        self.pending_request.insert(id, tx);

        Ok(rx)
    }
}

#[async_trait::async_trait]
impl<P: maelstrom_protocol::Payload + 'static> xtra::Handler<Response<P>> for Service<P> {
    async fn handle(&mut self, Response(message): Response<P>, _ctx: &mut xtra::Context<Self>) {
        let in_reply_to = match message.body.in_reply_to {
            Some(v) => v,
            None => return,
        };

        if let Some(tx) = self.pending_request.remove(&in_reply_to) {
            let _ = tx.send(message);
        }
    }
}
