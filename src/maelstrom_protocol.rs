use std::collections::HashSet;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<P> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,

    #[serde(flatten)]
    pub payload: P,
}

pub trait Payload: std::fmt::Debug + Sized + Send + Clone + Serialize + DeserializeOwned {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InitPayload {
    pub node_id: String,
    pub node_ids: HashSet<String>,
}

impl<P> Message<P> {
    pub fn new(src: String, dst: String, payload: P) -> Self {
        Self {
            src,
            dst,
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }

    pub fn make_response(&self, payload: P) -> Self {
        Self {
            src: self.dst.clone(),
            dst: self.src.clone(),
            body: Body {
                id: None,
                in_reply_to: self.body.id,
                payload,
            },
        }
    }
}

impl<P> xtra::Message for Message<P>
where
    P: Payload + 'static,
{
    type Result = Option<Message<P>>;
}
