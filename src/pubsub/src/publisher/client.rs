use crate::{generated::gapic_internal::client, model::PubsubMessage};

pub struct Publisher {
    topic: String,
    inner: client::Publisher,
}

impl Publisher {
    // TODO: change to builder.
    pub async fn new(topic: String) -> crate::Result<Self> {
        let inner = client::Publisher::builder()
            .build()
            .await
            .map_err(crate::Error::io)?; // wrong error don't care
        Ok(Self { topic, inner })
    }

    pub async fn publish(
        &self,
        msg: PubsubMessage,
    ) -> crate::Result<crate::model::PublishResponse> {
        self.inner
            .publish()
            .set_messages([msg])
            .set_topic(self.topic.clone())
            .send()
            .await
    }
}
