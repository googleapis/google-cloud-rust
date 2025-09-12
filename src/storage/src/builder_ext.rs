// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Extends [builder][crate::builder] with types that improve type safety and/or
//! ergonomics.

/// An extension trait for `RewriteObject` to provide a convenient way
/// to poll a rewrite operation until it is complete.
#[async_trait::async_trait]
pub trait RewriteObjectExt {
    /// Sends the request and polls the operation until it is complete.
    ///
    /// This helper function simplifies the process of handling a
    /// [StorageControl::rewrite_object][crate::client::StorageControl::rewrite_object]
    /// operation, which may require multiple requests to complete. It automatically
    /// handles the logic of sending the
    /// [rewrite_token][crate::generated::gapic::model::RewriteObjectRequest::rewrite_token]
    /// from one response in the next request.
    ///
    /// For more details on this loop, see the "Rewriting objects" section of the
    /// user guide:
    /// <https://googleapis.github.io/google-cloud-rust/storage/rewrite_object.html>
    ///
    /// # Example
    ///
    /// ```
    /// # use google_cloud_storage::client::StorageControl;
    /// # use google_cloud_storage::builder_ext::RewriteObjectExt;
    /// # async fn sample(client: &StorageControl) -> anyhow::Result<()> {
    /// const SOURCE_NAME: &str = "object-to-copy";
    /// const DEST_NAME: &str = "copied-object";
    /// let source_bucket_id = "source-bucket";
    /// let dest_bucket_id = "dest-bucket";
    /// let copied = client
    ///     .rewrite_object()
    ///     .set_source_bucket(format!("projects/_/buckets/{source_bucket_id}"))
    ///     .set_source_object(SOURCE_NAME)
    ///     .set_destination_bucket(format!("projects/_/buckets/{dest_bucket_id}"))
    ///     .set_destination_name(DEST_NAME)
    ///     .rewrite_until_done()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    async fn rewrite_until_done(self) -> crate::Result<crate::model::Object>;
}

#[async_trait::async_trait]
impl RewriteObjectExt for crate::builder::storage_control::RewriteObject {
    async fn rewrite_until_done(mut self) -> crate::Result<crate::model::Object> {
        loop {
            let resp = self.clone().send().await?;
            if resp.done {
                return Ok(resp
                    .resource
                    .expect("an object is always returned when the rewrite operation is done"));
            }
            self = self.set_rewrite_token(resp.rewrite_token);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::StorageControl;
    use crate::model::{Object, RewriteObjectRequest, RewriteResponse};
    use gax::options::RequestOptions;
    use gax::response::Response;

    mockall::mock! {
        #[derive(Debug)]
        StorageControl {}
        impl crate::stub::StorageControl for StorageControl {
            async fn rewrite_object( &self, _req: RewriteObjectRequest, _options: RequestOptions) -> gax::Result<Response<RewriteResponse>>;
        }
    }

    #[tokio::test]
    async fn test_rewrite_until_done() -> anyhow::Result<()> {
        let mut mock = MockStorageControl::new();
        let final_object = Object::new().set_name("final-object");

        let mut seq = mockall::Sequence::new();
        mock.expect_rewrite_object()
            .withf(|req: &RewriteObjectRequest, _| req.rewrite_token.is_empty())
            .times(1)
            .in_sequence(&mut seq)
            .returning(|_, _| {
                Ok(Response::from(
                    RewriteResponse::new()
                        .set_done(false)
                        .set_rewrite_token("token1"),
                ))
            });

        mock.expect_rewrite_object()
            .withf(|req: &RewriteObjectRequest, _| req.rewrite_token == "token1")
            .times(1)
            .in_sequence(&mut seq)
            .returning({
                let obj = final_object.clone();
                move |_, _| {
                    Ok(Response::from(
                        RewriteResponse::new()
                            .set_done(true)
                            .set_resource(obj.clone()),
                    ))
                }
            });

        let client = StorageControl::from_stub(mock);
        let result = client.rewrite_object().rewrite_until_done().await?;

        assert_eq!(result, final_object);
        Ok(())
    }
}
