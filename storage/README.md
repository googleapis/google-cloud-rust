# storage

A Google Cloud Storage Library generated from discovery document.

## Examples

```rust
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::model::Object;
    use super::*;

    #[tokio::main]
    #[test]
    async fn test_client_download() {
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .get("codyoss-workspace", "rust-test-1.txt")
            .execute()
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());

        let resp = client
            .objects_service()
            .get("codyoss-workspace", "rust-test-1.txt")
            .download()
            .await
            .unwrap();
        println!("{}", String::from_utf8(resp).unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_upload() {
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .insert("codyoss-workspace", Default::default())
            .name("rust-test-1.txt")
            .media_content_type("text/plain; charset=utf-8")
            .upload("this is a test from rust")
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_upload_file() {
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .insert("codyoss-workspace", Default::default())
            .name("rust-file-test-1.txt")
            .media_content_type("text/plain; charset=utf-8")
            .upload(BytesReader::from_path(
                "/Users/codyoss/oss/google-cloud-rust/storage/upload-me.txt",
            ))
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_update_metadata() {
        let client = Client::new().await.unwrap();
        let mut map: HashMap<String, String> = HashMap::new();
        map.insert("foo".into(), "bar".into());
        let resp = client
            .objects_service()
            .patch(
                "codyoss-workspace",
                "rust-file-test-1.txt",
                Object::builder().metadata(map).build(),
            )
            .execute()
            .await
            .unwrap();
        println!("{:?}", resp.metadata.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_list_bucket() {
        // no native nice iterator support, yet. Should be possible to make the
        // value returned directly impl support w/o breaking change. From looking
        // quickly this only applies to buckets, objects, and hmack keys.
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .list("codyoss-workspace")
            .execute()
            .await
            .unwrap();
        for item in resp.items.unwrap() {
            println!("{}", item.name.unwrap())
        }
    }
}
```
