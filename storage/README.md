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

        let mut buf: Vec<u8> = vec![];
        let resp = client
            .objects_service()
            .get("codyoss-workspace", "rust-test-1.txt")
            .download(&mut buf)
            .await
            .unwrap();
        println!("{}", String::from_utf8(buf).unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_upload() {
        let client = Client::new().await.unwrap();
        let mut bytes: &[u8] = "test 72".as_bytes();
        let resp = client
            .objects_service()
            .insert("codyoss-workspace", Default::default())
            .name("rust-test-72.txt")
            .media_content_type("text/plain; charset=utf-8")
            .upload(&mut bytes)
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_client_upload_file() {
        let mut file =
            tokio::fs::File::open("/Users/codyoss/oss/google-cloud-rust/storage/upload-me.txt")
                .await
                .unwrap();
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .insert("codyoss-workspace", Default::default())
            .name("rust-file-test-73.txt")
            .media_content_type("text/plain; charset=utf-8")
            .upload(&mut file)
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
