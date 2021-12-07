# storage

A Google Cloud Storage Library generated from discovery document.

## Examples

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::main]
    #[test]
    async fn test_client_download() {
        let client = Client::new().await.unwrap();
        let resp = client
            .objects_service()
            .get()
            .bucket("codyoss-workspace")
            .object("test.txt")
            .execute()
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());

        let resp = client
            .objects_service()
            .get()
            .bucket("codyoss-workspace")
            .object("test.txt")
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
            .insert(Default::default())
            .bucket("codyoss-workspace")
            .name("rust-test-1.txt")
            .upload(
                "this is a test from rust".into(),
                "text/plain; charset=utf-8",
            )
            .await
            .unwrap();
        println!("{}", resp.updated.unwrap());
    }
}
```
