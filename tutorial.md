# Google Cloud Client Libraries for Rust

This guide explains how to use the Google Cloud Client Libraries for Rust to interact with Google Cloud services in your applications.

> **Note**: These libraries are currently in development and not recommended for production use.

## Before you begin

1. [Install Rust](https://www.rust-lang.org/tools/install)
2. [Set up a Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
3. [Install and initialize the Google Cloud CLI](https://cloud.google.com/sdk/docs/install)

## Installing the client libraries

Add the required dependencies to your `Cargo.toml`:

```toml
[dependencies]
google-cloud-secretmanager-v1 = { version = "0.1", features = ["unstable-stream"] }
tokio = { version = "1.0", features = ["macros", "rt-multi-thread"] }
```

## Authenticating

### Using Application Default Credentials (Recommended)

```rust
use google_cloud_secretmanager_v1::client::SecretManagerServiceClient;

async fn init_client() -> Result<SecretManagerServiceClient, Box<dyn std::error::Error>> {
    // Creates a client using Application Default Credentials
    let client = SecretManagerServiceClient::new().await?;
    Ok(client)
}
```

### Using Service Account Keys

```rust
use google_cloud_auth::credentials::CredentialsFile;
use google_cloud_secretmanager_v1::client::SecretManagerServiceClient;

async fn init_client_with_credentials() -> Result<SecretManagerServiceClient, Box<dyn std::error::Error>> {
    // Load credentials from a service account key file
    let credentials = CredentialsFile::new_from_file(
        "/path/to/service-account-key.json"
    ).await?;
    
    // Initialize client with explicit credentials
    let client = SecretManagerServiceClient::builder()
        .with_credentials(credentials)
        .build()
        .await?;
    
    Ok(client)
}
```

## Using the client libraries

### Basic operations

#### Listing secrets

```rust
use google_cloud_secretmanager_v1::client::SecretManagerServiceClient;

async fn list_secrets(project_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the client
    let client = SecretManagerServiceClient::new().await?;
    
    // Format the parent resource name
    let parent = format!("projects/{}", project_id);
    
    // List all secrets in the project
    let response = client
        .list_secrets()
        .set_parent(parent)
        .send()
        .await?;
    
    // Process the response
    for secret in response.secrets {
        println!("Secret: {}", secret.name);
    }
    
    Ok(())
}
```

### Working with long-running operations

Some Google Cloud operations may take a long time to complete. Here's how to handle them:

```rust
use google_cloud_speech_v2::client::SpeechClient;
use google_cloud_gax::retry::ExponentialBackoff;
use std::time::Duration;

async fn handle_long_running_operation(
    project_id: &str
) -> Result<(), Box<dyn std::error::Error>> {
    // Initialize client with custom polling policy
    let backoff = ExponentialBackoff::new(
        Duration::from_secs(1),    // Initial delay
        Duration::from_secs(60),   // Maximum delay
        1.5,                       // Multiplier
    );
    
    let client = SpeechClient::builder()
        .with_polling_backoff_policy(backoff)
        .build()
        .await?;
    
    // Start a long-running operation
    let operation = client
        .create_recognizer()
        .set_parent(format!("projects/{}/locations/global", project_id))
        .set_recognizer_id("my-recognizer")
        .send()
        .await?;
    
    // Wait for completion
    let result = operation.await_complete().await?;
    
    Ok(())
}
```

### Error handling

Handle errors appropriately to ensure your application is robust:

```rust
use google_cloud_gax::error::{Code, ServiceError};

async fn robust_error_handling(
    project_id: &str
) -> Result<(), Box<dyn std::error::Error>> {
    let client = match SecretManagerServiceClient::new().await {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to initialize client: {}", e);
            return Err(e.into());
        }
    };

    let secret_name = format!("projects/{}/secrets/my-secret", project_id);
    
    match client.get_secret().set_name(secret_name).send().await {
        Ok(secret) => {
            println!("Retrieved secret: {}", secret.name);
            Ok(())
        }
        Err(e) => {
            if let Some(status) = e.downcast_ref::<ServiceError>() {
                match status.code() {
                    Code::NotFound => {
                        println!("Secret not found");
                        Ok(())
                    }
                    Code::PermissionDenied => {
                        eprintln!("Permission denied");
                        Err(e)
                    }
                    _ => {
                        eprintln!("Service error: {}", status);
                        Err(e)
                    }
                }
            } else {
                eprintln!("Unknown error: {}", e);
                Err(e)
            }
        }
    }
}
```

## Troubleshooting

### Common issues

1. **Authentication errors**
   - Ensure you've run `gcloud auth application-default login`
   - Verify your service account has the necessary permissions

2. **API not enabled**
   - Enable the required APIs in your project:
     ```bash
     gcloud services enable secretmanager.googleapis.com
     ```

3. **Permission denied**
   - Grant the necessary IAM roles to your account:
     ```bash
     gcloud projects add-iam-policy-binding PROJECT_ID \
         --member=user:EMAIL \
         --role=roles/secretmanager.admin
     ```

## Next steps

- Explore the [Google Cloud documentation](https://cloud.google.com/docs)
- View the [API reference](https://docs.rs/google-cloud-secretmanager-v1)
- Check out [sample applications](https://github.com/googleapis/google-cloud-rust)

## Additional resources

- [Rust Programming Language](https://www.rust-lang.org/)
- [Google Cloud Platform Console](https://console.cloud.google.com/)
- [Google Cloud CLI documentation](https://cloud.google.com/sdk/docs)
