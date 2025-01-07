use gcp_sdk_auth::{credentials::service_account_credential, token::TokenProvider};  // Assuming the path is correct, replace with actual package and module name
use tokio::runtime::Runtime;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rt = Runtime::new()?;
    let mut token_provider = service_account_credential::ServiceAccountTokenProvider {
        file_path: "/usr/local/google/home/harkamalj/Desktop/sa3.json".to_string(),
    };
    println!("{:?}", rt.block_on(token_provider.get_token())?);
    Ok(())
}