use gcp_sdk_auth::{credentials::service_account_credential, token::TokenProvider};  // Assuming the path is correct, replace with actual package and module name
use tokio::runtime::Runtime;
use rustls::crypto::CryptoProvider;


fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = CryptoProvider::install_default(rustls::crypto::aws_lc_rs::default_provider()); // Or another provider
    let rt = Runtime::new()?;
    let token_provider = service_account_credential::ServiceAccountTokenProvider {
        file_path: "/usr/local/google/home/harkamalj/Desktop/sa3.json".to_string(),
    };
    println!("{:?}", rt.block_on(token_provider.get_token())?);
    Ok(())
}