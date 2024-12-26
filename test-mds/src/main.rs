// use auth::credentials::mds_credential;
// use reqwest::Client;

fn main() {
    println!("Hello, world!");
}

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let token_provider = mds_credential::MDSAccessTokenProvider {
//         service_account_email: "default".to_string(),
//         scopes: None,
//     };
//     let client = Client::new();
//     let service_account_info = token_provider
//         .get_service_account_info(&client, Option::None)
//         .await;
//     // Process service_account_info
//     println!("{:?}", service_account_info);
//     Ok(())
// }
