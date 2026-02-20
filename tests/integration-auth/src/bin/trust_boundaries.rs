use google_cloud_auth::credentials::{Builder, CacheableResource};
use httptest::http::Extensions;
use scoped_env::ScopedEnv;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Enable trust boundaries
    let _env = ScopedEnv::set("GOOGLE_AUTH_ENABLE_TRUST_BOUNDARIES", "true");

    let _env = ScopedEnv::set(
        "GOOGLE_APPLICATION_CREDENTIALS",
        //"/usr/local/google/home/aviebrantz/keys/cicpclientproj-221b8675880e-impersonated.json",
        "/usr/local/google/home/aviebrantz/keys/cicpclientproj-221b8675880e.json",
    );

    println!("Building Credentials...");
    // Will use ADC with the environment variable set
    let creds = Builder::default().build()?;
    println!("Credentials built: {:?}", creds);

    for _ in 0..10 {
        let cached_headers = creds.headers(Extensions::new()).await?;
        let headers = match cached_headers {
            CacheableResource::New { data, .. } => data,
            CacheableResource::NotModified => unreachable!("should always get new headers"),
        };
        let token = headers.get("Authorization");
        println!("Token: {:?}", token);
        let locations = headers.get("x-goog-allowed-locations");
        println!("Locations: {:?}", locations);
        if locations.is_some() {
            println!("Locations found, breaking");
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    }

    Ok(())
}
