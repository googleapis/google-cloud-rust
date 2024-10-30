use std::sync::Arc;

pub mod model;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientRef>,
}

#[derive(Debug)]
struct ClientRef {
    http_client: reqwest::Client,
    token: String,
}

impl Client {
    pub fn new(tok: String) -> Self {
        let client = reqwest::Client::builder().build().unwrap();
        let inner = ClientRef {
            http_client: client,
            token: tok,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Secret Manager Service
    /// 
    ///  Manages secrets and operations using those secrets. Implements a REST
    ///  model with the following objects:
    /// 
    ///  * [Secret][google.cloud.secretmanager.v1.Secret]
    ///  * [SecretVersion][google.cloud.secretmanager.v1.SecretVersion]
    pub fn secret_manager_service(&self) -> SecretManagerService {
        SecretManagerService {
            client: self.clone(),
            base_path: "https://secretmanager.googleapis.com/".to_string(),
        }
    }
}

/// Secret Manager Service
/// 
///  Manages secrets and operations using those secrets. Implements a REST
///  model with the following objects:
/// 
///  * [Secret][google.cloud.secretmanager.v1.Secret]
///  * [SecretVersion][google.cloud.secretmanager.v1.SecretVersion]
#[derive(Debug)]
pub struct SecretManagerService {
    client: Client,
    base_path: String,
}

impl SecretManagerService {

    /// Creates a new [Secret][google.cloud.secretmanager.v1.Secret] containing no
    ///  [SecretVersions][google.cloud.secretmanager.v1.SecretVersion].
    pub async fn create_secret(&self, req: model::CreateSecretRequest) -> Result<model::Secret, Box<dyn std::error::Error>> {
        let client = self.client.inner.clone();
        let res = client.http_client
            .post(format!(
               "{}/v1/{}/secrets",
               self.base_path,
               req.parent,
            ))
            .query(&[("alt", "json")])
            .query(&[("secretId", req.secret_id.as_str())])
            .bearer_auth(&client.token)
            .json(&req.secret)
            .send().await?;
        if !res.status().is_success() {
            return Err("sorry the api you are looking for is not available, please try again".into());
        }
        res.json::<model::Secret>.await?
    }

    /// Gets metadata for a given [Secret][google.cloud.secretmanager.v1.Secret].
    pub async fn get_secret(&self, req: model::GetSecretRequest) -> Result<model::Secret, Box<dyn std::error::Error>> {
        let client = self.client.inner.clone();
        let res = client.http_client
            .get(format!(
               "{}/v1/{}",
               self.base_path,
               req.name,
            ))
            .query(&[("alt", "json")])
            .query(&[("name", req.name.as_str())])
            .bearer_auth(&client.token)
            .send().await?;
        if !res.status().is_success() {
            return Err("sorry the api you are looking for is not available, please try again".into());
        }
        res.json::<model::Secret>.await?
    }
}
