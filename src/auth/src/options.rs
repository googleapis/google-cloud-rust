use std::time::Duration;

#[derive(Debug, Clone)]
pub enum CredentialsSource {
    CredentialsFile(String),
    CredentialsJson(String),
    ForceUseMds,
}

#[derive(Debug, Clone)]
pub enum ScopesOrAudience {
    Scopes(Vec<String>),
    Audience(String),
}

#[derive(Debug)]
pub struct AccessTokenCredentialOptions {
    pub scopes_or_audience: Option<ScopesOrAudience>,
    pub credentials_source: Option<CredentialsSource>,
    pub subject: Option<String>, 
    pub early_token_refresh: Option<Duration>,
    pub disable_async_refresh: bool,
    pub token_url: Option<String>,
    pub sts_audience: Option<String>,
    pub universe_domain: Option<String>,
}


pub struct AccessTokenCredentialOptionsBuilder {
    pub scopes_or_audience: Option<ScopesOrAudience>,
    pub credentials_source: Option<CredentialsSource>,
    pub subject: Option<String>, 
    pub early_token_refresh: Option<Duration>,
    pub disable_async_refresh: bool,
    pub token_url: Option<String>,
    pub sts_audience: Option<String>,
    pub universe_domain: Option<String>,
}

impl AccessTokenCredentialOptionsBuilder {
    pub fn new() -> Self {
        AccessTokenCredentialOptionsBuilder {
            scopes_or_audience: None,
            credentials_source: None,
            subject: None,
            early_token_refresh: None,
            disable_async_refresh: false,
            token_url: None,
            sts_audience: None,
            universe_domain: None,
        }
    }

    pub fn scopes(&mut self, scopes: Vec<String>) -> &mut Self {
        self.scopes_or_audience = Some(ScopesOrAudience::Scopes(scopes));
        self
    }

    pub fn credentials_file(&mut self, file_path: String) -> &mut Self {
        self.credentials_source = Some(CredentialsSource::CredentialsFile(file_path));
        self
    }

    pub fn force_use_mds(&mut self) -> &mut Self {
        self.credentials_source = Some(CredentialsSource::ForceUseMds);
        self
    }

    pub fn build(&self) -> Result<AccessTokenCredentialOptions, anyhow::Error> {
        Ok(
            AccessTokenCredentialOptions {
                scopes_or_audience: self.scopes_or_audience.clone(),
                credentials_source: self.credentials_source.clone(),
                subject: self.subject.clone(),
                early_token_refresh: self.early_token_refresh.clone(),
                disable_async_refresh: self.disable_async_refresh,
                token_url: self.token_url.clone(),
                sts_audience: self.sts_audience.clone(),
                universe_domain: self.universe_domain.clone(),
        })
    }
}

pub struct AccessTokenCredentialOptions2 {
    pub credentials_source: Option<CredentialsSource>,
    // pub user_credential_options: Option<UserCredentialOptions>,
    // pub service_account_options: Option<ServiceCredentialOption>,
    pub credential_type_based_options: Option<CredentialTypeBasedOptions>
}

pub enum CredentialTypeBasedOptions {
    UserCredential(UserCredentialOptions),
    ServiceAccountCredential(ServiceAccountCredentialOptions)
}

pub struct UserCredentialOptions {
    pub scopes: Option<Vec<String>>,
    pub early_token_refresh: Option<Duration>,
    pub disable_async_refresh: bool,
    pub token_url: Option<String>,
}

pub struct UserCredentialOptionsBuilder {
    scopes: Option<Vec<String>>,
    early_token_refresh: Option<Duration>,
    disable_async_refresh: bool,
    token_url: Option<String>,
}

impl UserCredentialOptionsBuilder {
    pub fn new() -> Self {
        Self {
            scopes: None,
            early_token_refresh: None,
            disable_async_refresh: false,
            token_url: None,
        }
    }

    pub fn scopes(&mut self, scopes: Vec<String>) -> &mut Self {
        self.scopes = Some(scopes);
        self
    }

    pub fn early_token_refresh(&mut self, duration: Duration) -> &mut Self {
        self.early_token_refresh = Some(duration);
        self
    }

    pub fn disable_async_refresh(&mut self, disable: bool) -> &mut Self {
        self.disable_async_refresh = disable;
        self
    }

    pub fn token_url(&mut self, url: String) -> &mut Self {
        self.token_url = Some(url);
        self
    }

    pub fn build(&self) -> UserCredentialOptions {
        UserCredentialOptions {
            scopes: self.scopes.clone(),
            early_token_refresh: self.early_token_refresh,
            disable_async_refresh: self.disable_async_refresh,
            token_url: self.token_url.clone(),
        }
    }
}

pub struct ServiceAccountCredentialOptions {
    pub scopes_or_audience: Option<ScopesOrAudience>,
    pub subject: Option<String>,
    pub token_url: Option<String>,
    pub universe_domain: Option<String>,
}

pub struct MDSOptions {
    pub scopes: Option<Vec<String>>,
    pub early_token_refresh: Option<Duration>,
    pub disable_async_refresh: bool,
    pub token_url: Option<String>,
    pub universe_domain: Option<String>,
}










// 
pub struct AccessTokenCredentialOptions3 {
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
    pub credential_source: CredentialsSource2,
}

#[derive(Debug, Clone)]
pub enum CredentialsSource2 {
    CredentialsFile(String),
    CredentialsJson(String),
    DefaultCredential,
}

pub struct AccessTokenCredentialOptions3Builder {
    quota_project_id: Option<String>,
    scopes: Option<Vec<String>>,
    credential_source: CredentialsSource2,
}

impl AccessTokenCredentialOptions3Builder {
    pub fn default_credential() -> Self {
        Self {
            quota_project_id: None,
            scopes: None,
            credential_source: CredentialsSource2::DefaultCredential,
        }
    }

    pub fn from_file(file_path: String) -> Self {
        Self {
            quota_project_id: None,
            scopes: None,
            credential_source: CredentialsSource2::CredentialsFile(file_path),
        }
    }

    pub fn from_json(json: String) -> Self {
        Self {
            quota_project_id: None,
            scopes: None,
            credential_source: CredentialsSource2::CredentialsJson(json),
        }
    }

    // pub fn credential_source(&mut self, credential_source: CredentialsSource) -> &mut Self {
    //     self.credential_source = Some(credential_source);
    //     self
    // }

    pub fn quota_project_id(&mut self, quota_project_id: String) -> &mut Self {
        self.quota_project_id = Some(quota_project_id);
        self
    }

    pub fn scopes(&mut self, scopes: Vec<String>) -> &mut Self {
        self.scopes = Some(scopes);
        self
    }

    pub fn build(&self) -> Result<AccessTokenCredentialOptions3, anyhow::Error> {
        Ok(AccessTokenCredentialOptions3 {
            quota_project_id: self.quota_project_id.clone(),
            scopes: self.scopes.clone(),
            credential_source: self.credential_source.clone(),
        })
    }
}



pub enum CommonOptions {
    QuotaProjectID(String),
    Scopes(Vec<String>)
}

// pub enum CredentialSource2 {
//     CredentialFile(String),
//     CredentialJson(String),
//     Credential(Box<dyn crate::credentials::Credential>)
// }