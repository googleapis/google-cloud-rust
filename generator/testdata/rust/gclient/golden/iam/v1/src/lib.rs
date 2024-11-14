#![allow(dead_code)]

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

    /// API Overview
    ///
    /// Manages Identity and Access Management (IAM) policies.
    ///
    /// Any implementation of an API that offers access control features
    /// implements the google.iam.v1.IAMPolicy interface.
    ///
    /// ## Data model
    ///
    /// Access control is applied when a principal (user or service account), takes
    /// some action on a resource exposed by a service. Resources, identified by
    /// URI-like names, are the unit of access control specification. Service
    /// implementations can choose the granularity of access control and the
    /// supported permissions for their resources.
    /// For example one database service may allow access control to be
    /// specified only at the Table level, whereas another might allow access control
    /// to also be specified at the Column level.
    ///
    /// ## Policy Structure
    ///
    /// See google.iam.v1.Policy
    ///
    /// This is intentionally not a CRUD style API because access control policies
    /// are created and deleted implicitly with the resources to which they are
    /// attached.
    pub fn iam_policy(&self) -> Iampolicy {
        Iampolicy {
            client: self.clone(),
            base_path: "https://iam-meta-api.googleapis.com/".to_string(),
        }
    }
}

/// API Overview
///
/// Manages Identity and Access Management (IAM) policies.
///
/// Any implementation of an API that offers access control features
/// implements the google.iam.v1.IAMPolicy interface.
///
/// ## Data model
///
/// Access control is applied when a principal (user or service account), takes
/// some action on a resource exposed by a service. Resources, identified by
/// URI-like names, are the unit of access control specification. Service
/// implementations can choose the granularity of access control and the
/// supported permissions for their resources.
/// For example one database service may allow access control to be
/// specified only at the Table level, whereas another might allow access control
/// to also be specified at the Column level.
///
/// ## Policy Structure
///
/// See google.iam.v1.Policy
///
/// This is intentionally not a CRUD style API because access control policies
/// are created and deleted implicitly with the resources to which they are
/// attached.
#[derive(Debug)]
pub struct Iampolicy {
    client: Client,
    base_path: String,
}

impl Iampolicy {
    /// Sets the access control policy on the specified resource. Replaces any
    /// existing policy.
    ///
    /// Can return `NOT_FOUND`, `INVALID_ARGUMENT`, and `PERMISSION_DENIED` errors.
    pub async fn set_iam_policy(
        &self,
        req: crate::model::SetIamPolicyRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/{}:setIamPolicy",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::Policy>().await?;
        Ok(response)
    }

    /// Gets the access control policy for a resource.
    /// Returns an empty policy if the resource exists and does not have a policy
    /// set.
    pub async fn get_iam_policy(
        &self,
        req: crate::model::GetIamPolicyRequest,
    ) -> Result<crate::model::Policy, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/{}:getIamPolicy",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res.json::<crate::model::Policy>().await?;
        Ok(response)
    }

    /// Returns permissions that a caller has on the specified resource.
    /// If the resource does not exist, this will return an empty set of
    /// permissions, not a `NOT_FOUND` error.
    ///
    /// Note: This operation is designed to be used for building permission-aware
    /// UIs and command-line tools, not for authorization checking. This operation
    /// may "fail open" without warning.
    pub async fn test_iam_permissions(
        &self,
        req: crate::model::TestIamPermissionsRequest,
    ) -> Result<crate::model::TestIamPermissionsResponse, Box<dyn std::error::Error>> {
        let query_parameters = [None::<(&str, String)>; 0];
        let client = self.client.inner.clone();
        let res = client
            .http_client
            .post(format!(
                "{}/v1/{}:testIamPermissions",
                self.base_path, req.resource,
            ))
            .query(&[("alt", "json")])
            .query(
                &query_parameters
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(&str, String)>>(),
            )
            .bearer_auth(&client.token)
            .json(&req)
            .send()
            .await?;
        if !res.status().is_success() {
            return Err(
                "sorry the api you are looking for is not available, please try again".into(),
            );
        }
        let response = res
            .json::<crate::model::TestIamPermissionsResponse>()
            .await?;
        Ok(response)
    }
}
