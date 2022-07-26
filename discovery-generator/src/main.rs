// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(dead_code)]

use anyhow::{anyhow, Result};
use chrono::Datelike;
use regex::Regex;
use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::io;
use std::io::Write as IoWrite;
use std::path::Path;
use std::path::PathBuf;
use structopt::StructOpt;

mod model;
mod schema;
mod util;
use model::*;
use schema::*;
use util::*;

// TODO(codyoss): Find a way to sniff the content type for uploads, or we just way the user must provide the content-type?
// TODO(codyoss): support resumable/chunked uploads
// TODO(codyoss): Consult storage team about proper retrying for downloads/uploads. This gets tricky fast.

#[derive(StructOpt, Debug)]
#[structopt(name = "discogen")]
struct Opt {
    /// Input discovery document file to generate sources from.
    #[structopt(short, long, parse(from_os_str))]
    input: std::path::PathBuf,

    /// Output directory which contains generated sources, stdout if not present
    #[structopt(short, long, parse(from_os_str))]
    output: Option<std::path::PathBuf>,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    generate_api(opt.input, opt.output)
}

/// Entry point for generating a discovery based client from the passed in `input`
/// that is written to `output`.
fn generate_api<P: AsRef<Path>>(input: P, output: Option<P>) -> Result<()> {
    let contents = fs::read(input)?;
    let d: Document = serde_json::from_slice(&contents)?;
    let mut doc_gen = DocumentGenerator {
        b: String::new(),
        d,
    };
    if let Some(out) = output {
        let mut lib = PathBuf::new();
        lib.push(&out);
        lib.push("lib.rs");
        fs::write(&lib, doc_gen.gen_services()?)?;

        let mut schema = PathBuf::new();
        schema.push(&out);
        schema.push("model.rs");
        fs::write(&schema, doc_gen.gen_models()?)?;
    } else {
        io::stdout().write_all(doc_gen.gen_services()?.as_bytes())?;
        io::stdout().write_all(doc_gen.gen_models()?.as_bytes())?;
    }
    Ok(())
}

struct DocumentGenerator {
    /// An in memory buffer of the code to write out.
    b: String,
    /// The discovery document describing the code to generate.
    d: Document,
}

impl DocumentGenerator {
    /// Generate a client.
    fn gen_services(&mut self) -> Result<String> {
        self.license()?;
        self.service_imports()?;
        self.consts()?;
        self.scopes()?;
        let service_mapping = self.base_client()?;
        self.services(service_mapping)?;
        self.helpers()?;
        let buf = self.b.clone();
        self.b.clear();
        Ok(buf)
    }

    fn gen_models(&mut self) -> Result<String> {
        self.b = String::new();
        self.license()?;
        self.model_imports()?;
        self.schemas()?;
        let buf = self.b.clone();
        self.b.clear();
        Ok(buf)
    }

    /// Generate license headers.
    fn license(&mut self) -> Result<()> {
        let year = chrono::Utc::now().year();
        write!(
            &mut self.b,
            "// Copyright {} Google LLC
//
// Licensed under the Apache License, Version 2.0 (the \"License\");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an \"AS IS\" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

",
            year
        )?;
        Ok(())
    }

    /// Generate any required imports for lib.rs.
    fn service_imports(&mut self) -> Result<()> {
        write!(
            &mut self.b,
            "#![allow(dead_code)]

use google_cloud_auth::{{Credential, CredentialConfig}};
use serde::Deserialize;
use std::error::Error as StdError;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

pub mod model;

"
        )?;
        Ok(())
    }

    /// Generate any required imports for model.rs.
    fn model_imports(&mut self) -> Result<()> {
        write!(
            &mut self.b,
            "#![allow(dead_code)]

use serde::{{Deserialize, Serialize}};

"
        )?;
        Ok(())
    }

    /// Generate any required consts like default base paths.
    fn consts(&mut self) -> Result<()> {
        let mut base_url = self.d.root_url.to_string();
        base_url.push_str(&self.d.service_path);
        let mut mtls_base_url = self.d.mtls_root_url.to_string();
        mtls_base_url.push_str(&self.d.service_path);
        // TODO(codyoss): this should probs be done with proper URL parsing
        base_url = base_url.replace("//", "/");
        mtls_base_url = mtls_base_url.replace("//", "/");
        write!(
            &mut self.b,
            "const BASE_PATH: &str = \"{}\";
const MTLS_BASE_PATH: &str = \"{}\";

",
            base_url, mtls_base_url
        )?;
        Ok(())
    }

    /// Generate default scopes to request when using the API.
    fn scopes(&mut self) -> Result<()> {
        let scopes = self
            .d
            .auth
            .oauth2
            .scopes
            .keys()
            .map(|x| format!("\"{}\".to_string()", x))
            .collect::<Vec<String>>()
            .join(",");
        write!(
            &mut self.b,
            "fn default_scopes() -> Vec<String> {{
    vec![{}]
}}

",
            scopes
        )?;
        Ok(())
    }

    /// Generate the base client and all service structs.
    fn base_client(&mut self) -> Result<BTreeMap<String, BTreeMap<String, Method>>> {
        let mut service_mapping: BTreeMap<String, BTreeMap<String, Method>> = BTreeMap::new();
        get_service_names("", &self.d.resources, &mut service_mapping);

        let mut base_service_methods = String::new();
        for value in service_mapping.keys() {
            let pascal_value = snake_to_pascal(value);
            write!(
                &mut base_service_methods,
                "
    pub fn {}_service(&self) -> {}Service {{
        {}Service {{
            client: self.clone(),
        }}
    }}",
                camel_to_snake(value),
                pascal_value,
                pascal_value
            )?;
        }

        let mut service_structs = String::new();
        for value in service_mapping.keys() {
            write!(
                &mut service_structs,
                "

#[derive(Debug)]
pub struct {}Service {{
    client: Client,
}}",
                snake_to_pascal(value)
            )?;
        }
        write!(
            &mut self.b,
            "#[derive(Clone, Debug)]
pub struct Client {{
    inner: Arc<ClientRef>,
}}

struct ClientRef {{
    http_client: reqwest::Client,
    base_path: String,
    cred: Credential,
}}

impl std::fmt::Debug for ClientRef {{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{
        f.debug_struct(\"ClientRef\").field(\"http_client\", &self.http_client).field(\"base_path\", &self.base_path).finish()
    }}
}}

impl Default for ClientRef {{
    fn default() -> Self {{
        let mut headers = http::HeaderMap::with_capacity(1);
        headers.insert(\"User-Agent\", \"gcloud-rust/0.1\".parse().unwrap());
        headers.insert(
            \"x-goog-api-client\",
            format!(
                \"gl-rust/{{}}  gdcl/0.1\",
                rustc_version_runtime::version().to_string()
            )
            .parse()
            .unwrap(),
        );
        let client = reqwest::Client::builder().default_headers(headers).build().unwrap();
        Self {{
            http_client: client,
            base_path: BASE_PATH.into(),
            cred: Credential::default(),
        }}
    }}
}}

impl Client {{
    pub async fn new() -> Result<Client> {{
        let cc = CredentialConfig::builder()
            .scopes(default_scopes())
            .build()
            .map_err(Error::wrap)?;
        let cred = Credential::find_default(cc)
            .await
            .map_err(Error::wrap)?;
        let mut headers = http::HeaderMap::with_capacity(1);
        headers.insert(\"User-Agent\", \"gcloud-rust/0.1\".parse().unwrap());
        headers.insert(
            \"x-goog-api-client\",
            format!(
                \"gl-rust/{{}}  gdcl/0.1\",
                rustc_version_runtime::version().to_string()
            )
            .parse()
            .unwrap(),
        );
        let client = reqwest::Client::builder().default_headers(headers).build().unwrap();
        let inner = ClientRef {{
            base_path: BASE_PATH.into(),
            http_client: client,
            cred,
        }};
        Ok(Client {{
            inner: Arc::new(inner),
        }})
    }}{}
}}

impl Default for Client {{
    fn default() -> Self {{
        Self {{
            inner: Arc::new(ClientRef::default()),
        }}
    }}
}}{}

",
            base_service_methods, service_structs
        )?;
        Ok(service_mapping)
    }

    /// Generate the different service the struct impls.
    fn services(
        &mut self,
        service_mapping: BTreeMap<String, BTreeMap<String, Method>>,
    ) -> Result<()> {
        for (service, methods) in service_mapping {
            write!(
                &mut self.b,
                "impl {}Service {{{}
}}{}

",
                snake_to_pascal(&service),
                service_methods(&service, &methods)?,
                call_impl(&service, &methods)?,
            )?;
        }
        Ok(())
    }

    /// Generate all struct schemas used by the API. These are all of the request
    /// and response objects.
    fn schemas(&mut self) -> Result<()> {
        let struct_schemas = schema_structs(&self.d.schemas)?;
        for (struct_name, schema) in struct_schemas.schemas() {
            let mut fields = schema.fields.clone();
            fields.sort();
            let formatted_fields = formatted_struct_fields(&fields)?;
            let docs = if let Some(comment) = &schema.doc {
                as_comment("", comment.clone(), false)?
            } else {
                String::new()
            };
            write!(
                &mut self.b,
                "
{}#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = \"camelCase\")]
#[non_exhaustive]
pub struct {} {{{}
}}
",
                docs, struct_name, formatted_fields
            )?;

            let builder_struct = format!("{}Builder", struct_name);
            write!(
                &mut self.b,
                "
impl {} {{
    /// Creates a builder to more easily construct the [{}] struct.
    pub fn builder() -> {} {{
        Default::default()
    }}
}}
",
                struct_name, struct_name, &builder_struct
            )?;
            write!(
                &mut self.b,
                "
#[non_exhaustive]
#[derive(Clone, Debug, Default)]
/// A builder used to more easily construct the [{}] struct.
pub struct {} {{{}
}}
",
                struct_name,
                &builder_struct,
                formatted_builder_fields(&fields)?
            )?;
            write!(
                &mut self.b,
                "
impl {} {{{}
}}
",
                &builder_struct,
                builder_impl(&fields, struct_name)?
            )?;
        }
        Ok(())
    }

    /// Generate any private helpers methods the API may need.
    fn helpers(&mut self) -> Result<()> {
        write!(
            &mut self.b,
            "

fn set_path(base: &str, path: &str) -> String {{
    let mut url = reqwest::Url::parse(base).unwrap();
    url.set_path(path);
    url.to_string()
}}

#[derive(Debug)]
pub struct Error {{
    inner_error: Option<Box<dyn StdError + Send + Sync>>,
    message: Option<String>,
}}

impl Error {{
    fn new(msg: impl Into<String>) -> Self {{
        Self {{
            inner_error: None,
            message: Some(msg.into()),
        }}
    }}

    fn wrap<E>(error: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {{
        Self {{
            inner_error: Some(Box::new(error)),
            message: None,
        }}
    }}

    /// Returns a reference to the inner error wrapped if, if there is one.
    pub fn get_ref(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {{
        match &self.inner_error {{
            Some(err) => Some(err.as_ref()),
            None => None,
        }}
    }}
}}

impl std::fmt::Display for Error {{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{
        if let Some(inner_error) = &self.inner_error {{
            inner_error.fmt(f)
        }} else if let Some(msg) = &self.message {{
            write!(f, \"{{}}\", msg)
        }} else {{
            write!(f, \"unknown error\")
        }}
    }}
}}

impl StdError for Error {{}}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
struct ApiErrorReply {{
    error: ApiError,
}}

impl ApiErrorReply {{
    fn into_inner(self) -> ApiError {{
        self.error
    }}
}}

#[derive(Clone, Debug, Deserialize)]
#[non_exhaustive]
pub struct ApiError {{
    pub code: i32,
    pub message: String,
    #[serde(flatten)]
    extra: serde_json::value::Value,
}}

impl std::fmt::Display for ApiError {{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {{
        write!(
            f,
            \"{{}}: {{}}: {{}}\",
            self.code,
            self.message,
            self.extra.to_string()
        )
    }}
}}

impl StdError for ApiError {{}}
"
        )?;
        Ok(())
    }
}

/// Return a buffer of the different call objects for a service.
fn service_methods(service: &str, methods: &BTreeMap<String, Method>) -> Result<String> {
    let mut service_method_buf: String = String::new();
    //let mut method_buf: String = String::new();
    for (name, method) in methods {
        let service_method_call = snake_to_pascal(&format!("{}_{}Call", service, name));
        let service_method_input = service_method_input(method)?;
        let docs = if let Some(comment) = &method.description {
            let mut comment = as_comment("    ", comment.clone(), false)?;
            for param_key in &method.parameter_order {
                let param_docs = format!(
                    "- {}: {}",
                    camel_to_snake(param_key),
                    method
                        .parameters
                        .get(param_key)
                        .unwrap()
                        .schema
                        .description
                        .as_deref()
                        .unwrap()
                );
                let param_comment = as_comment("    ", param_docs, true)?;
                comment.push_str(&param_comment);
            }
            comment
        } else {
            String::new()
        };
        let set_required = service_method_set_required(method)?;
        write!(
            &mut service_method_buf,
            "
{}    pub fn {}(&self{}) -> {} {{
        let mut c = {} {{
            client: self.client.clone(),
            ..Default::default()
        }};{}
        c
    }}",
            docs,
            camel_to_snake(name),
            service_method_input,
            service_method_call,
            service_method_call,
            set_required
        )?;
    }
    Ok(service_method_buf)
}

/// Return a buffer indicating required request body for a service method call.
fn service_method_input(method: &Method) -> Result<String> {
    let mut buf = String::new();
    for param_key in &method.parameter_order {
        let param = method
            .parameters
            .get(param_key)
            .expect("must have associated parameter");
        let (param_type, _) = setter_type(
            param
                .schema
                .schema_type
                .as_deref()
                .ok_or_else(|| anyhow!("missing schema_type for schema: {:?}", param.schema))?,
            param.repeated.unwrap_or(false),
        );
        write!(&mut buf, ", {}: {}", camel_to_snake(param_key), param_type)?;
    }
    if let Some(req) = &method.request {
        write!(
            &mut buf,
            ", request: model::{}",
            req.schema_ref
                .as_ref()
                .ok_or_else(|| anyhow!("no schema_ref found for request: {:?}", req))?
        )?;
    }
    Ok(buf)
}

/// Return a buffer indicating required request body for a service method call.
fn service_method_set_required(method: &Method) -> Result<String> {
    let mut buf = String::new();
    for param_key in &method.parameter_order {
        let param = method.parameters.get(param_key).unwrap();
        let into_buf = if param.schema.schema_type.as_deref().unwrap() == "string"
            && param.repeated.unwrap_or(false) == false
        {
            ".into()"
        } else {
            ""
        };

        if param.location == "path" {
            write!(
                &mut buf,
                "
                c.{} = Some({}{});",
                camel_to_snake(param_key),
                camel_to_snake(param_key),
                into_buf
            )?;
        } else if !into_buf.is_empty() {
            // query location
            write!(
                &mut buf,
                "
                c.url_params.insert(\"{}\".into(), vec![{}{}]);",
                param_key,
                camel_to_snake(param_key),
                into_buf
            )?;
        } else {
            // query location, repeated
            write!(
                &mut buf,
                "
                c.url_params.insert(\"{}\".into(), {});",
                param_key,
                camel_to_snake(param_key)
            )?;
        }
    }
    if let Some(_) = &method.request {
        write!(&mut buf, "c.request = request;")?;
    }
    Ok(buf)
}

/// Return a buffer of the impl for a service call struct.
fn call_impl(service: &str, methods: &BTreeMap<String, Method>) -> Result<String> {
    let mut method_buf: String = String::new();
    for (name, method) in methods {
        let call = snake_to_pascal(format!("{}_{}Call", service, name).as_str());
        let mut call_params = call_params(method)?;
        let mut call_param_setters = call_param_setters(&method.parameters)?;
        if method.media_upload.is_some() {
            write!(
                &mut call_params,
                "
    media_content_type: Option<String>,"
            )?;
            write!(
                &mut call_param_setters,
                "
    /// Explicitly sets the content type of the media being uploaded.
    pub fn media_content_type(mut self, value: impl Into<String>) -> Self {{
        self.media_content_type = Some(value.into());
        self
    }}"
            )?;
        }
        let return_type: String = if let Some(response) = &method.response {
            let s = response
                .schema_ref
                .as_ref()
                .ok_or_else(|| anyhow!("no schema_ref for response: {:?}", response))?;
            format!("model::{}", s)
        } else {
            "()".into()
        };
        let mut return_response = String::new();
        if return_type.eq("()") {
            return_response.push_str(
                "
            Ok(())",
            )
        } else {
            return_response.push_str(&format!(
                "
            let res: {} = res.json().await.map_err(Error::wrap)?;
            Ok(res)",
                &return_type
            ))
        };
        let http_method = http_method(
            method
                .http_method
                .as_ref()
                .ok_or_else(|| anyhow!("missing http_method for method: {:?}", method))?,
        );
        let call_url = call_url(
            &http_method,
            method
                .path
                .as_ref()
                .ok_or_else(|| anyhow!("missing path for method: {:?}", method))?,
            &method.parameters,
            &method.parameter_order,
            method.media_upload.as_ref(),
        )?;
        let mut url_params = String::new();
        if method.parameters.values().any(|p| p.location.eq("query")) {
            // TODO(codyoss): This should do proper mapping for when vec contains more than one element
            write!(
                &mut url_params,
                "
            .query(&self.url_params.iter().map(|(k,v)| (k.as_str(), v[0].as_str())).collect::<Vec<(&str, &str)>>())")?;
        }
        let json_request = if method.request.is_some() {
            "
            .json(&self.request)"
        } else {
            ""
        };
        let execute_meth = execute_method(
            method.media_upload.is_some(),
            &return_type,
            &call_url,
            &url_params,
            json_request,
            &return_response,
        )?;
        let media_download_meth = media_download_method(
            method.supports_media_download.unwrap_or(false),
            &call_url,
            &url_params,
            json_request,
        )?;
        let media_upload_meth = media_upload_method(
            method.media_upload.is_some(),
            &return_type,
            &call_url,
            &url_params,
            &return_response,
        )?;
        write!(
            &mut method_buf,
            "
#[derive(Debug, Default)]
pub struct {} {{
    client: Client,{}
}}

impl {} {{{}
{}{}{}
}}
",
            call,
            call_params,
            call,
            call_param_setters,
            execute_meth,
            media_download_meth,
            media_upload_meth
        )?;
    }
    Ok(method_buf)
}

/// Recursively build up a mapping from services to all of their different methods.
fn get_service_names(
    key_prefix: &str,
    resources: &BTreeMap<String, Resource>,
    service_mapping: &mut BTreeMap<String, BTreeMap<String, Method>>,
) {
    for (key, value) in resources {
        let mut new_key: String = key_prefix.into();
        if new_key.is_empty() {
            new_key += key;
        } else {
            new_key = new_key + "_" + key.as_str();
        }
        if !value.methods.is_empty() {
            service_mapping.insert(new_key.clone(), value.methods.clone());
        }
        get_service_names(new_key.as_str(), &value.resources, service_mapping)
    }
}

/// Return a buffer of the field attributes for a service call struct.
fn call_params(method: &Method) -> Result<String> {
    let mut buf = String::new();
    if let Some(req) = &method.request {
        let in_type = req
            .schema_ref
            .as_ref()
            .ok_or_else(|| anyhow!("missing schema_ref for req: {:?}", req))?;
        let in_type = format!("model::{}", in_type);
        write!(
            &mut buf,
            "
    request: {},",
            in_type
        )?;
    }
    let parameters = &method.parameters;
    if parameters.values().any(|p| p.location.eq("query")) {
        write!(
            &mut buf,
            "
    url_params: std::collections::HashMap<String, Vec<String>>,"
        )?;
    }
    for (name, details) in parameters.iter().filter(|d| !d.1.location.eq("query")) {
        write!(
            &mut buf,
            "
    {}: Option<{}>,",
            camel_to_snake(name),
            struct_type(&details.schema)?
        )?;
    }

    Ok(buf)
}

/// Return a buffer of all the service call structs setters for things like setting
/// URL parameters or query values.
fn call_param_setters(parameters: &BTreeMap<String, Parameter>) -> Result<String> {
    let mut buf = String::new();
    for (name, details) in parameters {
        if details.required.unwrap_or(false) {
            // These will have already been set, no need for setters
            continue;
        }
        let name = camel_to_snake(name);
        let (in_type, into) =
            setter_type(
                details.schema.schema_type.as_deref().ok_or_else(|| {
                    anyhow!("missing schema_type for schema: {:?}", details.schema)
                })?,
                details.repeated.unwrap_or(false),
            );
        let val = if into {
            "value.into()".to_string()
        } else {
            "value.to_string()".to_string()
        };
        let docs = if let Some(comment) = &details.schema.description {
            as_comment("    ", comment.clone(), false)?
        } else {
            String::new()
        };
        if details.location.eq("query") {
            write!(
                &mut buf,
                "
{}    pub fn {}(mut self, value: {}) -> Self {{
        self.url_params.insert(\"{}\".into(), vec![{}]);
        self
    }}",
                docs,
                safe_method_name(&name),
                in_type,
                name,
                val
            )?;
        } else {
            write!(
                &mut buf,
                "
{}    pub fn {}(mut self, value: {}) -> Self {{
        self.{} = Some({});
        self
    }}",
                docs,
                safe_method_name(&name),
                in_type,
                name,
                val
            )?;
        }
    }

    Ok(buf)
}

fn builder_impl(fields: &[StructField], base_struct: &str) -> Result<String> {
    let mut buf = String::new();
    // Create setters
    for field in fields {
        let doc = if let Some(comment) = &field.doc {
            as_comment("    ", comment.clone(), false)?
        } else {
            String::new()
        };
        let is_into = if &field.field_type == "String" || &field.field_type == "i64" {
            true
        } else {
            false
        };
        if is_into {
            write!(
                &mut buf,
                "
{}    pub fn {}(mut self, value: impl Into<{}>) -> Self {{
        self.{} = Some(value.into());
        self
    }}",
                doc,
                safe_method_name(&field.name),
                &field.field_type,
                &field.name
            )?;
        } else {
            write!(
                &mut buf,
                "
{}    pub fn {}(mut self, value: {}) -> Self {{
        self.{} = Some(value);
        self
    }}",
                doc,
                safe_method_name(&field.name),
                &field.field_type,
                &field.name
            )?;
        }
    }

    // Create build method
    let mut field_buf = String::new();
    for field in fields {
        write!(
            &mut field_buf,
            "\n            {}: self.{},",
            &field.name, &field.name
        )?;
    }

    // Create build method
    write!(
        &mut buf,
        "
    /// Builds [{}].
    pub fn build(self) -> {} {{
        {}{{{}
        }}
    }}",
        base_struct, base_struct, base_struct, field_buf
    )?;

    Ok(buf)
}

/// Returns the proper Rust type that corresponds to a given schema discovery
/// document type.
fn struct_type(schema: &Schema) -> Result<String> {
    if let Some(schema_ref) = schema.schema_ref.as_ref() {
        return Ok(schema_ref.into());
    }
    let param_type = schema
        .schema_type
        .as_ref()
        .ok_or_else(|| anyhow!("missing schema_type for schema: {:?}", schema))?;
    let param_type = match param_type.as_str() {
        "integer" => "i64".into(),
        "string" => "String".into(),
        "object" => {
            println!("{:?}", schema);
            let add_prop = schema
                .additional_properties
                .as_ref()
                .ok_or_else(|| anyhow!("missing additional_properties for schema: {:?}", schema))?;
            if let Some(ref_type) = &add_prop.schema_ref {
                format!("std::collections::HashMap<String, {}>", ref_type)
            } else if let Some(schema_type) = &add_prop.schema_type {
                if schema_type.eq("any") {
                    "Vec<u8>".into()
                } else {
                    format!(
                        "std::collections::HashMap<String, {}>",
                        struct_type(add_prop.as_ref())?
                    )
                }
            } else {
                panic!("unknown type: {}", param_type)
            }
        }
        "array" => {
            let items = schema
                .items
                .as_ref()
                .ok_or_else(|| anyhow!("missing items for schema: {:?}", schema))?;
            if let Some(ref_type) = &items.schema_ref {
                format!("Vec<{}>", ref_type)
            } else {
                format!("Vec<{}>", struct_type(items.as_ref())?)
            }
        }
        _ => panic!("unknown type: {}", param_type),
    };
    Ok(param_type)
}

/// Returns a Rust type and wether that type needs to be transformed with `into()`
/// given a simple discovery document schema type.
fn setter_type(param_type: &str, repeated: bool) -> (String, bool) {
    if repeated {
        // repeated values are only ever strings
        return ("Vec<String>".into(), false);
    }
    match param_type {
        "string" => ("impl Into<String>".into(), true),
        "integer" => ("i64".into(), false),
        "boolean" => ("bool".into(), false),
        _ => panic!("unknown type: {}", param_type),
    }
}

/// Returns a buffer for contains the HTTP request methods and proper formatting
/// of the URL based on method and any string substitution that needs to take
/// place from user input.
///
/// Example return value: `get(format!("{}b/{}/o/{}", client.base_path,self.bucket.unwrap(),self.object.unwrap()))`.  
fn call_url(
    http_method: &str,
    path: &str,
    parameters: &BTreeMap<String, Parameter>,
    order: &[String],
    upload: Option<&MediaUpload>,
) -> Result<String> {
    let mut buf = String::new();
    let re = Regex::new(r"\{\+?[a-zA-Z]*\}")?;
    let mut param_buf = String::new();
    for key in order {
        let details = parameters
            .get(key)
            .ok_or_else(|| anyhow!("no parameter found for key: {}", key))?;
        if details.location.eq("path") {
            write!(&mut param_buf, ",self.{}.unwrap()", camel_to_snake(key))?;
        }
    }
    if let Some(upload) = upload {
        // This is specific to storage. Might need to be tweak of other APIs.
        let path = re.replace_all(
            &upload
                .protocols
                .get("simple")
                .ok_or_else(|| {
                    anyhow!("missing key `simple` in protocols: {:?}", upload.protocols)
                })?
                .path,
            "{}",
        );
        write!(
            &mut buf,
            "{}(set_path(&client.base_path, &format!(\"{}\"{})))",
            http_method, path, param_buf
        )?;
    } else {
        let path = re.replace_all(path, "{}");
        write!(
            &mut buf,
            "{}(format!(\"{{}}{}\", client.base_path{}))",
            http_method, path, param_buf
        )?;
    }
    Ok(buf)
}

/// Return the reqwest method for the corresponding discovery method.
fn http_method(method: &str) -> String {
    match method {
        "GET" => "get".into(),
        "PATCH" => "patch".into(),
        "POST" => "post".into(),
        "PUT" => "put".into(),
        "DELETE" => "delete".into(),
        _ => panic!("unsupported method: {}", method),
    }
}

/// Return a buffer of the formatted struct fields for a schema struct.
fn formatted_struct_fields(fields: &[StructField]) -> Result<String> {
    let mut buf = String::new();
    for field in fields {
        let mut prefix = String::new();
        if !field.prefix.is_empty() {
            prefix = format!("{}\n    ", field.prefix)
        }
        let doc = if let Some(comment) = &field.doc {
            as_comment("    ", comment.clone(), false)?
        } else {
            String::new()
        };

        write!(
            &mut buf,
            "
{}    {}pub {}: Option<{}>,",
            doc, prefix, field.name, field.field_type
        )?;
    }
    Ok(buf)
}

/// Return a buffer of the formatted builder fields for a schema struct.
fn formatted_builder_fields(fields: &[StructField]) -> Result<String> {
    let mut buf = String::new();
    for field in fields {
        write!(
            &mut buf,
            "
    {}: Option<{}>,",
            field.name, field.field_type
        )?;
    }
    Ok(buf)
}

/// Return a buffer for media download method.
fn media_download_method(
    supported: bool,
    call_url: &str,
    url_params: &str,
    json_request: &str,
) -> Result<String> {
    let mut buf = String::new();
    if !supported {
        return Ok(buf);
    }
    write!(
        &mut buf,
        "

    pub async fn download<'a, W>(self, writer: &'a mut W) -> Result<()>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {{
        let client = self.client.inner;
        let tok = client
            .cred
            .access_token()
            .await
            .map_err(Error::wrap)?;
        let mut res = client
            .http_client
            .{}{}{}
            .query(&[(\"alt\", \"media\")])
            .query(&[(\"prettyPrint\", \"false\")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {{
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }}
        while let Some(chunk) = res.chunk().await.map_err(Error::wrap)? {{
            writer.write(&chunk).await.map_err(Error::wrap)?;
        }}
        Ok(())
    }}",
        call_url, url_params, json_request
    )?;
    Ok(buf)
}

/// Return a buffer for a media upload method.
fn media_upload_method(
    is_upload: bool,
    return_type: &str,
    call_url: &str,
    url_params: &str,
    return_response: &str,
) -> Result<String> {
    let mut buf = String::new();
    if !is_upload {
        return Ok(buf);
    }
    write!(
        &mut buf,
        "

    pub async fn upload<'a, R>(mut self, reader: &'a mut R) -> Result<{}>
    where
    R: tokio::io::AsyncRead + Unpin,
    {{
        let client = self.client.inner;
        let tok = client.cred.access_token().await.map_err(Error::wrap)?;
        let body = serde_json::to_vec(&self.request).map_err(Error::wrap)?;
        let mut buf: Vec<u8> = vec![];
        tokio::io::copy(reader, &mut buf)
            .await
            .map_err(Error::wrap)?;
        let mut media_part = reqwest::multipart::Part::bytes(buf);
        if let Some(media_content_type) = self.media_content_type {{
            media_part = media_part
                .mime_str(&media_content_type)
                .map_err(Error::wrap)?;
        }}
        let form = reqwest::multipart::Form::new()
            .part(
                \"body\",
                reqwest::multipart::Part::bytes(body)
                .mime_str(\"application/json\")
                .map_err(Error::wrap)?,
            )
            .part(\"media\", media_part);
        self.url_params
            .insert(\"uploadType\".into(), vec![\"multipart\".into()]);

        let res = client
            .http_client
            .{}{}
            .multipart(form)
            .query(&[(\"alt\", \"json\")])
            .query(&[(\"prettyPrint\", \"false\")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {{
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }}{}
    }}
    ",
        return_type, call_url, url_params, return_response
    )?;
    Ok(buf)
}

/// Return a buffer for a general purpose HTTP request method.
fn execute_method(
    is_upload: bool,
    return_type: &str,
    call_url: &str,
    url_params: &str,
    json_request: &str,
    return_response: &str,
) -> Result<String> {
    let mut buf = String::new();
    if is_upload {
        return Ok(buf);
    }
    write!(
        &mut buf,
        "

    pub async fn execute(self) -> Result<{}> {{
        let client = self.client.inner;
        let tok = client
            .cred
            .access_token()
            .await
            .map_err(Error::wrap)?;
        let res = client
            .http_client
            .{}{}{}
            .query(&[(\"alt\", \"json\")])
            .query(&[(\"prettyPrint\", \"false\")])
            .bearer_auth(tok.value)
            .send()
            .await
            .map_err(Error::wrap)?;
        if !res.status().is_success() {{
            let error: ApiErrorReply = res.json().await.map_err(Error::wrap)?;
            return Err(Error::wrap(error.into_inner()));
        }}{}
    }}",
        return_type, call_url, url_params, json_request, return_response
    )?;
    Ok(buf)
}
