// Copyright 2025 Google LLC
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

use crate::KIB;
#[cfg(google_cloud_unstable_storage_bidi)]
use crate::MIB;
use crate::sample::Attempt;
use google_cloud_storage::{
    client::Storage, model::Object, model_ext::ReadRange, read_object::ReadObjectResponse,
};
use rand::prelude::IndexedRandom;
use std::time::Instant;

const UPLOADID: &str = "x-guploader-uploadid";

pub async fn json(client: &Storage, objects: &[Object]) -> Attempt {
    let object = objects
        .choose(&mut rand::rng())
        .expect("at least one object");
    let start = Instant::now();
    let result = client
        .read_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .set_read_range(ReadRange::head(8 * KIB as u64))
        .send()
        .await;
    match result {
        Ok(reader) => {
            let count = read_all(reader).await;
            Attempt {
                open_latency: start.elapsed(),
                object: object.name.clone(),
                uploadid: "[unknown]".to_string(),
                result: count.map(|_| ()),
            }
        }
        Err(e) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(e.http_headers()).unwrap_or_default(),
            result: Err(e.into()),
        },
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
pub async fn open(client: &Storage, objects: &[Object]) -> Attempt {
    let object = objects
        .choose(&mut rand::rng())
        .expect("at least one object");
    let start = Instant::now();
    let result = client
        .open_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send()
        .await;
    match result {
        Ok(d) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(Some(d.headers())).unwrap_or_default(),
            result: Ok(()),
        },
        Err(e) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(e.http_headers()).unwrap_or_default(),
            result: Err(e.into()),
        },
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
pub async fn open_read(client: &Storage, objects: &[Object]) -> Attempt {
    let object = objects
        .choose(&mut rand::rng())
        .expect("at least one object");
    let start = Instant::now();
    let result = client
        .open_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send()
        .await;
    match result {
        Ok(d) => {
            let open_latency = start.elapsed();
            // Read at least 4MiB so the first `next()` does not contain all the data.
            let reader = d.read_range(ReadRange::head(4 * MIB as u64)).await;
            let count = read_all(reader).await;
            Attempt {
                open_latency,
                object: object.name.clone(),
                uploadid: uploadid(Some(d.headers())).unwrap_or_default(),
                result: count.map(|_| ()),
            }
        }
        Err(e) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(e.http_headers()).unwrap_or_default(),
            result: Err(e.into()),
        },
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
pub async fn open_read_discard(client: &Storage, objects: &[Object]) -> Attempt {
    let object = objects
        .choose(&mut rand::rng())
        .expect("at least one object");
    let start = Instant::now();
    let result = client
        .open_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send()
        .await;
    match result {
        Ok(d) => {
            let open_latency = start.elapsed();
            // Read at least 4MiB so the first `next()` does not contain all the data.
            let mut reader = d.read_range(ReadRange::head(4 * MIB as u64)).await;
            let first = reader.next().await.transpose();
            Attempt {
                open_latency,
                object: object.name.clone(),
                uploadid: uploadid(Some(d.headers())).unwrap_or_default(),
                result: first.map(|_| ()).map_err(anyhow::Error::from),
            }
        }
        Err(e) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(e.http_headers()).unwrap_or_default(),
            result: Err(e.into()),
        },
    }
}

#[cfg(google_cloud_unstable_storage_bidi)]
pub async fn open_read_after_drop(client: &Storage, objects: &[Object]) -> Attempt {
    let object = objects
        .choose(&mut rand::rng())
        .expect("at least one object");
    let start = Instant::now();
    let result = client
        .open_object(&object.bucket, &object.name)
        .set_generation(object.generation)
        .send()
        .await;
    match result {
        Ok(d) => {
            let open_latency = start.elapsed();
            let uploadid = uploadid(Some(d.headers())).unwrap_or_default();
            let reader = d.read_range(ReadRange::offset(4 * MIB as u64)).await;
            drop(d);
            let count = read_all(reader).await;
            Attempt {
                open_latency,
                object: object.name.clone(),
                uploadid,
                result: count.map(|_| ()),
            }
        }
        Err(e) => Attempt {
            open_latency: start.elapsed(),
            object: object.name.clone(),
            uploadid: uploadid(e.http_headers()).unwrap_or_default(),
            result: Err(e.into()),
        },
    }
}

fn uploadid(headers: Option<&http::HeaderMap>) -> Option<String> {
    headers?.get(UPLOADID)?.to_str().ok().map(str::to_string)
}

async fn read_all(mut reader: ReadObjectResponse) -> anyhow::Result<usize> {
    let mut count = 0;
    while let Some(b) = reader.next().await.transpose()? {
        count += b.len();
    }
    Ok(count)
}
