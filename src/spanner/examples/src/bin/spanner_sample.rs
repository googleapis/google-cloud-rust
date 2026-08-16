// Copyright 2026 Google LLC
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

//! Simple command-line application to execute GoogleSQL Spanner samples.
//!
//! # Usage
//! ```bash
//! cargo run -p spanner-samples --bin spanner_sample -- <command> <instance-id> <database-id>
//! ```

use google_cloud_spanner::client::Spanner;
use spanner_samples::{client, database, dml, mutation, query, read};

fn print_usage_and_exit(program: &str) -> ! {
    eprintln!("Usage: {program} <command> <instance-id> <database-id>");
    eprintln!("Commands:");
    eprintln!("  createdatabase");
    eprintln!("  write");
    eprintln!("  insertusingdml | writeusingdml");
    eprintln!("  query");
    eprintln!("  querywithparameter");
    eprintln!("  read");
    eprintln!("  addindex | createindex");
    eprintln!("  readindex");
    eprintln!("  addcolumn | addmarketingbudget");
    eprintln!("  update");
    eprintln!("  updateusingdml");
    eprintln!("  updateusingpartitioneddml");
    eprintln!("  querynewcolumn | querymarketingbudget");
    eprintln!("  addstoringindex");
    eprintln!("  readstoringindex");
    eprintln!("  readonlytransaction");
    eprintln!("  deleteusingpartitioneddml");
    std::process::exit(1);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        print_usage_and_exit(&args[0]);
    }

    let command = &args[1];
    let instance_id = &args[2];
    let database_id = &args[3];

    let project_id = std::env::var("GOOGLE_CLOUD_PROJECT")
        .or_else(|_| std::env::var("SPANNER_EMULATOR_HOST").map(|_| "test-project".to_string()))
        .expect("GOOGLE_CLOUD_PROJECT environment variable must be set (or SPANNER_EMULATOR_HOST)");

    let instance_name = format!("projects/{project_id}/instances/{instance_id}");
    let database_name = format!("{instance_name}/databases/{database_id}");

    if command == "createdatabase" {
        let spanner = Spanner::builder().build().await?;
        let admin_client = spanner.database_admin_builder().build().await?;
        database::create_database::sample(&admin_client, &instance_name, database_id).await?;
        return Ok(());
    }

    let (client, admin_client) = client::init_client::sample(&database_name).await?;

    match command.as_str() {
        "write" => mutation::insert_data::sample(&client).await?,
        "insertusingdml" | "writeusingdml" => dml::dml_insert::sample(&client).await?,
        "query" => query::query_data::sample(&client).await?,
        "querywithparameter" => query::query_parameter::sample(&client).await?,
        "read" => read::read_data::sample(&client).await?,
        "addindex" | "createindex" => {
            database::create_index::sample(&admin_client, &database_name).await?
        }
        "readindex" => query::read_data_with_index::sample(&client).await?,
        "addcolumn" | "addmarketingbudget" => {
            database::add_column::sample(&admin_client, &database_name).await?
        }
        "update" => mutation::update_data::sample(&client).await?,
        "updateusingdml" => dml::dml_update::sample(&client).await?,
        "updateusingpartitioneddml" => dml::dml_partitioned_update::sample(&client).await?,
        "querynewcolumn" | "querymarketingbudget" => {
            query::query_new_column::sample(&client).await?
        }
        "addstoringindex" => {
            database::create_storing_index::sample(&admin_client, &database_name).await?
        }
        "readstoringindex" => query::read_data_with_storing_index::sample(&client).await?,
        "readonlytransaction" => query::read_only_transaction::sample(&client).await?,
        "deleteusingpartitioneddml" => dml::dml_partitioned_delete::sample(&client).await?,
        _ => {
            eprintln!("Unknown command: {command}");
            print_usage_and_exit(&args[0]);
        }
    }

    Ok(())
}
