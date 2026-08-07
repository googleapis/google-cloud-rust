# Spanner Client Library Examples

This directory contains a number of code examples used in the Spanner
documentation.

## Running the Sample Applications

You can run individual samples from the command line using Cargo. We provide two
binary applications corresponding to the GoogleSQL and PostgreSQL dialects:

### GoogleSQL Dialect (`spanner_sample`)

```bash
cargo run -p spanner-samples --bin spanner_sample -- <command> <instance-id> <database-id>
```

#### Example Commands

- `createdatabase`: Provision a GoogleSQL database and initial schema
- `write`: Insert initial data into Singers and Albums
- `insertusingdml` / `writeusingdml`: Insert rows into Singers table using DML
- `query`: Query Albums table
- `querywithparameter`: Query Singers table using a parameter
- `read`: Read rows from Albums table
- `addindex` / `createindex`: Create an index
- `readindex`: Read rows using an index
- `addcolumn` / `addmarketingbudget`: Add a MarketingBudget column
- `update`: Update MarketingBudget values
- `updateusingdml`: Update values using DML
- `updateusingpartitioneddml`: Update values using Partitioned DML
- `querynewcolumn` / `querymarketingbudget`: Query rows with the new column
- `addstoringindex`: Create a storing index
- `readstoringindex`: Read rows using a storing index
- `readonlytransaction`: Execute a read-only transaction
- `deleteusingpartitioneddml`: Delete rows using Partitioned DML

### PostgreSQL Dialect (`pg_spanner_sample`)

```bash
cargo run -p spanner-samples --bin pg_spanner_sample -- <command> <instance-id> <database-id>
```

#### Example Commands

- `createpgdatabase` / `createdatabase`: Provision a PostgreSQL-dialect database
  and initial schema
- `write`: Insert initial data into Singers and Albums
- `writeusingdml` / `insertusingdml`: Insert rows into Singers table using DML
- `querysingerstable` / `query`: Query Singers table
- `querywithparameter`: Query Singers table using a parameter
- `addindex` / `createindex`: Create an index
- `readindex`: Read rows using an index
- `addcolumn` / `addmarketingbudget`: Add a MarketingBudget column
- `update`: Update MarketingBudget values
- `updateusingdml`: Update values using DML
- `updateusingpartitioneddml`: Update values using Partitioned DML
- `querynewcolumn` / `querymarketingbudget`: Query rows with the new column
- `addstoringindex`: Create a storing index
- `readstoringindex`: Read rows using a storing index
- `readonlytransaction`: Execute a read-only transaction
- `deleteusingpartitioneddml`: Delete rows using Partitioned DML
