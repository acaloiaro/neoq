# Migrations

Migrations are implemented with https://github.com/golang-migrate/migrate 

## Install the CLI

To add new migrations, install the CLI. The version of the CLI is not particularly important. 

`go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@v4.16.2`

## Adding migrations 

Adding new migrations is done with the `migrate create` command 

`migrate create -dir backends/postgres/migrations -ext sql <descriptive_migration_name>`

## Running migrations 

Migrations are run every time `neoq` initializes the `postgres` backend. There is no need to run migrations explicitly.