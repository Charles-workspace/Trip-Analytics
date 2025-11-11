terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.83"
    }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user         = var.snowflake_user
  role         = var.snowflake_role
  private_key = file("~/.ssh/snowflake_key.pem")
  private_key_passphrase = ""
  authenticator          = "SNOWFLAKE_JWT"
}

resource "snowflake_database" "inbound_integration" {
  name    = "INBOUND_INTEGRATION"
  comment = "Database to hold source data"
}

resource "snowflake_database" "outbound_integration" {
  name    = "OUTBOUND_INTEGRATION"
  comment = "Database to hold processed data"
}

resource "snowflake_database" "ops" {
  name    = "OPS"
  comment = "Database to docker image of projects"
}