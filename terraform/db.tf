terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.83"
    }
  }
}

provider "snowflake" {
  account_name      = var.snowflake_account_name
  organization_name = var.snowflake_organization_name
  user          = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role

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