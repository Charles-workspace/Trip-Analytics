variable "snowflake_account_name" {
  type = string
}

variable "snowflake_organization_name" {
  type = string
}

variable "snowflake_user" {
  type = string
}

variable "snowflake_password" {
  type = string
  sensitive = true
}

variable "snowflake_role" {
  type = string
}