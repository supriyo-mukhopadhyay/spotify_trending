variable "project" {
  type        = string
  description = "Project name"
}

variable "region" {
  type        = string
  description = "AWS region"
}

variable "vpc_id" {
  type        = string
  description = "VPC ID"
}

variable "public_subnet_a_id" {
  type        = string
  description = "Public subnet A ID"
}

variable "spapi_bucket_name" {
  type        = string
  description = "Mqtt bucket"
}
