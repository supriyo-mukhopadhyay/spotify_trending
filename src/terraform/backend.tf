terraform {
  backend "local" {
    path            = "/home/coder/.local/share/code-server/User/terraform.tfstate"
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}


# Configure the AWS Provider
provider "aws" {
  region = "eu-west-1"
}