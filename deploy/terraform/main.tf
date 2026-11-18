terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.0" }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { Project = "llm-gateway", Course = "CS6650" }
  }
}

variable "region" {
  type    = string
  default = "us-west-2"
}

variable "gateway_desired_count" {
  type    = number
  default = 2
}
