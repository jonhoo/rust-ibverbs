terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.0"
    }
  }
}

provider "aws" {
  region = "us-west-1"
}

resource "aws_vpc" "rust_ibverbs" {
  cidr_block = "10.0.0.0/24"
}

locals {
  ubuntu_focal = {
    aws_id = "099720109477" # canonical
    ami_name = "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"
  }
  debian_buster = {
    aws_id = "099720109477" # debian
    ami_name = "ubuntu/images/hvm-ssd/ubuntu-focal-20.04-amd64-server-*"
  }
}

resource "aws_instance" "ubuntu" {
  most_recent = true

  filter {
    name = "name"
    values = [local.ubuntu_focal.ami_name]
  }

  filter {
    name = "virtualization-type"
    values = ["hvm"]
  }

  name = "rust_ibverbs_ci"
  ami = ""
  instance_type = ""
}