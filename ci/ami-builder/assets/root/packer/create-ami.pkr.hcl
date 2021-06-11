variable "ami_label" {
  type = string
  default = "rust_ibverbs-{{timestamp}}"
}

variable "aws_access_key_id" {
  type = string
  default = ""
}

variable "aws_secret_access_key" {
  type = string
  default = ""
}

variable "aws_region" {
  type = string
  default = "us-west-1"
}

source "amazon-ebssurrogate" "generated" {
  access_key = var.aws_access_key_id
  ami_description = "rust_ibverbs CI AMI"
  ami_name = var.ami_label
  ami_regions = [
    var.aws_region,
  ]
  skip_region_validation = true
  ami_root_device {
    delete_on_termination = true
    device_name = "/dev/xvda"
    source_device_name = "/dev/xvdf"
    volume_size = 32
    volume_type = "gp2"
  }
  ami_virtualization_type = "hvm"
  associate_public_ip_address = true
  instance_type = "t2.micro"
  launch_block_device_mappings {
    delete_on_termination = true
    device_name = "/dev/xvdf"
    volume_size = 32
    volume_type = "gp2"
  }
  secret_key = var.aws_secret_access_key
  source_ami_filter {
    filters = {
      name = "*debian-10-amd64-*"
      root-device-type = "ebs"
      virtualization-type = "hvm"
    }
    most_recent = true
    owners = [
      "136693071363", # debian aws id
    ]
  }
  ssh_pty = true
  ssh_timeout = "5m"
  ssh_username = "admin"
}

build {
  sources = [
    "source.amazon-ebssurrogate.generated"
  ]

  provisioner "file" {
    source = "/tmp/rust_ibverbs.img.zst"
    destination = "/tmp/rust_ibverbs.img.zst"
  }

  provisioner "file" {
    source = "provision-image.sh"
    destination = "/tmp/provision-image.sh"
  }

  provisioner "shell" {
    script = "install-image.sh"
    skip_clean = true
    start_retry_timeout = "5m"
  }

  post-processor "manifest" {
    output = "manifest.json"
  }

}
