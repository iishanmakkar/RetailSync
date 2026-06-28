variable "project_name" {
  type        = string
  description = "Project Name"

  default = "retailsync"
}

variable "environment" {
  type        = string
  description = "Environment"

  default = "dev"
}

variable "aws_region" {
  type        = string
  description = "AWS Region"

  default = "us-east-1"
}