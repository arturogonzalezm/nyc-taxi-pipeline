terraform {
  required_version ">= 1.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }
}

provider "google" {
  # Uses ADC from:
  # gcloud auth application-default login

}
