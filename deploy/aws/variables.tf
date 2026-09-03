variable "name" {
  description = "Name prefix for every resource, and the Project tag."
  type        = string
  default     = "omnigent"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,30}$", var.name))
    error_message = "name must be lowercase alphanumeric/hyphen, 2-31 chars."
  }
}

variable "region" {
  description = "AWS region. Must offer the chosen instance type."
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = <<-EOT
    EC2 instance type for the pipeline host.

    sbx runs each agent in a microVM launched by KVM, so the host needs real
    processor virtualization extensions. That used to mean bare metal. Since
    February 2026 it does not: Nitro passes Intel VT-x through to ORDINARY
    virtual instances when nested virtualization is enabled, which this
    module turns on via cpu_options.

    So the type must be on AWS's nested-virtualization list — all Intel,
    all x86_64:

      C7i  C7i-flex  M7i  M7i-flex  R7i  I7i
      C8i  C8i-flex  M8i  M8i-flex  R8i  R8i-flex  X8i
      C8id M8id      R8id

    Anything else (Graviton, T-family, older generations) cannot start a
    sandbox at all. A `.metal` type also works — it has the extensions
    natively — but set enable_nested_virtualization = false with it.

    Sizing from measured behaviour, not guesswork: this pipeline peaks
    around 3-4 concurrent microVMs, and the server config should pin each to
    4 vCPU / 8 GiB (unset, a guest sees EVERY host CPU while capped at half
    the host's memory, which is how a linker gets OOM-killed). The default
    m7i.4xlarge — 16 vCPU / 64 GiB — fits four such guests with headroom for
    the host, the server and the runner.

      m7i.2xlarge   8 vCPU /  32 GiB   the literal 32 GB ask; tight at peak
      m7i.4xlarge  16 vCPU /  64 GiB   default; comfortable
      r7i.4xlarge  16 vCPU / 128 GiB   if you raise per-guest memory

    AWS notes that latency-sensitive workloads should still evaluate bare
    metal; a build host is throughput-bound, so nested is the right trade.
  EOT
  type        = string
  default     = "m7i.4xlarge"
}

variable "enable_nested_virtualization" {
  description = <<-EOT
    Pass processor virtualization extensions into the instance. Required for
    sbx on any non-metal type — without it there is no /dev/kvm and not one
    sandbox will start.

    Set false ONLY for a `.metal` instance_type, which already has the
    extensions and rejects the option.
  EOT
  type        = bool
  default     = true
}

variable "ami_name_filter" {
  description = <<-EOT
    AMI name glob, matched against Canonical's published images.

    amd64, because every nested-virtualization-capable type is Intel. sbx on
    Linux wants Ubuntu 24.04 or later.
  EOT
  type        = string
  default     = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
}

variable "root_volume_gb" {
  description = <<-EOT
    Root EBS volume size. Everything lands here by default — the sbx image
    store, agent worktrees, and each writer's build tree — so this is the
    "500 GB" knob. Measured on a live Rust campaign: the sbx image store
    alone reached 42 GB with six microVMs up, and a single writer's build
    tree reached 12 GB.
  EOT
  type        = number
  default     = 500

  validation {
    condition     = var.root_volume_gb >= 100
    error_message = "root_volume_gb must be at least 100."
  }
}

variable "data_volume_gb" {
  description = <<-EOT
    Optional SEPARATE data volume mounted at /srv, for state that should
    outlive the instance (a campaign's run directory is days of work).
    0 disables it. When set, point the runner at it:
      --canonical-root /srv/omnigent/canonical
      --worktree-root  /srv/omnigent/worktrees
    and set sandbox.sbx.worktree_root to the SAME worktree path.
  EOT
  type        = number
  default     = 0

  validation {
    condition     = var.data_volume_gb == 0 || var.data_volume_gb >= 20
    error_message = "data_volume_gb must be 0 (disabled) or at least 20."
  }
}

variable "root_volume_iops" {
  description = <<-EOT
    gp3 IOPS. 3000 is INCLUDED FREE; anything above bills at $0.005 per
    IOPS-month, so the 6000 this module used to hard-code was $15/month for
    a build volume. Raise it only if you measure the volume as the
    bottleneck.
  EOT
  type        = number
  default     = 3000
}

variable "root_volume_throughput" {
  description = <<-EOT
    gp3 throughput in MB/s. 125 is INCLUDED FREE; above that bills at $0.04
    per MB/s-month (the previous 500 was another $15/month).
  EOT
  type        = number
  default     = 125
}

variable "enable_detailed_monitoring" {
  description = <<-EOT
    1-minute CloudWatch metrics instead of 5-minute. Roughly $2/month, and
    the guest's own tooling tells you more about a stuck build than this
    does. Off by default.
  EOT
  type        = bool
  default     = false
}

variable "enable_flow_logs" {
  description = "VPC flow logs. Ingestion is billed per GB."
  type        = bool
  default     = true
}

variable "flow_log_traffic_type" {
  description = <<-EOT
    ALL, ACCEPT or REJECT. Defaults to REJECT: on an egress-only host with
    no inbound path, rejected traffic is the security-relevant signal, and
    ALL on a box pulling toolchains all day is mostly log-ingestion bills.
  EOT
  type        = string
  default     = "REJECT"

  validation {
    condition     = contains(["ALL", "ACCEPT", "REJECT"], var.flow_log_traffic_type)
    error_message = "flow_log_traffic_type must be ALL, ACCEPT or REJECT."
  }
}

variable "vpc_cidr" {
  description = "CIDR for the VPC. /16 leaves room for both subnets."
  type        = string
  default     = "10.60.0.0/16"

  validation {
    condition     = can(cidrhost(var.vpc_cidr, 0))
    error_message = "vpc_cidr must be a valid IPv4 CIDR block."
  }
}

variable "publish_secret_arn" {
  description = <<-EOT
    Optional Secrets Manager ARN holding the pipeline's git publish token.
    When set, the instance role is granted read on THAT ONE ARN and nothing
    else, so --publish-token-command can be:

      aws secretsmanager get-secret-value --secret-id <arn> \
        --query SecretString --output text

    Leave null and the role gets no Secrets Manager access at all.
  EOT
  type        = string
  default     = null

  validation {
    condition = (
      var.publish_secret_arn == null ||
      can(regex("^arn:aws[a-z-]*:secretsmanager:", var.publish_secret_arn))
    )
    error_message = "publish_secret_arn must be a Secrets Manager ARN."
  }
}

variable "manage_session_preferences" {
  description = <<-EOT
    Manage the SSM-SessionManagerRunShell document (KMS-encrypted sessions
    + CloudWatch session logging).

    Default false ON PURPOSE: that document is a SINGLETON per account and
    region. Turning this on makes this module the owner of a setting shared
    by every other instance in the account, and `terraform destroy` would
    take it away from them. Enable it only in an account this stack owns.
  EOT
  type        = bool
  default     = false
}

variable "log_retention_days" {
  description = "Retention for flow logs and session logs."
  type        = number
  default     = 90
}

variable "tags" {
  description = "Extra tags merged into every resource."
  type        = map(string)
  default     = {}
}
