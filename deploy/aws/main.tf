##############################################################################
# A single private build host for the sbx-omnigent pipeline.
#
# Shape, and why:
#   * ONE machine. The launcher resolves the mount sentinel with realpath on
#     its OWN filesystem, so the process that creates worktrees and the
#     process that mounts them must share a disk. Runner, server, launcher
#     and both roots therefore live together. See docs/CLOUD.md.
#   * NO public IP, NO SSH key, NO inbound rule of any kind. Access is
#     Session Manager only, which dials OUT to SSM — nothing dials in.
#   * A NAT gateway, because the agents genuinely need the internet: package
#     mirrors, language toolchains, the model API. SSM itself does not use
#     it; interface endpoints keep that traffic inside the VPC.
##############################################################################

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Not every instance type is offered in every AZ. Pin the subnet to one
# that actually has it rather than discovering that at apply time.
data "aws_ec2_instance_type_offerings" "host" {
  location_type = "availability-zone"

  filter {
    name   = "instance-type"
    values = [var.instance_type]
  }
}

data "aws_ami" "host" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = [var.ami_name_filter]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

locals {
  az            = sort(data.aws_ec2_instance_type_offerings.host.locations)[0]
  account_id    = data.aws_caller_identity.current.account_id
  region        = data.aws_region.current.region
  data_volume   = var.data_volume_gb > 0
  log_group_arn = "arn:aws:logs:${local.region}:${local.account_id}:log-group:*"
}

##############################################################################
# KMS — one customer-managed key for every at-rest artifact this stack makes.
##############################################################################

data "aws_iam_policy_document" "kms" {
  # Without this the key is orphaned: nobody, including an account admin,
  # could ever administer or delete it.
  statement {
    sid       = "AccountRoot"
    effect    = "Allow"
    actions   = ["kms:*"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${local.account_id}:root"]
    }
  }

  # CloudWatch Logs encrypts each log group with a data key; the condition
  # scopes that to log groups in THIS account and region, so the grant
  # cannot be borrowed for someone else's log group.
  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "kms:Encrypt*",
      "kms:Decrypt*",
      "kms:ReEncrypt*",
      "kms:GenerateDataKey*",
      "kms:Describe*",
    ]
    resources = ["*"]

    principals {
      type        = "Service"
      identifiers = ["logs.${local.region}.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = [local.log_group_arn]
    }
  }

  # The host needs the key to read its own encrypted volumes and to take
  # part in an encrypted Session Manager session. Decrypt + GenerateDataKey
  # only — it can never administer the key.
  statement {
    sid       = "HostUse"
    effect    = "Allow"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = ["*"]

    principals {
      type        = "AWS"
      identifiers = [aws_iam_role.host.arn]
    }
  }
}

resource "aws_kms_key" "main" {
  description             = "${var.name} pipeline host: EBS, logs, SSM sessions"
  enable_key_rotation     = true
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.kms.json
}

resource "aws_kms_alias" "main" {
  name          = "alias/${var.name}-host"
  target_key_id = aws_kms_key.main.key_id
}

##############################################################################
# Network
##############################################################################

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_support   = true # required for interface endpoints
  enable_dns_hostnames = true

  tags = { Name = "${var.name}-vpc" }
}

# Public subnet exists only to hold the NAT gateway. Nothing runs in it.
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 0)
  availability_zone       = local.az
  map_public_ip_on_launch = false # nothing here should ever get one implicitly

  tags = { Name = "${var.name}-public" }
}

resource "aws_subnet" "private" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 1)
  availability_zone       = local.az
  map_public_ip_on_launch = false

  tags = { Name = "${var.name}-private" }

  lifecycle {
    precondition {
      condition     = length(data.aws_ec2_instance_type_offerings.host.locations) > 0
      error_message = <<-EOT
        No availability zone in ${var.region} offers ${var.instance_type}.
        Pick another type or region — and keep it on the nested-virtualization
        list, or sbx cannot start a sandbox at all. See the instance_type
        variable for that list.
      EOT
    }
  }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.name}-igw" }
}

resource "aws_eip" "nat" {
  domain     = "vpc"
  depends_on = [aws_internet_gateway.main]
  tags       = { Name = "${var.name}-nat" }
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id
  depends_on    = [aws_internet_gateway.main]

  tags = { Name = "${var.name}-nat" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.name}-public" }
}

resource "aws_route" "public_default" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.name}-private" }
}

# Outbound only. Nothing can route back IN through a NAT gateway.
resource "aws_route" "private_default" {
  route_table_id         = aws_route_table.private.id
  destination_cidr_block = "0.0.0.0/0"
  nat_gateway_id         = aws_nat_gateway.main.id
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}

##############################################################################
# Security groups
#
# The host group has NO ingress rules at all — not a narrowed CIDR, none.
# Session Manager needs none, because the agent opens the connection.
##############################################################################

resource "aws_security_group" "host" {
  name        = "${var.name}-host"
  description = "Pipeline host: egress only, no inbound path whatsoever"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.name}-host" }

  lifecycle {
    create_before_destroy = true
  }
}

# Agents fetch toolchains, packages and model APIs. Ports, not "all traffic":
# the sbx per-sandbox policy is the fine-grained control, this is the floor.
resource "aws_vpc_security_group_egress_rule" "host_https" {
  security_group_id = aws_security_group.host.id
  description       = "Package registries, git forge, model APIs"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

resource "aws_vpc_security_group_egress_rule" "host_http" {
  security_group_id = aws_security_group.host.id
  description       = "apt/deb mirrors that still serve over plain HTTP"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 80
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "host_dns_udp" {
  security_group_id = aws_security_group.host.id
  description       = "VPC resolver"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "udp"
  from_port         = 53
  to_port           = 53
}

resource "aws_vpc_security_group_egress_rule" "host_dns_tcp" {
  security_group_id = aws_security_group.host.id
  description       = "VPC resolver (truncated responses)"
  cidr_ipv4         = var.vpc_cidr
  ip_protocol       = "tcp"
  from_port         = 53
  to_port           = 53
}

resource "aws_security_group" "endpoints" {
  name        = "${var.name}-endpoints"
  description = "SSM interface endpoints: 443 from the host only"
  vpc_id      = aws_vpc.main.id

  tags = { Name = "${var.name}-endpoints" }

  lifecycle {
    create_before_destroy = true
  }
}

# Sourced from the host security GROUP, not a CIDR: anything else that ever
# lands in this subnet still cannot reach the endpoints.
resource "aws_vpc_security_group_ingress_rule" "endpoints_https" {
  security_group_id            = aws_security_group.endpoints.id
  description                  = "HTTPS from the pipeline host"
  referenced_security_group_id = aws_security_group.host.id
  ip_protocol                  = "tcp"
  from_port                    = 443
  to_port                      = 443
}

##############################################################################
# VPC endpoints — SSM control traffic never leaves the VPC.
##############################################################################

resource "aws_vpc_endpoint" "ssm" {
  for_each = toset(["ssm", "ssmmessages", "ec2messages"])

  vpc_id              = aws_vpc.main.id
  service_name        = "com.amazonaws.${local.region}.${each.key}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = [aws_subnet.private.id]
  security_group_ids  = [aws_security_group.endpoints.id]
  private_dns_enabled = true

  tags = { Name = "${var.name}-${each.key}" }
}

# Gateway endpoint: free, and keeps SSM agent/patch downloads off the NAT.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${local.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.private.id]

  tags = { Name = "${var.name}-s3" }
}

##############################################################################
# IAM — the host gets Session Manager and nothing else.
##############################################################################

data "aws_iam_policy_document" "host_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "host" {
  name               = "${var.name}-host"
  assume_role_policy = data.aws_iam_policy_document.host_assume.json
}

# AWS's minimum for Session Manager. Deliberately NOT paired with
# AmazonSSMFullAccess or an EC2 wildcard.
resource "aws_iam_role_policy_attachment" "ssm_core" {
  role       = aws_iam_role.host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

data "aws_iam_policy_document" "publish_secret" {
  count = var.publish_secret_arn == null ? 0 : 1

  statement {
    effect    = "Allow"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [var.publish_secret_arn] # exactly one secret, never "*"
  }
}

resource "aws_iam_role_policy" "publish_secret" {
  count = var.publish_secret_arn == null ? 0 : 1

  name   = "${var.name}-publish-token"
  role   = aws_iam_role.host.id
  policy = data.aws_iam_policy_document.publish_secret[0].json
}

resource "aws_iam_instance_profile" "host" {
  name = "${var.name}-host"
  role = aws_iam_role.host.name
}

##############################################################################
# Logging
##############################################################################

resource "aws_cloudwatch_log_group" "flow" {
  count = var.enable_flow_logs ? 1 : 0

  name              = "/${var.name}/vpc-flow-logs"
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.main.arn
}

data "aws_iam_policy_document" "flow_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["vpc-flow-logs.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "flow" {
  count = var.enable_flow_logs ? 1 : 0

  name               = "${var.name}-flow-logs"
  assume_role_policy = data.aws_iam_policy_document.flow_assume.json
}

data "aws_iam_policy_document" "flow" {
  count = var.enable_flow_logs ? 1 : 0

  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogStreams",
    ]
    resources = ["${aws_cloudwatch_log_group.flow[0].arn}:*"]
  }
}

resource "aws_iam_role_policy" "flow" {
  count = var.enable_flow_logs ? 1 : 0

  name   = "${var.name}-flow-logs"
  role   = aws_iam_role.flow[0].id
  policy = data.aws_iam_policy_document.flow[0].json
}

resource "aws_flow_log" "main" {
  count = var.enable_flow_logs ? 1 : 0

  vpc_id               = aws_vpc.main.id
  traffic_type         = var.flow_log_traffic_type
  log_destination_type = "cloud-watch-logs"
  log_destination      = aws_cloudwatch_log_group.flow[0].arn
  iam_role_arn         = aws_iam_role.flow[0].arn
  # 600s rather than 60s: the same information in fewer, larger records,
  # and log ingestion is billed per GB.
  max_aggregation_interval = 600
}

resource "aws_cloudwatch_log_group" "sessions" {
  count = var.manage_session_preferences ? 1 : 0

  name              = "/${var.name}/ssm-sessions"
  retention_in_days = var.log_retention_days
  kms_key_id        = aws_kms_key.main.arn
}

# ACCOUNT-WIDE SINGLETON — see the manage_session_preferences variable.
resource "aws_ssm_document" "session_preferences" {
  count = var.manage_session_preferences ? 1 : 0

  name            = "SSM-SessionManagerRunShell"
  document_type   = "Session"
  document_format = "JSON"

  content = jsonencode({
    schemaVersion = "1.0"
    description   = "Session Manager preferences: encrypted and logged"
    sessionType   = "Standard_Stream"
    inputs = {
      kmsKeyId                    = aws_kms_key.main.arn
      cloudWatchLogGroupName      = aws_cloudwatch_log_group.sessions[0].name
      cloudWatchEncryptionEnabled = true
      cloudWatchStreamingEnabled  = true
      idleSessionTimeout          = "60"
      shellProfile = {
        linux = "cd /srv 2>/dev/null || cd ~"
      }
    }
  })
}

##############################################################################
# Storage + host
##############################################################################

resource "aws_ebs_volume" "data" {
  count = local.data_volume ? 1 : 0

  availability_zone = local.az
  size              = var.data_volume_gb
  type              = "gp3"
  iops              = var.root_volume_iops
  throughput        = var.root_volume_throughput
  encrypted         = true
  kms_key_id        = aws_kms_key.main.arn

  tags = { Name = "${var.name}-data" }

  # This is the volume whose whole purpose is outliving the instance.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_volume_attachment" "data" {
  count = local.data_volume ? 1 : 0

  device_name = "/dev/sdf"
  volume_id   = aws_ebs_volume.data[0].id
  instance_id = aws_instance.host.id
}

resource "aws_instance" "host" {
  ami                    = data.aws_ami.host.id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.private.id
  vpc_security_group_ids = [aws_security_group.host.id]
  iam_instance_profile   = aws_iam_instance_profile.host.name
  monitoring             = var.enable_detailed_monitoring

  # THE line that lets this be an ordinary instance instead of bare metal.
  # sbx runs each agent in a microVM launched by KVM, so the host needs real
  # processor virtualization extensions. Since Feb 2026 Nitro passes Intel
  # VT-x through to virtual instances, so a normal m7i/r7i/c7i-class box
  # exposes /dev/kvm. Set enable_nested_virtualization = false ONLY if you
  # deliberately move to a .metal type, which has the extensions natively
  # and rejects the option.
  dynamic "cpu_options" {
    for_each = var.enable_nested_virtualization ? [1] : []

    content {
      nested_virtualization = "enabled"
    }
  }

  # The whole point: reachable only through SSM.
  associate_public_ip_address = false
  key_name                    = null # no SSH key pair, ever

  # IMDSv2 only. A token-less v1 request is exactly what an SSRF in an
  # agent's own code would use to read this role's credentials; hop limit 1
  # stops a container on the box from reaching it at all.
  metadata_options {
    http_endpoint               = "enabled" # the SSM agent needs it
    http_tokens                 = "required"
    http_put_response_hop_limit = 1
    instance_metadata_tags      = "disabled"
  }

  root_block_device {
    volume_size           = var.root_volume_gb
    volume_type           = "gp3"
    iops                  = var.root_volume_iops
    throughput            = var.root_volume_throughput
    encrypted             = true
    kms_key_id            = aws_kms_key.main.arn
    delete_on_termination = true

    tags = { Name = "${var.name}-root" }
  }

  # No secrets here: user_data is plaintext to anyone who can call
  # DescribeInstanceAttribute. It only prepares directories and the mount.
  user_data = templatefile("${path.module}/user_data.sh.tftpl", {
    data_volume_id = local.data_volume ? aws_ebs_volume.data[0].id : ""
  })

  tags = { Name = "${var.name}-host" }

  lifecycle {
    # Canonical publishes new images constantly and `most_recent` would
    # follow them — which on the next apply would DESTROY a host holding
    # days of campaign state. Roll the AMI deliberately: bump it, plan,
    # and confirm the run directory is safe first.
    ignore_changes = [ami]
  }
}
