output "instance_id" {
  description = "The pipeline host. This is the SSM session target."
  value       = aws_instance.host.id
}

output "private_ip" {
  description = "Private address. There is no public one, by design."
  value       = aws_instance.host.private_ip
}

output "availability_zone" {
  description = "AZ chosen because it offers the requested instance type."
  value       = local.az
}

output "ami_id" {
  description = "AMI the host launched from (pinned; see the ignore_changes note)."
  value       = data.aws_ami.host.id
}

output "kms_key_arn" {
  description = "CMK encrypting the volumes, logs, and SSM sessions."
  value       = aws_kms_key.main.arn
}

output "shell_command" {
  description = "Open a shell on the host. No SSH key, no open port."
  value       = "aws ssm start-session --region ${local.region} --target ${aws_instance.host.id}"
}

output "ui_tunnel_command" {
  description = <<-EOT
    Forward the Omnigent UI to localhost:6767 over SSM, then browse
    http://localhost:6767. Keep the server bound to 127.0.0.1 on the host:
    the pipeline runner sends NO bearer token, so a server it can talk to
    cannot be requiring one, and an exposed port is unauthenticated remote
    code execution.
  EOT
  value = join(" ", [
    "aws ssm start-session",
    "--region ${local.region}",
    "--target ${aws_instance.host.id}",
    "--document-name AWS-StartPortForwardingSession",
    "--parameters '{\"portNumber\":[\"6767\"],\"localPortNumber\":[\"6767\"]}'",
  ])
}

output "flow_log_group" {
  description = "VPC flow logs (null when disabled)."
  value       = one(aws_cloudwatch_log_group.flow[*].name)
}
