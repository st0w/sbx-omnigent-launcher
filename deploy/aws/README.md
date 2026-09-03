# AWS pipeline host

Terraform for one private EC2 host that runs the whole sbx-omnigent stack —
sbx daemon and its microVMs, the Omnigent server, the launcher, the pipeline
runner, and both worktree roots. Your laptop keeps only a browser.

Placement is forced rather than chosen: the launcher resolves the mount
sentinel with `realpath` on its **own** filesystem, so the process that
creates worktrees and the process that mounts them must share a disk. The
reasoning, and everything that is not AWS-specific, is in
[`../../docs/CLOUD.md`](../../docs/CLOUD.md).

## Read this before you apply

**An ordinary instance is enough — but only a specific set of them.**

sbx runs each agent in a **microVM**, not a container, and on Linux that
microVM is launched by KVM. Docker's Ubuntu install adds you to the `kvm`
group; the architecture post is titled *Why MicroVMs*. So the host genuinely
needs processor virtualization extensions, and a container running happily
on a `t3.nano` says nothing about it — a container shares the host kernel and
needs no virtualization at all, which is exactly the isolation property sbx
declines to rely on.

That requirement used to mean bare metal. **Since February 2026 it does
not.** EC2 supports nested virtualization on virtual instances: Nitro passes
Intel VT-x through to the guest, KVM is a supported L1 hypervisor, it is
available in all commercial regions, and it costs nothing extra. This module
turns it on with `cpu_options { nested_virtualization = "enabled" }`.

The catch is the instance list. All Intel, all x86_64:

```
C7i  C7i-flex  M7i  M7i-flex  R7i  I7i
C8i  C8i-flex  M8i  M8i-flex  R8i  R8i-flex  X8i
C8id M8id      R8id
```

Graviton, T-family and older generations are **not** on it and cannot start
a single sandbox. That is why the AMI default is amd64.

**Sizing, from this pipeline's measured behaviour.** It peaks at 3–4
concurrent microVMs, and the server config should pin each guest to 4 vCPU /
8 GiB — left unset, a guest sees *every* host CPU while capped at half the
host's memory, which is precisely how a linker gets OOM-killed.

| | | |
| --- | --- | --- |
| `m7i.2xlarge` | 8 vCPU / 32 GiB | the literal 32 GB ask; tight at peak |
| **`m7i.4xlarge`** | **16 vCPU / 64 GiB** | **default** — four guests plus headroom |
| `r7i.4xlarge` | 16 vCPU / 128 GiB | if you raise per-guest memory |

AWS does note that latency-sensitive workloads should still evaluate bare
metal. A build host is throughput-bound, so nested is the right trade. If you
do move to `.metal`, set `enable_nested_virtualization = false` — metal has
the extensions natively and rejects the option.

**Confirm KVM on the box before installing anything else.** `sbx diagnose`
will **not** tell you: it checks the CLI, daemon, storage and auth, and
passes cheerfully on a host that cannot start one sandbox. The real check:

```bash
sudo apt-get install -y cpu-checker && kvm-ok
ls -l /dev/kvm
```

If `/dev/kvm` is missing on a supported type, nested virtualization is off.
It can be turned on after the fact, but the instance must be **stopped**:

```bash
aws ec2 modify-instance-cpu-options \
  --instance-id "$(terraform output -raw instance_id)" \
  --nested-virtualization enabled
```

## What it costs, and how to cut it

Rates below are us-east-1 on-demand, mid-2026. Verify against the pricing
calculator before believing any of it.

The instance dominates everything else, and **most of the bill is hours you
are not using.** This workload is bursty and interactive — a campaign blocks
on a human approving a plan, and you spend days between runs.

| | 24/7 | ~130 h/month |
| --- | ---: | ---: |
| `m7i.4xlarge` @ $0.8064/h | $589 | $105 |
| 500 GB gp3 (billed even while stopped) | $40 | $40 |
| NAT gateway @ $0.045/h (**billed regardless of the instance**) | $33 | $33 |
| Public IPv4 for the NAT @ $0.005/h | $4 | $4 |
| KMS key + flow logs | ~$3 | ~$3 |
| **Total** | **~$668** | **~$184** |

Ranked by what they actually save:

1. **Stop the instance between campaigns — ~$480/month.** By far the biggest
   lever, and it costs you nothing; EBS persists while stopped.
   ```bash
   aws ec2 stop-instances  --instance-ids "$(terraform output -raw instance_id)"
   aws ec2 start-instances --instance-ids "$(terraform output -raw instance_id)"
   ```
2. **Drop to `m7i.2xlarge` — ~$52/month at 130 h.** The literal 32 GiB ask.
   Tight at a 4-guest peak, so lower `sandbox.sbx.cpus`/`memory` with it.
3. **Spot — typically well over half off compute.** Unusually good fit here:
   the pipeline already survives interruption via `--resume`, and a
   campaign's state is on disk. Pair it with `data_volume_gb` so the run
   directory outlives a reclaim. Check your region's actual discount with
   `aws ec2 describe-spot-price-history --instance-types m7i.4xlarge`.
4. **Replace the NAT gateway with a NAT instance — ~$30/month.** A
   `t4g.nano` doing NAT is a few dollars. Adds a single point of failure to
   maintain; worth it only once the instance bill is already small.
5. **Shrink the volume.** 500 GB is $40/month forever. Tasks #7 and #8 in
   the launcher backlog (reclaim on resume, pre-baked toolchain images)
   attack the same problem from the software side.

Two defaults here are deliberately the *free* gp3 tier — 3000 IOPS and
125 MB/s — which saves $30/month over what this module originally hard-coded.
Be aware that is a real trade: **125 MB/s is slow for a workspace compile.**
If builds crawl, `root_volume_iops` and `root_volume_throughput` are the
knobs, at $0.005/IOPS-month and $0.04/MB/s-month above the free tier.

### Is AWS the right answer at all?

Worth asking honestly. Prices are list, mid-2026, for a 16 vCPU / 64 GiB
class machine running 24/7 — check them yourself before deciding.

| | compute/mo | notes |
| --- | ---: | --- |
| **AWS** `m7i.4xlarge` | $589 | + ~$80 EBS/NAT/IPv4 → **~$668** |
| **GCP** `n2-standard-16` | $567 | + disk and Cloud NAT → **~$650** |
| **Hetzner** AX102 | **~$136** | Ryzen 9 7950X3D 16c/32t, **128 GB**, 2×1.92 TB NVMe |
| **Hetzner** AX41-NVMe | **~$50** | Ryzen 5 3600 6c/12t, 64 GB ECC, 2×512 GB NVMe |
| **OVHcloud** bare metal | ~$67+ | 64 GB / NVMe tiers exist; pricing varies — verify |
| ~~Equinix Metal~~ | — | **shut down 30 June 2026** |

Three things fall out of that:

**GCP saves you nothing.** `n2-standard-16` is within a few percent of the
AWS instance, so the earlier suggestion to move there for cost was wrong.
It is a fine platform, just not a cheaper one. (Its nested virtualization
has improved, though: `enableNestedVirtualization` is now a field you set
on the VM directly — no custom image or `enable-vmx` license needed. It
requires Intel Haswell or newer and is unavailable on E2, memory-optimized,
AMD and Arm machine types.)

**Equinix Metal is gone**, sunset 30 June 2026. Don't build on it.

**Dedicated hardware wins on both axes, not just price.** Hetzner's AX102 is
about a fifth the cost *and* better suited: a 7950X3D's cores are far
quicker than a shared-tenancy cloud vCPU for compiles, 128 GB doubles the
RAM, and 3.8 TB of local NVMe reads in GB/s where the gp3 volume here is
tuned to 125 MB/s. It is also real bare metal, so the whole
nested-virtualization question disappears.

What you give up going that route:

- **Session Manager.** Use Tailscale or WireGuard instead — `docs/CLOUD.md`
  covers the options, and the "no inbound port" posture survives intact.
- **Managed KMS, IAM and Secrets Manager.** The publish token needs another
  home; `--publish-token-command` takes any command, so `pass`, libsecret or
  a self-hosted Vault all work.
- **Elasticity.** A dedicated box is a monthly commitment (plus a one-time
  setup fee), so "stop it when idle" no longer saves anything — but at $136
  flat it does not need to.
- **Data residency and compliance.** Worth a deliberate decision for a
  security product, not an afterthought.

**Your own hardware** remains the cheapest of all. The stack already worked
on your laptop; the only blockers were host RAM and disk. A box with 64 GB
and a 2 TB NVMe is a one-time cost that beats even Hetzner within a year,
with the fastest disk of any option here.

## What it builds

| | |
| --- | --- |
| VPC | private subnet for the host; public subnet holds only the NAT gateway |
| Egress | NAT gateway — agents need package mirrors, toolchains, model APIs |
| Access | **Session Manager only.** No public IP, no key pair, no inbound rule |
| SSM | interface endpoints for `ssm`/`ssmmessages`/`ec2messages`, so control traffic never leaves the VPC; S3 gateway endpoint keeps agent downloads off the NAT |
| Storage | 500 GB encrypted gp3 root (6000 IOPS / 500 MB/s), optional separate persistent volume at `/srv` |
| Keys | one customer-managed KMS key, rotation on, covering volumes, logs and sessions |
| IAM | `AmazonSSMManagedInstanceCore` and nothing else, plus read on exactly one Secrets Manager ARN if you set `publish_secret_arn` |
| Audit | VPC flow logs to an encrypted log group |

## Prerequisites

- AWS credentials in the environment (SSO, a named profile, an instance
  profile) — never in a `.tf` file.
- The **Session Manager plugin** for the AWS CLI. Without it
  `aws ssm start-session` fails with a plugin-not-found error, and there is
  no other way in.

## Use it

```bash
cp terraform.tfvars.example terraform.tfvars   # then edit
terraform init
terraform plan
terraform apply

aws ssm start-session --target "$(terraform output -raw instance_id)"
```

`terraform output shell_command` and `ui_tunnel_command` print the exact
invocations, region included.

## After apply — what Terraform cannot do for you

Everything below ends in an interactive login, so it happens once, over a
Session Manager shell. Details in `docs/CLOUD.md`.

1. **Confirm KVM, then install sbx.** `kvm-ok` and `ls -l /dev/kvm`
   first — `sbx diagnose` will pass on a host that cannot start a
   single sandbox, because it never checks virtualization.
2. **Re-apply the sbx network policy.** The allowlist is host state and does
   not travel with a git clone: package mirrors, language toolchain hosts,
   and your git forge all need `sbx policy allow network <host>:<port>`.
3. **Authenticate the coding harness** on this host.
4. **Bootstrap the Antigravity trusted box**, pointing its workspace at a
   persistent path — the default lives under the system temp directory, and
   a host that clears that on boot strands it:
   ```bash
   omni-sbx-agy bootstrap --workspace /srv/omnigent/agy-ws
   sbx exec -it agy-auth-trusted agy      # then /login
   ```
   The consent URL must open in a browser that can reach the callback, so
   forward that port over the same SSM session.
5. **Pin the microVM size** in the server config. Both keys *derive from the
   host* when unset — every guest sees every host CPU while its memory is
   capped at half the host, which is how a linker gets OOM-killed on a
   64-core box:
   ```yaml
   sandbox:
     sbx:
       cpus: 8
       memory: '16g'
       worktree_root: /srv/omnigent/worktrees   # must equal --worktree-root
   ```
6. **Move the publish token off the desktop keychain.** Put it in Secrets
   Manager, set `publish_secret_arn`, and use:
   ```bash
   --publish-token-command 'aws secretsmanager get-secret-value \
     --secret-id <arn> --query SecretString --output text'
   ```
7. **Run the runner under tmux or systemd.** A dropped session otherwise
   kills a campaign mid-flight; `--resume` recovers, but an interrupted
   review round is re-run from scratch.

## Security posture

- **No inbound path exists.** The host security group has zero ingress
  rules — not a narrowed CIDR, none. Session Manager works because the
  agent dials out.
- **IMDSv2 required**, hop limit 1. Agent code runs on this box; a v1
  metadata endpoint is exactly what an SSRF would use to read the instance
  role's credentials, and a hop limit of 1 keeps a container from reaching
  it at all.
- **Everything at rest is encrypted with a rotating CMK** — root volume,
  data volume, flow logs, session logs. The key policy grants the host
  `Decrypt`/`GenerateDataKey` only; it can never administer the key.
- **Least privilege.** One AWS-managed SSM policy. The optional secret grant
  names a single ARN, never `*`.
- **Endpoint access is by security group reference**, not CIDR — anything
  else that ever lands in the subnet still cannot reach the SSM endpoints.
- **No secrets in `user_data`.** It is plaintext to anyone who can call
  `ec2:DescribeInstanceAttribute`; it only prepares directories and a mount.
- **Flow logs on**, to an encrypted group.

Two things left deliberately to you: `manage_session_preferences` is off
because `SSM-SessionManagerRunShell` is an account-wide singleton and this
module should not silently own a setting shared with every other instance;
and the state backend is commented out because remote state is an
organizational choice.

## Destroying it

`prevent_destroy` guards the optional data volume — the one resource whose
entire purpose is outliving the instance. Remove that lifecycle block
consciously if you really mean it.

The root volume is *not* guarded and holds the run directory. Get anything
you care about off it first; a campaign's `state.json` and its reviewer
reports are days of work.
