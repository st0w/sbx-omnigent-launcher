# Running on a cloud VM

Pipelines are disk- and memory-hungry: every agent gets its own microVM, every
writer builds into its own host worktree, and a module's peak is measured in
tens of gigabytes. On a laptop that competes with everything else you are doing.
Moving the stack to a dedicated machine removes that contention — but it
has one hard placement constraint and several things that quietly assume a
desktop. This document covers both.

## Everything runs on ONE machine

The sbx daemon, the Omnigent server, the launcher, the runner, and the worktree
roots all live on the same host. That is not a convenience — it is enforced by
how mounts work:

- The runner hands the launcher a **mount sentinel** naming a host path
  (`git@sbxmount:/srv/worktrees/<run>/nodes/<node>#rw`).
- `SbxLauncher._resolve_worktree_path` resolves that path **on its own
  filesystem**, requires it to be an existing directory, and refuses anything
  outside the configured `sandbox.sbx.worktree_root`.

So the process that *creates* the worktrees and the process that *mounts* them
must see the same filesystem. `--worktree-root` (runner) and
`sandbox.sbx.worktree_root` (server) must name the same directory.

Only two things stay off the box:

| Component | Where | Why |
| --- | --- | --- |
| sbx daemon + agent microVMs | the VM | this is what needs the RAM and disk |
| Omnigent server + launcher | the VM | the launcher shells out to `sbx` |
| Pipeline runner | the VM | writes the worktrees the launcher mounts |
| Worktree + canonical roots | the VM's disk | same constraint |
| `agy-auth-trusted` box | the VM | it is an sbx sandbox like any other |
| **The Omnigent UI** | **your browser** | reached over a tunnel (see below) |
| **Your git remote** | **unchanged** | the runner pushes to it over HTTPS |

## Why not Lambda / Cloud Run / Fargate

Serverless is not a fit, for reasons that are structural rather than tunable:

- **sbx boots real microVMs.** Its install ships a Linux kernel, an initrd, and
  a containerd shim; it needs hardware virtualization. Serverless runtimes are
  themselves guests (Firecracker, gVisor) and do not expose `/dev/kvm`.
- **Runs last hours**, not minutes — a multi-module campaign runs for a day or
  more, with a human approving plans partway through.
- **A large persistent writable filesystem is load-bearing.** The runner clones
  and commits into worktrees the launcher bind-mounts; a single compiled node's
  tree can exceed 8 GB. Ephemeral container disk cannot hold it, and a compile
  over a network filesystem is painfully slow.
- **The server holds long-lived SSE streams and in-memory session state.**
  Scale-to-zero or multi-instance routing kills a turn mid-flight.
- **The trusted Antigravity box is persistent state** that must survive between
  runs.

Use a VM (or bare metal). One of them.

## Machine shape

**Ready-made for AWS:** [`deploy/aws/`](../deploy/aws/) is Terraform for
exactly this shape — one private host with nested virtualization enabled,
Session Manager access, no inbound rule, encrypted 500 GB volume.

### Virtualization

sbx runs each agent in a **microVM**, not a container, and on Linux that
microVM is launched by KVM — Docker's Ubuntu install adds you to the `kvm`
group, and the architecture post is titled *Why MicroVMs*. The host
therefore needs real processor virtualization extensions.

"Docker runs fine on a small instance" is not evidence about this. A
container shares the host kernel and needs no virtualization at all, which
is precisely the isolation property sbx declines to rely on.

That requirement does **not** imply bare metal any more. Older guides
(including Arm's, which flatly says *"KVM requires bare metal and does not
work on virtual machines"*) predate **nested virtualization** being widely
available, and that is now the normal answer on every major cloud:

- **AWS** — since **February 2026**, nested virtualization on ordinary
  virtual instances, all commercial regions, no extra cost. Enabled as a CPU
  option (`--cpu-options NestedVirtualization=enabled`, or
  `modify-instance-cpu-options` on a *stopped* instance). Restricted to the
  Intel/x86_64 list — C7i/M7i/R7i/I7i, the 8i families and their flex/d
  variants — so **Graviton and T-family will not work**. Terraform in
  [`deploy/aws/`](../deploy/aws/) sets it.
- **GCP** — nested virtualization on **N2** and similar Intel families
  (**not** E2 or N2D). Minimum CPU platform Intel Haswell, AMD unsupported,
  x86_64 only, and the image needs the free `enable-vmx` license.
- **Azure** — Dv3/Ev3 and later.
- **Bare metal** (Hetzner, OVHcloud, …) — always works, no flag needed, and
  worth evaluating if the workload turns out to be latency-sensitive; AWS
  says as much. A build host is throughput-bound, so nested is normally
  fine. It is also **far cheaper**: see the cost comparison in
  [`deploy/aws/README.md`](../deploy/aws/README.md#is-aws-the-right-answer-at-all),
  where a dedicated box lands around a fifth of the equivalent cloud
  instance with better cores and local NVMe. (Equinix Metal is no longer an
  option — it shut down on 30 June 2026.)

Verify on the target host **before** installing anything:

```bash
sudo apt-get install -y cpu-checker && kvm-ok
ls -l /dev/kvm
```

Do not rely on `sbx diagnose` for this — it checks the CLI, daemon, storage
and authentication, and passes happily on a host that cannot start a single
sandbox. A machine without KVM fails at the first agent, not at
provisioning time.

### Sizing

Start from the pipeline's own preflight (`preflight_disk`) and then add margin,
because the per-unit defaults are deliberately conservative rather than
accurate for every project:

- **Disk** — the preflight's estimate is a floor. A compiled project's build
  trees dominate: a single node's `target/` can pass 8 GB, and a coverage gate
  produces a second full tree. The sbx image store grows with concurrent
  sandboxes and is reclaimed when they are removed. Provisioning several
  hundred GB is cheaper than debugging a host that filled mid-run — when the
  host fills, guest filesystems remount **read-only** and agents fail with
  opaque I/O errors rather than anything naming disk.
- **RAM** — see the next section; this is the number most likely to bite.
- **CPU** — build parallelism inside a guest defaults to its CPU count, so
  CPUs and memory are coupled. See below.

## Sizing the microVMs

Two server-config keys control what every agent sandbox gets. Both are optional
and **both default to values derived from the host**, which is why the same
pipeline behaves differently on a laptop and on a build server:

```yaml
sandbox:
  sbx:
    cpus: 4            # int; omitted/0 = every host CPU
    memory: '12g'      # string; omitted = 50% of host memory, capped at 32 GiB
```

The defaults are the trap. `cpus` unset means each guest sees **every host
CPU**, and most build tools set their job count from the CPU count — so a
workspace build runs that many compiler and linker processes at once, each
holding memory, inside a guest whose limit is only half the host. That
combination is the usual cause of an OOM-killed linker.

Two consequences worth internalizing:

- **Lowering `cpus` lowers peak memory**, because it lowers build parallelism.
  It is often the more effective knob of the two.
- **Raising `memory` beyond half the host is counterproductive** when several
  sandboxes run concurrently. Size it against *peak concurrent sandboxes*, not
  against one.

Pin both explicitly on a shared host. Leaving them to derive from the hardware
means behavior changes silently when you move machines.

Both keys are server-side: **changing them requires a server restart.**

## Reaching the UI without exposing it

The human approves plans and answers the planner in the Omnigent UI, so you
need to reach the server from your workstation. **Do not publish its port.**

Two facts make this non-negotiable:

- The server's API creates sandboxes and runs code. An open port is remote code
  execution as whoever holds it.
- **The runner does not send a bearer token.** It constructs its session client
  with no credential, so the server it talks to cannot be requiring one. (The
  `omni-sbx-swarm` CLI does support `OMNI_TOKEN`, but the pipeline runner does
  not use it.)

Keep the listener on loopback and reach it through an authenticated tunnel.
Confirm what it is bound to with `ss -lntp` (or `lsof -nP -iTCP:6767`) — you
want `127.0.0.1`, not `0.0.0.0`.

Pick whichever fits your environment:

| Approach | Notes |
| --- | --- |
| **SSH local forward** | `ssh -N -L 6767:localhost:6767 user@host`, then browse `http://localhost:6767`. Zero infrastructure; needs an open SSH path. |
| **Tailscale / WireGuard** | Device-identity mesh; no public ingress at all. Good when several people need access. |
| **GCP IAP TCP forwarding** | `gcloud compute start-iap-tunnel <vm> 6767 --local-host-port=localhost:6767`. No public IP and no open firewall port; access is IAM-governed and audited. |
| **AWS SSM Session Manager** | `aws ssm start-session --document-name AWS-StartPortForwardingSession …`. Same shape as IAP: IAM-governed, no inbound rules, no SSH keys. |
| **Azure Bastion** | Native tunneling to a VM with no public IP. |
| **Cloudflare Access / other identity proxy** | Adds SSO in front of the port. Only reasonable option if you genuinely need browser access without a client-side tunnel. |

The cloud-native options (IAP, SSM, Bastion) are worth preferring over raw SSH
where available: access is tied to an identity you already manage, revocation
is immediate, and there is an audit trail. All of them also let you drop
inbound firewall rules entirely.

If the box needs no inbound access at all, do that — every option above except
the identity proxy works with a fully closed ingress.

## Secrets on a headless host

**Publishing identity.** The runner takes `--publish-token-file` or
`--publish-token-command`; the token never appears in argv, and it is injected
only into the `git push` and PR-creation steps. `--publish-token-command` runs
an arbitrary command, so it adapts to whatever the host has:

```bash
# a file, mode 0600, owned by the run user
--publish-token-file /etc/omnigent/publish.token

# or a secret manager
--publish-token-command 'gcloud secrets versions access latest --secret=pipeline-pat'
--publish-token-command 'aws secretsmanager get-secret-value --secret-id pipeline-pat --query SecretString --output text'
--publish-token-command 'vault kv get -field=token secret/pipeline-pat'
--publish-token-command 'pass show pipeline/github-pat'
--publish-token-command 'secret-tool lookup service pipeline-pat'
```

Desktop keychain helpers do not exist on a headless server — if your current
invocation calls one, it must be replaced before the first publish, or the run
falls back to whatever identity `gh` is authenticated as.

**Scope the token to what it needs.** It pushes branches and opens pull
requests on one repository; nothing more.

## First-run setup on a fresh host

Several pieces of state live on the machine rather than in the repository, and
none of them migrate with a `git clone`.

### sbx network policy

The default allowlist does not include everything a real build needs. Package
mirrors, language toolchain hosts, and your git forge are all host-level policy
and must be re-applied:

```bash
sbx policy ls                                  # what is already allowed
sbx policy allow network <host>:<port>         # add what your builds require
```

Per-pipeline rules also come from `sandbox.sbx.egress_allow` in the server
config and from the verification gate's own allowlist; the global policy is
additive to those, not a replacement.

### Coding-harness authentication

Subscription-based harnesses authenticate per host. Complete that setup on the
new machine before the first run, or every agent turn fails unauthenticated.

### The Antigravity trusted box

This is the most awkward step on a headless host, because it ends in a browser
consent flow:

```bash
omni-sbx-agy bootstrap                # creates the trusted box, scopes egress
sbx exec -it agy-auth-trusted agy     # run '/login' inside it
```

The consent URL must open in a browser that can reach the callback. With the
SSH (or IAP/SSM) tunnel already in place, forward the callback port as well and
complete the flow in your local browser. Budget time for this; it is
interactive and cannot be scripted.

Two follow-on notes:

- The trusted box's workspace defaults to a path under the system temp
  directory. Hosts that clear that directory on boot will strand the box, and
  the harvester will report a sandbox that cannot start. Point `--workspace` at
  a **persistent** directory when bootstrapping on a server:
  `omni-sbx-agy bootstrap --workspace /var/lib/omnigent/agy-ws`.
- The runner starts a token harvester itself and stops it at the end, so there
  is no second background command to remember.

## Keeping a run alive

The runner is a foreground process that owns the whole campaign. A dropped SSH
session will take it down, and while `--resume` makes that recoverable, it is
not free — an interrupted review round is re-run from scratch.

Run it under a multiplexer or a service manager:

```bash
tmux new -s pipeline
# … start the runner, then detach with Ctrl-B D
tmux attach -t pipeline          # reattach later, from anywhere
```

`screen` works identically. For unattended operation a `systemd` unit is
better still — it survives reboots, captures output to the journal, and can be
restarted without a shell. Either way, **do not** run it bare over SSH.

Note that the run is not fully unattended regardless: an interactive planner
blocks for human approval in the UI. Plan to be reachable, or pass
`--no-interactive-plan`.

## Migration checklist

- [ ] Instance supports hardware virtualization; `sbx diagnose` passes.
- [ ] Disk provisioned well past the preflight estimate.
- [ ] `sandbox.sbx.cpus` and `sandbox.sbx.memory` pinned explicitly.
- [ ] `sandbox.sbx.worktree_root` equals the runner's `--worktree-root`.
- [ ] Server listening on loopback; verified with `ss -lntp`.
- [ ] Tunnel working (SSH / Tailscale / IAP / SSM / Bastion); no inbound port.
- [ ] Publish token available via file or secret-manager command.
- [ ] Coding-harness authentication completed on the host.
- [ ] `omni-sbx-agy bootstrap --workspace <persistent path>` plus `/login`.
- [ ] sbx network policy rules re-applied for your toolchain and git forge.
- [ ] Runner started under tmux/screen/systemd.
- [ ] A short pipeline run all the way to publish, before a real campaign.
