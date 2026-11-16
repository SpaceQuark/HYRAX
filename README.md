# HYRAX

![HYRAX](hyrax.jpeg)

HYRAX (Heterogeneous Yield & Resource Allocation eXecutor) is a lightweight distributed GPU job orchestrator designed for small
research labs with GPUs distributed across multiple workstations.

It suits labs where the machines are shared workstations, not a managed
cluster: people still SSH in and start jobs by hand, and HYRAX works around
them.

## What it does

- **Roaming-first scheduling.** A job runs on any machine with a suitable free
  GPU, unless you pin it to one.
- **VRAM-aware placement.** Jobs say how much GPU memory they need and are
  placed on a GPU that has it, keeping the largest cards for the jobs that
  need them.
- **Detection of externally launched workloads.** GPUs in use by processes
  HYRAX did not start are left alone.
- **Reservations.** GPUs can be set aside and are never scheduled.
- **Remote job execution.** Your project's source travels with the job; no
  shared filesystem is needed.
- **Job lifecycle management.** Queueing, logs, cancellation, output
  retrieval, and recovery after agent or control-plane restarts.

## How it is built

- A **control plane** written in Java (Spring Boot, Postgres) holds the
  queue, makes placement decisions and serves the API and dashboard.
- A lightweight **Python agent** on each machine reports GPU telemetry and
  runs jobs as ordinary user-level processes. It needs no root, no container
  runtime and no inbound ports.

## Example

```console
$ hyrax run --vram 20G -- python train.py
Submitted job 12 (any node with a suitable free GPU).

$ hyrax ps
   ID  USER       STATE      NODE           GPU    AGE  COMMAND
   12  alice      RUNNING    node-3         0      40s  python train.py

$ hyrax logs -f 12
epoch 1/5  loss 0.0965
```

## Limitations

HYRAX does not provide GPU virtualization or process isolation. It decides
where a job runs; it does not limit what the job can use once it is running,
and jobs run as the account the agent runs under.
