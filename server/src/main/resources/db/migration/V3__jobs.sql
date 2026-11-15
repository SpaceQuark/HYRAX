-- Jobs: who may submit, what to run, where it ran, and which GPUs it holds.

create table app_user (
    id            bigserial primary key,
    name          text not null unique,
    -- SHA-256 of the personal API token. The token itself is shown once and never stored.
    token_hash    text unique,
    slack_user_id text unique,
    is_admin      boolean not null default false,
    created_at    timestamptz not null default now(),
    disabled_at   timestamptz
);

-- Per-node agent credential, replacing the shared hyrax.agent-token. Null until the node is enrolled.
alter table node add column agent_token_hash text unique;

-- What a node has locally, e.g. 'dataset:imagenet' or 'conda:torch2'. No shared storage, so jobs match on these.
create table node_label (
    node  text not null references node (name),
    label text not null,
    primary key (node, label)
);

-- A source tree uploaded at submit time. The archive lives on the control plane's disk, named by its hash.
create table source_snapshot (
    id          bigserial primary key,
    sha256      text not null unique,
    size_bytes  bigint not null,
    git_commit  text,
    -- True when the archive includes changes that were not committed.
    git_dirty   boolean not null default false,
    uploaded_by bigint not null references app_user (id),
    created_at  timestamptz not null default now()
);

create table job (
    id                  bigserial primary key,
    user_id             bigint not null references app_user (id),
    name                text,
    -- argv, run without a shell unless the user asks for one.
    command             text[] not null check (cardinality(command) > 0),
    -- Extra environment variables. Not for secrets: these are stored and shown in the clear.
    env                 jsonb not null default '{}',
    snapshot_id         bigint references source_snapshot (id),
    -- Directory inside the snapshot to run from. Without a snapshot the job runs in its own empty directory.
    workdir             text not null default '.',
    -- How the Python environment is made: 'uv' syncs the snapshot's lockfile, 'conda' activates conda_env.
    environment         text not null default 'none' check (environment in ('none', 'uv', 'conda')),
    conda_env           text,

    -- Resource request. All GPUs of one attempt are on one node.
    gpus                int not null default 1 check (gpus > 0),
    -- Each GPU must have at least this much total VRAM, and this much free when the job is placed.
    vram_mb             bigint check (vram_mb > 0),
    -- Case-insensitive substring of the GPU name, e.g. '4090'.
    gpu_model           text,
    -- Set by --node or --local; null lets the job roam.
    node                text references node (name),
    -- Labels a node must have, and labels that only break ties between eligible nodes.
    required_labels     text[] not null default '{}',
    preferred_labels    text[] not null default '{}',

    state               text not null default 'QUEUED'
        check (state in ('QUEUED', 'ASSIGNED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'LOST')),
    -- 1 means never retried automatically.
    max_attempts        int not null default 1 check (max_attempts > 0),
    cancel_requested_at timestamptz,
    submitted_at        timestamptz not null default now(),
    finished_at         timestamptz,

    -- Why the job is still queued: the scheduler's verdict on every GPU it looked at, e.g.
    -- [{"gpu": "GPU-…", "node": "node-1", "rejected": "external process holds 9120 MiB"}].
    last_decision       jsonb,
    last_considered_at  timestamptz,

    check ((environment = 'conda') = (conda_env is not null))
);

-- The scheduler walks queued jobs in submission order.
create index job_queued on job (submitted_at, id) where state = 'QUEUED';
create index job_by_user on job (user_id, submitted_at desc);

-- One try at running a job on one node. The attempt number is the fencing token: it only grows,
-- and an agent ignores any command for a job that carries a lower number than one it has seen.
create table job_attempt (
    job_id           bigint not null references job (id),
    attempt          int not null check (attempt > 0),
    node             text not null references node (name),
    state            text not null default 'ASSIGNED'
        check (state in ('ASSIGNED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'LOST', 'EXPIRED', 'REJECTED', 'ABANDONED')),
    -- An ASSIGNED attempt the agent has not acknowledged by this time becomes EXPIRED and the job requeues.
    lease_expires_at timestamptz not null,
    assigned_at      timestamptz not null default now(),
    started_at       timestamptz,
    finished_at      timestamptz,
    -- Process-group leader on the node. Lets telemetry tell a HYRAX job from a process launched by hand.
    pid              int,
    exit_code        int,
    -- Why the attempt ended, when the exit code does not say: lease expiry, GPU taken by hand, node reboot.
    reason           text,
    primary key (job_id, attempt)
);

-- LOST counts as live: the process may still be running on a node we cannot reach. A retry of a lost
-- job first marks the old attempt ABANDONED, and the higher attempt number fences it if the node returns.
create unique index job_attempt_one_live_per_job on job_attempt (job_id)
    where state in ('ASSIGNED', 'RUNNING', 'LOST');
create index job_attempt_live_by_node on job_attempt (node)
    where state in ('ASSIGNED', 'RUNNING', 'LOST');

-- The GPUs an attempt holds. Inserted in the same transaction that creates the attempt, and released
-- when the attempt reaches a final state.
create table gpu_allocation (
    job_id       bigint not null,
    attempt      int not null,
    gpu_uuid     text not null references gpu (uuid),
    allocated_at timestamptz not null default now(),
    released_at  timestamptz,
    primary key (job_id, attempt, gpu_uuid),
    foreign key (job_id, attempt) references job_attempt (job_id, attempt)
);

-- The guarantee that two jobs never get the same GPU, whatever the scheduler does.
create unique index gpu_allocation_one_live_per_gpu on gpu_allocation (gpu_uuid) where released_at is null;

-- Job output, uploaded by the agent with byte offsets so a repeated chunk is harmless.
create table job_log_chunk (
    job_id      bigint not null,
    attempt     int not null,
    byte_offset bigint not null check (byte_offset >= 0),
    data        bytea not null,
    primary key (job_id, attempt, byte_offset),
    foreign key (job_id, attempt) references job_attempt (job_id, attempt)
);
