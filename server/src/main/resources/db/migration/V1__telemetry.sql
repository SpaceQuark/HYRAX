create table node (
    name          text primary key,
    agent_version text,
    first_seen    timestamptz not null default now(),
    last_seen     timestamptz not null
);

create table gpu (
    uuid            text primary key,
    node            text not null references node (name),
    index           int not null,
    name            text not null,
    memory_total_mb bigint not null,
    reserved        boolean not null default false,
    owner           text,
    reason          text
);

-- One row per GPU per minute. Live 3-second samples are never stored.
create table gpu_sample_1m (
    gpu_uuid              text not null references gpu (uuid),
    minute                timestamptz not null,
    samples               int not null,
    utilization_avg       real,
    utilization_max       int,
    memory_used_avg_mb    real not null,
    memory_used_max_mb    bigint not null,
    -- Memory held by compute processes only, excluding desktop sessions.
    compute_memory_max_mb bigint not null,
    primary key (gpu_uuid, minute)
);

create index gpu_sample_1m_minute on gpu_sample_1m (minute);
