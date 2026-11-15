-- Who claimed a hand-launched process. Removed when the process exits.
create table process_owner (
    node          text not null,
    pid           int not null,
    slack_user_id text not null,
    claimed_at    timestamptz not null default now(),
    primary key (node, pid)
);

-- One row per idle episode: a process holding GPU memory while the GPU sits idle.
create table alert (
    id             bigserial primary key,
    node           text not null,
    gpu_uuid       text not null,
    gpu_index      int not null,
    gpu_name       text not null,
    pid            int not null,
    os_user        text,
    command        text,
    memory_mb      bigint not null,
    idle_since     timestamptz not null,
    opened_at      timestamptz not null,
    closed_at      timestamptz,
    owner_slack_id text,
    slack_channel  text,
    slack_ts       text,
    -- Set by Snooze: ping again at this time if the episode is still open.
    remind_at      timestamptz
);

create unique index alert_one_open_per_process on alert (node, gpu_uuid, pid) where closed_at is null;
