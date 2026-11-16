-- Where a labelled thing lives on the node, e.g. label 'dataset:camelyon16' at path '/data/camelyon16'.
-- Jobs placed there get the path in HYRAX_DATASET_CAMELYON16.
alter table node_label add column path text;

-- Files a job attempt left in its output directory. The bytes are on the control plane's disk.
create table job_artifact (
    job_id      bigint not null,
    attempt     int not null,
    -- Relative to the output directory, '/' separated, never absolute and never containing '..'.
    path        text not null,
    size_bytes  bigint not null,
    sha256      text not null,
    uploaded_at timestamptz not null default now(),
    primary key (job_id, attempt, path),
    foreign key (job_id, attempt) references job_attempt (job_id, attempt)
);
