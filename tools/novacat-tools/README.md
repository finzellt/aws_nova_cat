# novacat-tools

Operational interfaces for the NovaCat catalog.

## Layout

```
novacat-tools/
├── scripts/
│   └── nova-ingest.sh        # Interface 1: trigger initialize_nova
├── tools/
│   ├── novacat_query.py      # Interface 2: DynamoDB query module
│   └── novacat_logs.py       # Interface 3: CloudWatch Logs Insights module
└── notebooks/
    └── novacat.ipynb         # Notebook: DynamoDB + logs, visualization
```

---

## Interface 1 — Trigger a nova ingestion

```zsh
chmod +x scripts/nova-ingest.sh
./scripts/nova-ingest.sh "V1324 Sco"
```

Starts an `initialize_nova` Step Functions execution. Prints the
`execution_arn` and `correlation_id`. The `correlation_id` traces the full
chain (initialize → ingest → refresh_references + discover_spectra →
acquire_and_validate) in CloudWatch.

---

## Interface 2 — Query DynamoDB

**From the notebook:** open `notebooks/novacat.ipynb`, run *Imports & Setup*,
then use any section.

**From the CLI:**
```zsh
cd tools

# Table-wide stats
python novacat_query.py --dashboard

# Per-nova views
python novacat_query.py --nova "V1324 Sco" --view spectra
python novacat_query.py --nova "V1324 Sco" --view summary
python novacat_query.py --nova "V1324 Sco" --view jobs
python novacat_query.py --nova "V1324 Sco" --view refs
```

---

## Interface 3 — CloudWatch Logs

**From the notebook:** use Section 4 in `notebooks/novacat.ipynb`.

**From the CLI:**
```zsh
cd tools

# Trace a full execution chain by correlation_id
python novacat_logs.py --trace <correlation_id>

# See what's been running in the last 10 minutes
python novacat_logs.py --recent 10

# Filter to a specific workflow
python novacat_logs.py --recent 30 --workflow refresh_references

# Error scan (last hour)
python novacat_logs.py --errors 1

# Job run history (last 24h)
python novacat_logs.py --jobs 24
```

---

## Requirements

```
boto3
pandas
matplotlib
```

AWS credentials must be configured (`~/.aws/credentials` or environment
variables) with read access to DynamoDB table `NovaCat` and CloudWatch Logs
log groups `/aws/lambda/nova-cat-*`.
