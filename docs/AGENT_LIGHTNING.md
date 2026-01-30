# Agent Lightning (Pilot)

This project includes an optional, Docker-based pilot setup for Microsoft Agent Lightning.
It runs in a Linux container (Ubuntu 22.04) so it does not affect your local Python 3.9
virtualenv.

## Why this pilot
- Give us a dedicated environment to run Agent Lightning experiments.
- Keep tooling isolated from the main ingestion/runtime environment.
- Make it easy to define evals and repeat them when we refactor pipelines.

## Quick start
From the project root:

```bash
docker compose -f docker-compose.agent-lightning.yml up --build -d
docker compose -f docker-compose.agent-lightning.yml run --rm agent-lightning bash
```

Inside the container:

```bash
python3 -c "import agentlightning; print(agentlightning.__version__)"
```

## Where to put pilot artifacts
Create a folder like `devtools/agent_lightning/` for:
- evaluation scripts
- prompts
- datasets
- experiment notes

This keeps the pilot separate from production ingestion code.

## Notes
- The container is CPU-only by default.
- If GPU support is needed later, we can add a CUDA base image and `--gpus` support.
- DB access from Docker: if your `.env` uses `localhost`, switch to `host.docker.internal`
  (Mac/Windows) or the Docker bridge IP so the container can reach Postgres.

## Pilot evals included
Two data-quality evals are included:
1) Layer6 risk_vulnerability checks (coverage + 0-1 score ranges)
2) Timeseries regression checks (coverage + score ranges)

Run them from the repo root:

```bash
python devtools/agent_lightning/run_evals.py
```

Optional flags:

```bash
python devtools/agent_lightning/run_evals.py --evals layer6
python devtools/agent_lightning/run_evals.py --evals timeseries --min-coverage-ratio 0.7
python devtools/agent_lightning/run_evals.py --write-baseline
python devtools/agent_lightning/run_evals.py --coverage-tolerance 0.05 --count-tolerance 0
```
