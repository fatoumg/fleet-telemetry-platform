# Learning path

Start here. This is the curriculum; the
[design spec](../superpowers/specs/2026-08-07-telemetry-platform-design.md) is the architecture
behind it and can be read later.

You are building a data pipeline **and** the system that feeds it. Five phases, following the
stages every pipeline has.

| Phase | Question it answers | Guide |
| --- | --- | --- |
| 1 | What data exists, what shape is it, how often does it arrive? | `01-source-system.md` |
| 2 | How do I get it out, without losing or duplicating any? | `02-ingestion.md` |
| 3 | How do I turn raw records into something trustworthy? | `03-transformation.md` |
| 4 | How do I make it run reliably without me? | `04-orchestration.md` |
| 5 | How does anyone actually use it? | `05-serving.md` |

Guides appear as each phase is built. An empty slot means that phase has not started.

## How to work through this

**Do not skip to the tools.** Each phase deliberately starts with the obvious hand-rolled version,
lets you hit its limits, and only then introduces the real tool. Building a batch poller and
watching it fail to notice a deleted row is what makes change data capture make sense. Skip that
and you will end up with a working pipeline you cannot explain, which is the specific outcome this
project is designed to avoid.

**Type the code, don't paste it.** Where a guide gives you code, the comments explaining *why* are
the content. The code is just what the reasoning produced.

**When something breaks, write down what happened before you fix it.** The repository already does
this: [docker-compose.yml](../../docker/docker-compose.yml) explains at length why the database
port is 55432 rather than 5432, because a Windows port collision presented as an authentication
failure and cost a day of debugging. Those notes are worth more than the code around them, because
they are the part no tutorial can give you.

**Answer the explain-back questions from memory.** Each phase ends with two or three. If you cannot
answer them without looking, the phase is not finished — even if everything runs.

## The one thing this project is about

> **Events arrive late, out of order, and in bursts. The warehouse must still be correct.**

A vehicle drives out of signal, buffers an hour of GPS readings, and uploads them all at once when
it reconnects. Its clock is four minutes fast. It retries a failed upload, so some readings arrive
twice. Meanwhile the aggregate for 09:00–10:00 was calculated an hour ago and is now wrong.

Handling that properly is Phase 3, and it is the hardest and most valuable part. Everything before
it exists to get you to the point where you can.

## Before you start

You need Docker and Python 3.13. There are **no accounts to create and no API keys to obtain** —
this project deliberately depends on no external service. (The previous version of this project
depended on three, and died when the first one refused access; see
[docs/archive/](../archive/README.md).)

```bash
pip install -e ".[dev]"
python -m fleet_telemetry.config     # shows what resolved; nothing should be missing
pytest
docker compose -f docker/docker-compose.yml up -d
```

If all four succeed, you are ready for Phase 1.

## Vocabulary you will meet

Defined properly in the phase that introduces them; listed here so none of them is a surprise.

| Term | Rough meaning | Phase |
| --- | --- | --- |
| OLTP / OLAP | The database an app writes to / the one analysts query | 1 |
| Event time vs processing time | When something happened / when you found out | 1 |
| Idempotent | Running it twice gives the same result as once | 2 |
| Watermark | "I have processed everything up to here" | 2 |
| CDC | Reading a database's change log instead of polling it | 2 |
| At-least-once | Delivery may duplicate, so you must deduplicate | 2 |
| Medallion (bronze/silver/gold) | Raw → cleaned → modelled | 3 |
| Grain | What exactly one row of a table represents | 3 |
| Fan-out | An accidental join that silently multiplies rows | 3 |
| SCD Type 2 | Keeping history when a dimension changes | 3 |
| Restatement | A published number changing because late data arrived | 3 |
| Backfill | Re-running the pipeline over a past period | 4 |
| DAG | A dependency graph of tasks | 4 |
| Freshness | How stale the data a consumer sees is allowed to be | 5 |
