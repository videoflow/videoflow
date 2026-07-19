# Videoflow white paper

`videoflow-whitepaper.pdf` — a self-contained description of the system (programming model,
architecture, messaging topology, wire format, deployment) plus an experimental evaluation run
on toy graphs. Everything in it is regenerable from this directory.

## Editing the paper

The paper is written in HTML and rendered to PDF with [WeasyPrint](https://weasyprint.org/).

```bash
$EDITOR paper/paper_template.html     # the source — edit this
make                                  # rebuild videoflow-whitepaper.pdf
```

Two things to know before your first edit:

- **`out/paper.html` is generated, not source.** It is overwritten on every build and gitignored.
  Edit `paper/paper_template.html` instead.
- **`$` is special in the template.** `$E2_EFF8` and friends are placeholders that `build_pdf.py`
  fills in from `results/*.json`, so a literal dollar sign must be written `$$`. This is what
  keeps every measured number in the text, tables, and abstract bound to the actual experiment
  output — re-running the experiments propagates into the PDF with no manual edits. The full
  placeholder list is the `tokens` dict in [code/build/build_pdf.py](code/build/build_pdf.py).

Styling (fonts, margins, page headers, table and figure rules) is the `<style>` block at the top
of the template. Page geometry uses CSS Paged Media (`@page`), which WeasyPrint implements.

## Layout

| Path | What it is |
|---|---|
| `videoflow-whitepaper.pdf` | The built paper (A4, 15 pages) |
| `Makefile` | The build — see `make help` |
| `paper/paper_template.html` | **Paper source.** Prose, tables, and styling |
| `code/experiments/` | Experiment harness: instrumented toy nodes + the driver |
| `code/diagrams/` | Graphviz sources for the architecture and topology diagrams |
| `code/build/` | `make_figures.py` (result charts), `build_pdf.py` (HTML → PDF) |
| `figures/` | Rendered images — diagrams (`dot`) and charts (matplotlib) |
| `results/` | Experiment output: aggregated `*.json` plus per-message `raw/*.jsonl` |
| `out/` | Build intermediates (gitignored) |

Nothing outside this directory is referenced, and nothing here is imported by the framework —
the whole folder can be moved or removed without affecting `videoflow` itself.

## Build targets

```bash
make              # build the PDF, regenerating stale figures first
make figures      # diagrams + charts only
make experiments  # re-run the experiment suite (~6 min; see below)
make clean        # drop build intermediates
```

The build is incremental: editing the template rebuilds only the PDF, editing a `.dot` file
re-renders just that diagram, and changing `results/*.json` re-renders the charts and the PDF.

## Re-running the experiments

`make experiments` overwrites `results/`, which every number in the paper derives from. It needs a
live broker, started from the repository root:

```bash
docker compose up -d      # NATS JetStream :4222 + Redis :6379
cd white-paper && make experiments && make
```

Individual families can be run alone — `uv run python code/experiments/run_experiments.py e2 e4`
— where `e1` is the wire-format microbenchmark, `e2` stage scale-out, `e3` per-hop overhead, `e4`
overload semantics, `e5` partitioned join scaling, and `e6` the time-aligned quorum join.

Table 6 of the paper records the machine the shipped results came from. Re-running elsewhere will
shift absolute latencies but not the scaling or semantics findings.

## Dependencies

`uv` fetches the Python ones per-invocation (`--with matplotlib`, `--with weasyprint`), so the
only thing to install yourself is Graphviz for the diagrams:

```bash
sudo apt-get install graphviz      # provides `dot`
```
