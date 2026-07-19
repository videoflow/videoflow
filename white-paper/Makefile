# White paper build.
#
#   make            build the PDF (regenerating any stale figures first)
#   make figures    diagrams + result charts only
#   make experiments  re-run the experiment suite (needs a live NATS + Redis)
#   make clean      drop build intermediates
#
# Editing the paper: edit paper/paper_template.html, then `make`. Do not edit
# out/paper.html — it is generated and overwritten on every build.
#
# Measured numbers are never typed into the template: build_pdf.py substitutes
# them from results/*.json, so re-running the experiments propagates into the
# PDF automatically.

UV       := uv run
PDF      := videoflow-whitepaper.pdf
TEMPLATE := paper/paper_template.html

DOT_SRC   := $(wildcard code/diagrams/*.dot)
DOT_PNG   := $(patsubst code/diagrams/%.dot,figures/%.png,$(DOT_SRC))
DOT_SVG   := $(patsubst code/diagrams/%.dot,figures/%.svg,$(DOT_SRC))
RESULTS   := $(wildcard results/*.json)
CHART_STAMP := out/.charts.stamp

.PHONY: all pdf figures diagrams charts experiments clean help
.DEFAULT_GOAL := all

all: pdf

pdf: $(PDF)

$(PDF): $(TEMPLATE) code/build/build_pdf.py $(DOT_PNG) $(CHART_STAMP) $(RESULTS)
	$(UV) --with weasyprint python code/build/build_pdf.py

figures: diagrams charts

diagrams: $(DOT_PNG) $(DOT_SVG)

# Graphviz sources render to both PNG (embedded in the PDF) and SVG (for reuse).
figures/%.png: code/diagrams/%.dot
	dot -Tpng -Gdpi=180 $< -o $@

figures/%.svg: code/diagrams/%.dot
	dot -Tsvg $< -o $@

charts: $(CHART_STAMP)

# One script emits all five result charts, so a stamp file stands in for them
# rather than repeating the recipe per output.
$(CHART_STAMP): code/build/make_figures.py $(RESULTS)
	@mkdir -p out
	$(UV) --with matplotlib python code/build/make_figures.py
	@touch $@

# Not part of `all`: this takes ~6 minutes and overwrites results/, which every
# number in the paper is derived from. Needs `docker compose up -d` first.
experiments:
	$(UV) python code/experiments/run_experiments.py

clean:
	rm -rf out

help:
	@echo 'make              build the PDF (regenerating any stale figures first)'
	@echo 'make figures      diagrams + result charts only'
	@echo 'make experiments  re-run the experiment suite (needs a live NATS + Redis)'
	@echo 'make clean        drop build intermediates'
	@echo
	@echo 'Edit paper/paper_template.html, then `make`.'
	@echo 'Do not edit out/paper.html — it is generated and overwritten.'
