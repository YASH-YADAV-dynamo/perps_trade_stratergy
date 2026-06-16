.PHONY: install run dry-run clean env list

# Default strategy (options: wick_catcher_v2, stat_arb_hype_btc, aggressive_mm)
S ?= wick_catcher_v2

install:
	pip install -r requirements.txt

env:
	@test -f .env || (echo "ERROR: .env file not found. Create it first." && exit 1)
	@test -f .env && echo ".env exists"

# Run a strategy: make run S=wick_catcher_v2
run: env
	python3 strategies/$(S)/main.py

# Dry run: make dry-run S=wick_catcher_v2
dry-run: env
	ENABLE_TRADING=false python3 strategies/$(S)/main.py

# List available strategies
list:
	@echo "Available strategies:"
	@ls -d strategies/*/  2>/dev/null | sed 's|strategies/||;s|/||' | while read s; do echo "  - $$s"; done

clean:
	find . -name '*.pyc' -delete 2>/dev/null || true
	find . -name '__pycache__' -type d -exec rm -rf {} + 2>/dev/null || true
