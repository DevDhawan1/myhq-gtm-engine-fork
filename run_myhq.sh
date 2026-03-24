#!/bin/bash
# myHQ GTM Engine v2 — Quick runner
# Usage: ./run_myhq.sh [mode] [options]

set -euo pipefail
cd "$(dirname "$0")"

# Load env
if [ -f .env ]; then
    set -a; source .env; set +a
fi

MODE="${1:-full}"
shift 2>/dev/null || true

case "$MODE" in
    dry|test)
        python3 agent_v2.py --run full --dry-run "$@"
        ;;
    full)
        python3 agent_v2.py --run full "$@"
        ;;
    signals)
        python3 agent_v2.py --run signals "$@"
        ;;
    enrich)
        python3 agent_v2.py --run enrich "$@"
        ;;
    sdr)
        python3 agent_v2.py --run sdr "$@"
        ;;
    competitors)
        python3 agent_v2.py --run competitors "$@"
        ;;
    content)
        python3 agent_v2.py --run content "$@"
        ;;
    whatsapp)
        python3 agent_v2.py --run whatsapp "$@"
        ;;
    blr)
        python3 agent_v2.py --run full --city BLR "$@"
        ;;
    mum)
        python3 agent_v2.py --run full --city MUM "$@"
        ;;
    del)
        python3 agent_v2.py --run full --city DEL "$@"
        ;;
    setup)
        echo "Setting up Airtable tables..."
        python3 setup_airtable.py
        echo "Done. Now add API keys to .env"
        ;;
    *)
        echo "myHQ GTM Engine v2"
        echo ""
        echo "Usage: ./run_myhq.sh [mode] [options]"
        echo ""
        echo "Modes:"
        echo "  dry          Full pipeline with synthetic data"
        echo "  full         Full pipeline with live APIs"
        echo "  signals      Signal detection only"
        echo "  enrich       Signals + enrichment"
        echo "  sdr          Full pipeline → SDR call list"
        echo "  competitors  Weekly competitor scan"
        echo "  content      LLM content generation"
        echo "  whatsapp     Send WhatsApp messages"
        echo "  blr          Full pipeline, Bengaluru only"
        echo "  mum          Full pipeline, Mumbai only"
        echo "  del          Full pipeline, Delhi only"
        echo "  setup        Create Airtable tables"
        echo ""
        echo "Options: --persona 1|2|3  --tier hot|warm  --verbose"
        ;;
esac
