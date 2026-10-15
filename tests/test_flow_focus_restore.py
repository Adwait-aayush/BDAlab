import re
from pathlib import Path


def test_back_to_flow_restores_focus_to_last_selected_flow_node():
    app_js = Path("/home/runner/work/BDAlab/BDAlab/web/app.js").read_text(encoding="utf-8")

    assert "let lastFlowNode = null;" in app_js
    assert 'if (action.classList.contains("flow-node")) lastFlowNode = action;' in app_js

    back_focus_block = re.search(
        r"requestAnimationFrame\(\(\) => \{[\s\S]*?if \(lastFlowNode\) \{[\s\S]*?lastFlowNode\.focus\(\);[\s\S]*?\}\s*else if \(flowMap\) \{[\s\S]*?flowMap\.focus\(\);[\s\S]*?\}[\s\S]*?\}\);",
        app_js,
    )

    assert back_focus_block is not None
