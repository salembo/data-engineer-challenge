"""Gold: the three business domains of the README (inventory, POS sales, pipeline health).

Every run recomputes gold in full from silver (reconciliation also reads bronze) and
synchronises each table with its result, so identical silver gives identical gold and a
re-run with no changes writes nothing (the v2-v4 PRD, writes and re-runs).
"""
