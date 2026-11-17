"""Supply network and product graph (the v2-v4 PRD, v3).

Two base tables (graph.nodes, graph.edges) built from the silver dimensions, and four
result tables that answer the README's traversal questions: upstream dependencies and
bottlenecks, cross-tenant dependencies, and multi-hop substitution paths. Traversals run
with GraphFrames inside the pipeline container; no graph database is added.
"""
