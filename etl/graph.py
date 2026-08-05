"""
graph.py — the orchestration graph, defined ONCE.

Both consumers read from here so there is a single source of truth:
  * dags/etl_dag.py       builds the Airflow DAG from build_graph()
  * etl/local_runner.py   runs the same graph locally (for demos + tests)

Ingest specs have no upstream (source -> silver). Model specs depend on the
ingest nodes that produce their input silver tables.
"""
from __future__ import annotations

from dataclasses import dataclass

from etl.registry import IngestSpec, ModelSpec, load_specs


@dataclass(frozen=True)
class Node:
    name: str
    upstream: tuple[str, ...]


def build_graph() -> list[Node]:
    specs = load_specs()
    # map silver table name -> the ingest spec that produces it
    producer = {
        s.target["table"]: s.name
        for s in specs.values()
        if isinstance(s, IngestSpec)
    }
    nodes: list[Node] = []
    for spec in specs.values():
        if isinstance(spec, ModelSpec):
            upstream = tuple(producer[i] for i in spec.inputs if i in producer)
        else:
            upstream = ()
        nodes.append(Node(name=spec.name, upstream=upstream))
    return nodes


def topological_order(nodes: list[Node]) -> list[str]:
    order: list[str] = []
    remaining = {n.name: set(n.upstream) for n in nodes}
    while remaining:
        ready = [n for n, deps in remaining.items() if deps <= set(order)]
        if not ready:
            raise ValueError("Cycle detected in pipeline graph")
        for n in sorted(ready):
            order.append(n)
            remaining.pop(n)
    return order
