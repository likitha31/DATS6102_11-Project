# DAG Generator and Traversal Performance Benchmark

This repository contains code and instructions for generating large Directed Acyclic Graphs (DAGs), importing them into Neo4j, and evaluating the performance of various traversal algorithms using Cypher, APOC, and the Neo4j Graph Data Science (GDS) library.

## 📌 Project Overview

The goal of this project is to:

- Generate CSV files containing parent-child relationships that form a valid DAG.
- Import the DAG into Neo4j using Python and Cypher.
- Evaluate and compare the performance of BFS and DFS traversal using:
  - Native Cypher
  - APOC library
  - GDS library
- Visualize performance results using Matplotlib.

## 📁 CSV Format

Each CSV file contains three columns:


### Relationship Types

- `M`, `O`, `star`, `A`, `R`, `E`

## 📌 Rules for Generating DAGs

1. No duplicate parent-child pairs.
2. A node cannot be its own child.
3. A parent with `M` or `O` relationships may only have children with `M`/`O` (plus optional `R`/`E`).
4. A `star` parent can only have `star` children (plus optional `R`/`E`).
5. An `A` parent can only have `A` children (plus optional `R`/`E`).
6. A `M` parent cannot have `R` or `E` children.
7. The graph must be connected; no orphan nodes.
8. Cycles are allowed *only* if formed by `R` or `E` relationships.
9. A node may have multiple parents only if at least one parent is not `R` or `E`.

## 🚀 Getting Started

### Requirements

- Python 3.x
- Neo4j Desktop or Neo4j Aura
- Matplotlib
- `neo4j` Python driver
- APOC and GDS plugins installed in Neo4j

### Setup Neo4j

1. Create a new Neo4j database.
2. Open the database details → Plugins → Install `APOC` and `Graph Data Science`.
3. Restart the database after plugin installation.

### Clone this Repository

```bash
git clone https://github.com/your-username/dag-generator.git
cd dag-generator

pip install -r requirements.txt

python generate_dag.py


LOAD CSV WITH HEADERS FROM 'file:///CSVFiletoImport_DAGexample.csv' AS line
MERGE (n:Child {name: line.child})
MERGE (m:Parent {name: line.parent})
MERGE (n)<-[r:isParentOf {rType: line.relationshiptype}]-(m)


