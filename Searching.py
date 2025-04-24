#%%
from neo4j import GraphDatabase
import time
from py2neo import Graph
import matplotlib.pyplot as plt



# %%
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
graph = Graph("bolt://localhost:7687", auth=("neo4j", "password"))

# %%
with driver.session(database='neo4j') as session:

    #---------------------------------------------------------------------------------


    loadQuery = """
    LOAD CSV WITH HEADERS FROM 'file:///unique_relationship_matrix_40.csv' AS line
    MERGE (n:Node {name: line.child_node})
    MERGE (m:Node {name: line.parent_node})
    MERGE (n)<-[r:isParentOf {rType: line.relationship_type}]-(m)
    """
    startTime = time.time()

    # the query to load data
    query = session.run(loadQuery)

    endTime = time.time()

    print("\n\nThe execution time to load the CSV file to Neo4j is :", endTime - startTime, "seconds\n")


# %%

with driver.session(database='neo4j') as session:

    # DFS cypher
    query1 = """
    MATCH (start:Node {name: 'root'})
    WITH start
    MATCH path = (start)-[:A|E|M|O|R|star]->(child)
    RETURN [node IN nodes(path) | node.name] AS dfs_traversal
    ORDER BY length(path)
    LIMIT 1

    """
    # BFS cypher
    query2 = """
    MATCH (start:Node {name: 'root'})
    WITH start
    MATCH path = (start)-[:A|E|M|O|R|star]->(child)
    RETURN [node IN nodes(path) | node.name] AS bfs_traversal
    """
    startTime1 = time.time()
    dfs = graph.run(query1).data()
    t1 = time.time() - startTime1
    
    startTime2 = time.time()
    bfs = graph.run(query2).data()
    t2 = time.time() - startTime2

    print("\n\nThe execution time taken to perform DFS using CypherQuery is :", t1, "seconds\n")
    print("\n\nThe execution time taken to perform BFS using CypherQuery is :", t2, "seconds\n")


    labels = ['DFS', 'BFS']
    execution_times = [t1, t2]

    plt.bar(labels, execution_times, color=['blue', 'green'])
    plt.xlabel('Algorithm')
    plt.ylabel('Execution Time (seconds)')
    plt.title('DFS vs BFS Performance using Cypher')
    plt.show()

with driver.session(database='neo4j') as session:


      # BFS using GDS
      startTime1 = time.time()
      query1 = """
      MATCH (source:Node{node:'root'})
      CALL gds.bfs.stream('graphProjection', {
      sourceNode: source
      })
      YIELD path
      RETURN path"""
      graph.run(query1)
      t1= time.time() - startTime1

      # DFS using GDS
      startTime2 = time.time()
      query2= """
      MATCH (source:Node{node:'root'})
      CALL gds.dfs.stream('graphProjection', {
      sourceNode: source
      })
      YIELD path
      RETURN path"""

      graph.run(query2)
      t2= time.time() - startTime2
      print("\n\nThe execution time taken to perform DFS using GDS is :", t2, "seconds\n")
      print("\n\nThe execution time taken to perform BFS using GDS is :", t1, "seconds\n")
      labels = ['DFS', 'BFS']
      execution_times = [t2, t1]
      plt.bar(labels, execution_times, color=['blue', 'green'])
      plt.xlabel('Algorithm')
      plt.ylabel('Execution Time (seconds)')
      plt.title('DFS vs BFS Performance using Cypher')
      plt.show()

    
      

with driver.session(database='neo4j') as session:


      # BFS using apoc
      startTime1 = time.time()
      query1  = """
    MATCH (start:Node {node: 'root'})
    CALL apoc.path.subgraphNodes(start, {
        relationshipFilter: 'isParentOf',
        labelFilter: 'Node',
        strategy: 'BREADTH_FIRST'
    })
    YIELD node
    RETURN node"""
      graph.run(query1)
      t1= time.time() - startTime1

      # DFS using apoc
      startTime2 = time.time()
      query2 = """
    MATCH (start:Node {node: 'root'})
    CALL apoc.path.subgraphNodes(start, {
        relationshipFilter: 'isParentOf',
        labelFilter: 'Node',
        strategy: 'DEPTH_FIRST'
    })
    YIELD node
    RETURN node"""
      graph.run(query2)
      t2= time.time() - startTime2
      print("\n\nThe execution time taken to perform DFS using apoc is :", t2, "seconds\n")
      print("\n\nThe execution time taken to perform BFS using apoc is :", t1, "seconds\n")
      labels = ['DFS', 'BFS']
      execution_times = [t2, t1]
      plt.bar(labels, execution_times, color=['blue', 'green'])
      plt.xlabel('Algorithm')
      plt.ylabel('Execution Time (seconds)')
      plt.title('DFS vs BFS Performance using Cypher')
      plt.show()

# %%
