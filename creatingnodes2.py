import pandas as pd
import random
import warnings

# Suppress future warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Constants
NUM_RECORDS = 1000  # Total records for the relationship graph
RELATIONSHIP_TYPES = ["M", "O", "star", "A", 'R', 'E']  # Different types of relationships

def create_relationship_graph(max_links=10):
    relationship_df = pd.DataFrame(columns=["parent_node", "child_node", "relation_type"])  # DataFrame to store relationships
    node_set = set({"root"}) # Set to keep track of unique nodes
    link_count = 0  # Counter for the number of links
    node_count = 1  # Counter for the number of nodes
    current_parent = "root"  # Starting node
    retain_child = False  # Flag to retain the current child node for the next iteration

    while link_count <= max_links:
      
        choice = random.random()  # Random decision to create a new node or use an existing one

        # Decide on the child node based on the retain_child flag
        if not retain_child:
            relation = RELATIONSHIP_TYPES[random.randint(0, len(RELATIONSHIP_TYPES) - 1)]
            current_child = f"Node{node_count}" if choice >= 0.8 else f"Node{random.randint(1, node_count)}"
            node_set.add(current_child)
            node_count = len(node_set)
        else:
            current_parent = f"Node{random.randint(1, node_count-1)}"
        

        # Rule checks for the relationship graph
        if current_child == current_parent or \
           check_graph_rules(relationship_df, current_parent, current_child, relation, node_count):
            continue
        if relationship_df[relationship_df["child_node"] == current_child].shape[0] > 0 and relation not in ['R', 'E']:
            current_parent = current_child
            continue
        if relation == 'R':
            if node_count < 10: #Condition to ensure there are enough nodes in the graph before we start making R or E relationships.
                continue
            #If the child node already has a parent with relationship other than R, do not create an E relationship since a child can have multiple parents of only R and E relationships.
            if relationship_df[(relationship_df["child_node"] == current_child) & (relationship_df["relation_type"] != 'E')].shape[0] > 0:
                continue

            #If the child has a parent with relationship as E, then we can create the R relationship, else we create the R relationship and ensure an E relationship is formed next with the same child from another parent.
            if relationship_df[(relationship_df["child_node"] == current_child) & (relationship_df["relation_type"] == 'E')].shape[0] > 0:
                relationship_df = relationship_df.append({"parent_node": current_parent, "child_node": current_child, "relation_type": relation}, ignore_index=True)
                current_parent = current_child
                retain_child = False #Indicates whether the current child has to be maintained for the next iteration.
            else:
                relationship_df = relationship_df.append({"parent_node": current_parent, "child_node": current_child, "relation_type": relation}, ignore_index=True)
                relation = 'E'
                retain_child = True #In this case, we created an R relationship with the child node, but we also need to create an E relationship. So we maintain the same child while choosing a parent at random.
            link_count += 1
            continue
        if relation == 'E':
            if node_count < 10: #Condition to ensure there are enough nodes in the graph before we start making R or E relationships.
                continue
            #If the child node already has a parent with relationship other than R, do not create an E relationship since a child can have multiple parents of only R and E relationships.
            if relationship_df[(relationship_df["child_node"] == current_child) & (relationship_df["relation_type"] != 'R')].shape[0] > 0:
                continue

            #If the child has a parent with relationship as E, then we can create the R relationship, else we create the R relationship and ensure an E relationship is formed next with the same child from another parent.
            if relationship_df[(relationship_df["child_node"] == current_child) & (relationship_df["relation_type"] == 'R')].shape[0] > 0:
                relationship_df = relationship_df.append({"parent_node": current_parent, "child_node": current_child, "relation_type": relation}, ignore_index=True)
                current_parent = current_child
                retain_child = False #Indicates whether the current child has to be maintained for the next iteration.
            else:
                relationship_df = relationship_df.append({"parent_node": current_parent, "child_node": current_child, "relation_type": relation}, ignore_index=True)
                relation = 'R'
                retain_child = True #In this case, we created an R relationship with the child node, but we also need to create an E relationship. So we maintain the same child while choosing a parent at random.
            link_count += 1
            continue
        # Add the valid relationship to the DataFrame
        relationship_df = relationship_df.append({"parent_node": current_parent, "child_node": current_child, "relation_type": relation}, ignore_index=True)
        link_count += 1
        current_parent = current_child

    return relationship_df

def check_graph_rules(df, parent, child, relation, total_nodes):
    # Implementing all specified rules
    return any([
        (df[((df["parent_node"] == parent) | (df["child_node"] == parent)) & (df["relation_type"] == 'M')].shape[0] > 0) and relation not in ['M', 'O'],
        (df[(df["parent_node"] == parent) & ((df["relation_type"] == 'R') | (df["relation_type"] == 'E'))].shape[0] > 0) and relation == 'M',
        (df[((df["parent_node"] == parent) | (df["child_node"] == parent)) & (df["relation_type"] == 'O')].shape[0] > 0) and relation not in ['M', 'O', 'R', 'E'],
        (df[((df["parent_node"] == parent) | (df["child_node"] == parent)) & (df["relation_type"] == 'star')].shape[0] > 0) and relation not in ['star', 'R', 'E'],
        (df[((df["parent_node"] == parent) | (df["child_node"] == parent)) & (df["relation_type"] == 'A')].shape[0] > 0) and relation not in ['A', 'R', 'E']
    ])

def determine_child_retention(df, child, relation):
    return (relation == 'E' and df[(df["child_node"] == child) & (df["relation_type"] == 'R')].empty()) or (relation == 'R' and df[(df["child_node"] == child) & (df["relation_type"] == 'E')].empty())

# Main execution
if __name__ == "__main__":
    relationship_graph = create_relationship_graph(NUM_RECORDS)
    print(relationship_graph)

    # Saving the graph to a CSV file
    csv_filename = f"relationship_graph_{NUM_RECORDS}.csv"
    relationship_graph.to_csv(csv_filename, index=False)

