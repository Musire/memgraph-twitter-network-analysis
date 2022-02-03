import gc
import itertools
import numpy as np
from typing import Dict, List, Tuple
import mgp

TOP_K_PREDICTIONS = 8

# calculate an adjacency matrix taking in a dictionary object 
# and return dictionary object 
def calculate_adjacency_matrix(embeddings: Dict[int, List[float]], threshold=0.0) -> Dict[Tuple[int, int], float]:
    
    # if vertex 1 or vertex 2 are not found in embeddings 
    # then return -1
    # otherwise return the dot product of the vertices
    def get_edge_weight(i, j) -> float:
        if embeddings[i] is None or embeddings[j] is None:
            return -1
        return np.dot(embeddings[i], embeddings[j])

    # 'nodes' created by converting dictionary keys into array
    # sort 'nodes' array
    nodes = list(embeddings.keys())
    nodes = sorted(nodes)

    # variable annotation explaining complete matrix specifications
    adj_mtx_r: Dict[Tuple[int, int], float] = {}
    
    cnt = 0

    # for every pair in all possible pairings available from node pool
    for pair in itertools.combinations(nodes, 2):

        # if the modulus of 'cnt' by '1000000' equals '0'  
        if cnt % 1000000 == 0:
            # redefine using a dictionary comprehension that returns a sorted
            # a lamba function as the sorting key on index 1 of the items
            adj_mtx_r = {k: v for k, v in sorted(adj_mtx_r.items(), key=lambda item: -1 * item[1])}
            adj_mtx_r = {k: adj_mtx_r[k] for k in list(adj_mtx_r)[:TOP_K_PREDICTIONS]}
            gc.collect()

        # calculate edge weight from 2 vertices in the pair
        weight = get_edge_weight(pair[0], pair[1])
        # if weight is less than threshold (0.0) then jump to the next iteration
        # otherwise increase 'cnt' by 1 and overwright the 2 vertices 
        # with the calculated edge weight
        if weight <= threshold:
            continue
        cnt += 1
        adj_mtx_r[(pair[0], pair[1])] = get_edge_weight(pair[0], pair[1])

    return adj_mtx_r

# decorate using 'read procedure' from Memgraph Python API
@mgp.read_proc
# input takes Memgraph procedure context object 
# and returns Memgraph record object 
def predict(ctx: mgp.ProcCtx) -> mgp.Record(edges=mgp.List[mgp.List[mgp.Vertex]]):
    
    # variable annotation for 'embeddings' explained when populated
    embeddings: Dict[int, List[float]] = {}

    # for each vertex in the vertices of the graph of the context 'ctx'
    for vertex in ctx.graph.vertices:
        # If 'get' method is found then 'embedding' is value if not 'None'
        # If 'embedding' is equal to 'None' skip to the next iteration
        # otherwise overwrite the embeddings dictionary on the index
        # of the vertex's id and the value is the embedding
        embedding = vertex.properties.get("embedding", None)
        if embedding is None:
            continue
        embeddings[int(vertex.id)] = embedding

    adj_matrix = calculate_adjacency_matrix(embeddings)
    # dictionary comprehension that returns a sorted key and values pairs
    # a lamba function as the sorting key on index 1 of the items 
    sorted_predicted_edges = {k: v for k, v in sorted(adj_matrix.items(), key=lambda item: -1 * item[1])}

    # instatiate with explained annotation of expected completed array
    edges: List[mgp.List[mgp.Vertex]] = []

    # for every edge in the sorted_predicted_edges append
    # the from and to vertices to the 'edges' array
    # and return a Memgraph record object who's edges equal 'edges' array
    for edge, similarity in sorted_predicted_edges.items():
        vertex_from = ctx.graph.get_vertex_by_id(edge[0])
        vertex_to = ctx.graph.get_vertex_by_id(edge[1])

        edges.append([vertex_from, vertex_to])

    return mgp.Record(edges=edges)