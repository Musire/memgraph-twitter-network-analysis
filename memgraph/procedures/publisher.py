from kafka import KafkaProducer
import json
import mgp
import os

KAFKA_IP = os.getenv("KAFKA_IP", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")

# decorate using 'read procedure' method from Memgraph Python API
@mgp.read_proc

# input takes Memgraph 'any' objects
# return is Memgraph 'record' object

def create(created_objects: mgp.Any) -> mgp.Record():
    created_objects_info = {"vertices": [], "edges": []}

    for obj in created_objects:
        # if object 'event type' is equal to 'created_vertex'
        # append information from 'vertex' index of the object to
        # the 'vertices' index of 'created_objects_info'
        if obj["event_type"] == "created_vertex":
            created_objects_info["vertices"].append(
                {
                    "id": obj["vertex"].id,
                    "labels": [label.name for label in obj["vertex"].labels],
                    "username": obj["vertex"].properties["username"],
                    "rank": obj["vertex"].properties["rank"],
                    "cluster": obj["vertex"].properties["cluster"],
                }
            )
            
        
        # otherwise append 'edge' index of object's information
        # to the 'edges' index from 'created_objects_info'
        else:
            created_objects_info["edges"].append(
                {
                    "id": obj["edge"].id,
                    "type": obj["edge"].type.name,
                    "source": obj["edge"].from_vertex.id,
                    "target": obj["edge"].to_vertex.id,
                }
            )

    # instantiate kafka_producer on the ENV ip and port
    # kafka producer will publish a topic 'created objects' to cluster
    kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_IP + ":" + KAFKA_PORT)
    kafka_producer.send(
        "created_objects", json.dumps(created_objects_info).encode("utf8")
    )

    return None