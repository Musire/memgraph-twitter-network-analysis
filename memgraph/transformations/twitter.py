import mgp
import json

# decorate with 'transformation' from Memgraph Python API
@mgp.transformation

# input takes Memgraph 'Messages' object
# return Memgraph 'record' object 
def tweet(messages: mgp.Messages
          ) -> mgp.Record(query=str, parameters=mgp.Nullable[mgp.Map]):
    
    result_queries = []

    # for index in range of total number of messages in input
    for i in range(messages.total_messages()):

        # instantiate message from 'messages'
        message = messages.message_at(i)

        # instantiate dictionary from the 'message' payload and decode in 'utf8'
        tweet_dict = json.loads(message.payload().decode('utf8'))

        # if payload has key 'target username' then
        # append Memgraph 'record' with merged query and parameters
        # source and target userames
        # otherwise append Memgraph 'record' query and parameters  
        # from 'source username'
        if tweet_dict["target_username"]:
            result_queries.append(
                mgp.Record(
                    query=("MERGE (u1:User {username: $source_username}) "
                           "MERGE (u2:User {username: $target_username}) "
                           "MERGE (u1)-[:RETWEETED]-(u2)"),
                    parameters={
                        "source_username": tweet_dict["source_username"],
                        "target_username": tweet_dict["target_username"]}))
        else:
            result_queries.append(
                mgp.Record(
                    query=("MERGE (:User {username: $source_username})"),
                    parameters={
                        "source_username": tweet_dict["source_username"]}))
    return result_queries