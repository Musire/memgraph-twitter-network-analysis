from multiprocessing import Process
import argparse
import csv
import kafka_utils
import os

# instantiate variables for Kafka IP, PORT, and TOPIC or new defaults
KAFKA_IP = os.getenv('KAFKA_IP', 'kafka')
KAFKA_PORT = os.getenv('KAFKA_PORT', '9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'retweets')

# file path for Twitter data
TWITTER_DATA = "data/scraped_tweets.csv"

# attempt to create float from input
# if error equals ValueError, raise a parsed error message
# if float is not between 0.0 and 3.0, raise a parsed error message
def restricted_float(x):
    try:
        x = float(x)
    except ValueError:
        raise argparse.ArgumentTypeError("%r not a floating-point literal" % (x,))
    if x < 0.0 or x > 3.0:
        raise argparse.ArgumentTypeError("%r not in range [0.0, 3.0]" % (x,))
    return x


# instantiate an ArgumentParser from argparse
# add argument to parser and return all parsed arguments
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stream-delay', type=restricted_float, default=2.0,
                        help='Seconds to wait before producing a new message (MIN=0.0, MAX=3.0)')
    value = parser.parse_args()
    return value


# create generator using a context manager to input twitter data from csv
# for every row in csv, you will yield a data dictionary with source and target info
def generate_tweets():
    while True:
        with open(TWITTER_DATA) as file:
            csvReader = csv.DictReader(file)
            for rows in csvReader:
                data = {
                    'source_username': rows['source_username'],
                    'target_username': rows['target_username']
                }
                yield data

# instantiate arguents from previous method parse_arguments
# instantiate empty list for process list
# create a kafka topic with values instantiated previously
# Create a producer process, start the process, and append to process list
# Create a consumer process, start the process, and append to process list
# join both processes to block rest of the executions
def main():
    args = parse_arguments()
    process_list = list()

    kafka_utils.create_topic(KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC)

    p1 = Process(target=lambda: kafka_utils.producer(
        KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, generate_tweets, args.stream_delay))
    p1.start()
    process_list.append(p1)

    p2 = Process(target=lambda: kafka_utils.consumer(
        KAFKA_IP, KAFKA_PORT, KAFKA_TOPIC, "Kafka"))
    p2.start()
    process_list.append(p2)

    for process in process_list:
        process.join()


# run the main function
if __name__ == "__main__":
    main()