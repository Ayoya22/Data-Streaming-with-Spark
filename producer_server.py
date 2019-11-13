import pathlib
import json
import logging
import pykafka
import time

INPUT_FILE = pathlib.Path(__file__).parent.joinpath('resources/police-department-calls-for-service2.json')

logger = logging.getLogger(__name__)


def read_file() -> json:
    with open(INPUT_FILE, 'r') as f:
        data = json.load(f)
    return data


def generate_data() -> None:
    data = read_file()
    for i in data:
        message = dict_to_binary(i)
        producer.produce(message)
        # print(message)
        time.sleep(2)



def dict_to_binary(json_dict: dict) -> bytes:
    return json.dumps(json_dict).encode('utf-8')

if __name__ == "__main__":
    client = pykafka.KafkaClient("localhost:9092")
    print("topics", client.topics)
    producer = client.topics[b'test'].get_producer()

    generate_data()
