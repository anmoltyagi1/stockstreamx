import pandas as pd
from time import sleep
import json
from json import dumps
from kafka import KafkaConsumer, KafkaProducer


def main():
    # create the producer
    prod = KafkaProducer(
        bootstrap_servers=["AWS_IP:9092"],
        value_serializer=lambda x: dumps(x).encode("utf-8"),
    )

    # read data
    data = pd.read_csv("data.csv")

    # send data to kafka
    data.head()
    while 1:
        stock_map = data.sample(1).to_dict(orient="records")[0]
        prod.send("test", value=stock_map)
        sleep(1)

    # clear out data
    prod.flush()


if __name__ == "__main__":
    main()
