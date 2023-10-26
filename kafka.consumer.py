from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
from s3fs import S3FileSystem


def main():
    cons = KafkaConsumer(
        "test",
        bootstrap_servers=["AWS_IP:9092"],
        value_deserializer=lambda x: loads(x.decode("utf-8")),
    )

    s3 = S3FileSystem(anon=False)

    for i, c in enumerate(cons):
        with s3.open(
            "s3://kafka-stockstreamx-anmol/stockstream_{}.json".format(i), "w"
        ) as file:
            json.dump(c.value, file)


if __name__ == "__main__":
    main()
