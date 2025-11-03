from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
import json
import logging
import time
import requests
from confluent_kafka import Producer

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="produce_user_automation",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Lima"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
    Streams random user data to Kafka topic `users_created` for ~60 seconds per run.
    """,
)
def upload_dag():

    def get_data():
        # small reusable session would be even nicer, but fine for this load
        resp = requests.get("https://randomuser.me/api/", timeout=15)
        resp.raise_for_status()
        return resp.json()["results"][0]

    def format_data(res):
        return {
            "first_name": res["name"]["first"],
            "last_name": res["name"]["last"],
            "email": res["email"],
        }

    def _delivery_report(err, msg):
        if err:
            logging.error("Delivery failed: %s", err)
        else:
            logging.debug("Delivered to %s [%d] @ %d", msg.topic(), msg.partition(), msg.offset())

    @task
    def stream_data():
        producer = Producer({"bootstrap.servers": "broker:9092"})
        end = time.time() + 60  # stream ~60s

        try:
            while time.time() < end:
                try:
                    data = format_data(get_data())
                    producer.produce(
                        "users_created",
                        json.dumps(data).encode("utf-8"),
                        callback=_delivery_report,
                    )
                    # Serve delivery callbacks without blocking
                    producer.poll(0)
                    # be nice to the API and your broker
                    time.sleep(0.2)
                except requests.RequestException as e:
                    logging.warning("HTTP error: %s", e)
                    time.sleep(0.5)
                except Exception as e:
                    logging.exception("Unexpected error: %s", e)
                    time.sleep(0.5)
        finally:
            # ensure messages are flushed even if we error out
            producer.flush()

    stream_data()

dag = upload_dag()
