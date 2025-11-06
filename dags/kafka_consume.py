from airflow.decorators import dag, task
from datetime import timedelta
import pendulum
import json
import logging
import time
from confluent_kafka import Consumer, KafkaException, KafkaError
import json, time, logging
from scripts.azure_upload import upload_to_adls


DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id="consume_user_automation",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Lima"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="""
    Streams random user data to Kafka topic `users_created` for ~60 seconds per run.
    """,
)
def download_dag():

    @task
    def consume_users_created(duration_sec: int = 60, max_messages: int | None = None):
        """
        Consume messages from topic 'users_created' for up to `duration_sec`
        (or until `max_messages` is reached). Returns a tiny summary dict so
        Airflow can XCom it if used inside a task.
        """
        conf = {
            "bootstrap.servers": "broker:9092",          # internal listener (compose)
            "group.id": "user_automation_consumer",      # change if you want independent offsets
            "auto.offset.reset": "earliest",             # 'latest' once in steady state
            "enable.auto.commit": False,                 # commit after successful processing
            # Optional hardening:
            # "max.poll.interval.ms": 600000,
            # "session.timeout.ms": 10000,
        }
        c = Consumer(conf)
        consumed = 0
        end = time.time() + duration_sec

        try:
            c.subscribe(["users_created"])
            while time.time() < end and (max_messages is None or consumed < max_messages):
                msg = c.poll(1.0)  # seconds
                if msg is None:
                    continue

                if msg.error():
                    # benign end-of-partition
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logging.error("Kafka error: %s", msg.error())
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    # >>> Your business logic here <<<
                    logging.info(
                        "Consumed key=%s partition=%s offset=%s payload=%s",
                        None if msg.key() is None else msg.key().decode("utf-8", "ignore"),
                        msg.partition(),
                        msg.offset(),
                        payload,
                    )
                    # Commit only after successful processing
                    c.commit(message=msg, asynchronous=False)
                    consumed += 1

                except json.JSONDecodeError as e:
                    logging.warning("Bad JSON at offset %s: %s", msg.offset(), e)
                    # Decide: commit (skip) or not. Here we skip committing so we can reprocess later.
                except Exception as e:
                    logging.exception("Handler error at offset %s: %s", msg.offset(), e)
                    # Decide: commit or retry later. Not committing will retry next time.

            return {"consumed": consumed}

        except KafkaException as e:
            logging.exception("KafkaException: %s", e)
            return {"consumed": consumed, "error": str(e)}
        finally:
            c.close()

    consume_users_created()

dag = download_dag()
