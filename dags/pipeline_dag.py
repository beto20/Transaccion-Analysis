from __future__ import annotations

import json
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import Optional, List

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from confluent_kafka import Consumer, KafkaError, KafkaException

from scripts.azure_upload import upload_to_adls
from scripts.helpers import add_date_suffix

class Transaction:
    def __init__(self, firstname, lastname):
        self.firstname = firstname
        self.lastname = lastname

    def to_dict(self):
        return {"firstname": self.firstname, "lastname": self.lastname}

LOCAL_DIR = Path("/opt/airflow/data")
LOCAL_DIR.mkdir(parents=True, exist_ok=True)

CONTAINER_NAME = "datalake"
BLOB_NAME = "raw/airflow/G5/archivo_G5_test.parquet"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def _upload_to_storage(local_path: Path) -> None:
    new_blob_name = add_date_suffix(BLOB_NAME)
    upload_to_adls(
        local_file_path=str(local_path),
        container_name=CONTAINER_NAME,
        blob_name=new_blob_name,
    )

@dag(
    dag_id="consume_user_automation_v2",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Lima"),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    doc_md="Consumes `users_created`, batches to Parquet, uploads to ADLS.",
)
def download_dag():

    @task
    def consume_and_batch_to_adls(
        duration_sec: int = 60,
        max_messages: Optional[int] = None,
        batch_size: int = 20,
    ) -> dict:
        log = logging.getLogger("consume_and_batch_to_adls")
        conf = {
            "bootstrap.servers": "broker:9092",
            "group.id": "user_automation_consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
        c = Consumer(conf)

        buffer: List[Transaction] = []
        consumed = 0
        uploaded_files = 0
        end = time.time() + duration_sec

        def flush_and_upload():
            logging.info("START_FLUSH")
            nonlocal uploaded_files, buffer
            if not buffer:
                return
            df = pd.DataFrame([t.to_dict() for t in buffer])
            # atomic write
            tmp_path = LOCAL_DIR / f"batch_{int(time.time()*1e6)}.parquet.tmp"
            final_path = Path(str(tmp_path).removesuffix(".tmp"))
            df.to_parquet(tmp_path, engine="pyarrow", index=False)
            tmp_path.replace(final_path)
            _upload_to_storage(final_path)
            try:
                final_path.unlink(missing_ok=True)
            except Exception:
                log.warning("Could not remove local file %s", final_path)
            buffer.clear()
            uploaded_files += 1

        try:
            logging.info("START_SUBSCRIBE")
            
            c.subscribe(["users_created"])
            while time.time() < end and (max_messages is None or consumed < max_messages):
                msg = c.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    log.error("Kafka error: %s", msg.error())
                    continue

                try:
                    logging.info("START_MESSAGE_PROCESSING")
                    
                    payload = json.loads(msg.value().decode("utf-8"))
                    trx = Transaction(payload.get("first_name"), payload.get("last_name"))
                    logging.info("trx: %s", trx.to_dict())
                    buffer.append(trx)

                    c.commit(message=msg, asynchronous=False)
                    consumed += 1

                    if len(buffer) >= batch_size:
                        flush_and_upload()

                except json.JSONDecodeError as e:
                    log.warning("Bad JSON at offset %s: %s. Skipping.", msg.offset(), e)
                    c.commit(message=msg, asynchronous=False)
                except Exception as e:
                    log.exception("Handler error at offset %s: %s", msg.offset(), e)
                    time.sleep(0.2)

            flush_and_upload()
            return {"consumed": consumed, "uploads": uploaded_files}

        except KafkaException as e:
            log.exception("KafkaException: %s", e)
            try:
                flush_and_upload()
            except Exception:
                pass
            return {"consumed": consumed, "uploads": uploaded_files, "error": str(e)}
        finally:
            c.close()

    consume_and_batch_to_adls()

dag = download_dag()
