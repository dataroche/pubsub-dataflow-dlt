import os
import uuid

from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

import argparse
import logging
import orjson

from apache_beam import (
    DoFn,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WithKeys,
    WindowInto,
    GroupByKey,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


def parse_json(data: bytes) -> Iterable[dict]:
    yield orjson.loads(data)


def get_event_key(data: dict) -> str:
    return data.get("eventName") or "unknown"


class ParseJsonAndGroup(PTransform):
    def __init__(
        self,
        window_size_secs: int = 5,
        table_name_data_key: Optional[str] = None,
    ):
        super().__init__()
        self.window_size_secs = window_size_secs
        self.table_name_data_key = table_name_data_key

    def expand(self, pcoll):
        return (
            pcoll
            | WindowInto(FixedWindows(self.window_size_secs))
            | "Parse JSON" >> ParDo(parse_json).with_output_types(dict)
            | "Extract event key"
            >> WithKeys(
                lambda data: data.get(self.table_name_data_key, None) or "default"
            ).with_output_types(Tuple[str, dict])
            | "Create batches" >> GroupByKey()
        )


class ProcessWithDlt(DoFn):
    def __init__(
        self,
        dataset_name: str,
        sql_url: str,
        pipeline_name: str = "pubsub_to_postgres",
        table_name_prefix: str = "",
    ):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.dataset_name = dataset_name
        self.sql_url = sql_url
        self.table_name_prefix = table_name_prefix

    def process(self, element: Tuple[str, Iterable[dict]]):
        """Write messages in a batch to the DLT pipeline"""

        import dlt

        RUN_ID = uuid.uuid1().hex
        dlt_pipeline = dlt.pipeline(
            pipeline_name=self.pipeline_name,
            pipelines_dir=f"/tmp/.dlt/run-{RUN_ID}",
            destination="postgres",
            dataset_name=self.dataset_name,
            credentials=self.sql_url,
        )

        event_name, batch = element

        dlt_pipeline.run(
            batch,
            table_name=f"{self.table_name_prefix}{event_name}",
        )


@dataclass
class Args:
    input_topic: str
    input_subscription: str
    window_size_secs: int
    dataset_name: str
    sql_url: str
    table_name_data_key: str
    table_name_prefix: str


def run(args: Args, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(pipeline_args, streaming=True)

    sql_url = args.sql_url or os.environ.get("DATABASE_URL")

    if not sql_url:
        raise RuntimeError("Missing --sql_url or DATABASE_URL environment variable")

    if not (args.input_topic or args.input_subscription):
        raise RuntimeError("Either input_topic or input_subscription is required.")

    with Pipeline(options=pipeline_options) as pipeline:
        _ = (
            pipeline
            | "Read from Pub/Sub"
            >> io.ReadFromPubSub(
                topic=args.input_topic, subscription=args.input_subscription
            )
            | "Parse and group"
            >> ParseJsonAndGroup(
                table_name_data_key=args.table_name_data_key,
                window_size_secs=args.window_size_secs,
            )
            | "Write to DLT pipeline"
            >> ParDo(
                ProcessWithDlt(
                    dataset_name=args.dataset_name,
                    sql_url=sql_url,
                    table_name_prefix=args.table_name_prefix,
                )
            )
        )


def main():
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument(
        "--input_subscription",
        help="The Cloud Pub/Sub subscription to read from."
        '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
    )
    parser.add_argument("--sql_url", help="The output SQL connection URL", default="")
    parser.add_argument(
        "--window_size_secs",
        help="The max buffering duration between batched inserts",
        default=5,
    )
    parser.add_argument(
        "--dataset_name", help="The output Postgres schema", default="analytics"
    )
    parser.add_argument(
        "--table_name_data_key",
        help="When reading json records, use this key as the output table name - i.e. data[table_name_data_key]",
        default="",
    )
    parser.add_argument(
        "--table_name_prefix",
        help="This prefix will be appended to all table names",
        default="",
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(
        Args(**vars(known_args)),
        pipeline_args,
    )
