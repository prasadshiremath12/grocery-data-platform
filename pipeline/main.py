"""Batch Dataflow pipeline: GCS CSV -> validate -> BigQuery (+ dead-letter table).

Run locally (DirectRunner):
  python -m pipeline.main --runner=DirectRunner --project=my-proj --input_dir=data \
      --dataset=grocery --temp_location=gs://my-bucket/tmp --feeds=products,customers

Run on Dataflow:
  python -m pipeline.main --runner=DataflowRunner --project=my-proj --region=asia-south1 \
      --input_dir=gs://my-bucket/landing/2026-10-07 --dataset=grocery \
      --temp_location=gs://my-bucket/tmp --staging_location=gs://my-bucket/staging \
      --setup_file=./setup.py
"""
import csv
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from pipeline.schemas import FEEDS


class ParseCsvRow(beam.DoFn):
    """Parse one CSV line. Valid rows -> main output, bad rows -> 'dead' output."""

    def __init__(self, feed_name, header):
        self.feed_name = feed_name
        self.header = header

    def process(self, line):
        parser = FEEDS[self.feed_name]["parser"]
        try:
            values = next(csv.reader([line]))
            row = dict(zip(self.header, values))
            yield parser(row)
        except Exception as e:  # noqa: BLE001 - we want every failure captured
            yield beam.pvalue.TaggedOutput("dead", {
                "feed": self.feed_name,
                "raw_line": line[:2000],
                "error": str(e)[:500],
                "loaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            })


def read_header(path):
    from apache_beam.io.filesystems import FileSystems
    with FileSystems.open(path) as f:
        return next(csv.reader([f.readline().decode("utf-8").strip()]))


class Options(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, p):
        p.add_argument("--input_dir", required=True, help="Folder (local or gs://) containing the CSVs")
        p.add_argument("--dataset", required=True, help="BigQuery dataset")
        p.add_argument("--feeds", default=",".join(FEEDS), help="Comma separated feeds to load")
        p.add_argument("--write_mode", default="WRITE_TRUNCATE", choices=["WRITE_TRUNCATE", "WRITE_APPEND"])
        p.add_argument("--output_dir", default=None,
                       help="If set, write JSON lines here instead of BigQuery (local testing)")


def run(argv=None):
    opts = PipelineOptions(argv)
    opts.view_as(SetupOptions).save_main_session = True
    o = opts.view_as(Options)
    project = opts.get_all_options().get("project")
    dataset = f"{project}:{o.dataset}" if project else o.dataset

    with beam.Pipeline(options=opts) as p:
        dead_all = []
        for name in [f.strip() for f in o.feeds.split(",") if f.strip()]:
            feed = FEEDS[name]
            path = f"{o.input_dir.rstrip('/')}/{feed['file']}"
            header = read_header(path)

            parsed = (
                p
                | f"Read {name}" >> beam.io.ReadFromParquet(path, skip_header_lines=1)
                | f"Parse {name}" >> beam.ParDo(ParseCsvRow(name, header)).with_outputs("dead", main="ok")
            )
            good = parsed.ok
            if feed["key"]:
                good = (
                    good
                    | f"Key {name}" >> beam.Map(lambda r, k=feed["key"]: (r[k], r))
                    | f"Dedup {name}" >> beam.combiners.Latest.PerKey()
                    | f"Unkey {name}" >> beam.Values()
                )

            if o.output_dir:
                (good | f"Json {name}" >> beam.Map(json.dumps)
                 | f"Write {name} local" >> beam.io.WriteToText(
                     f"{o.output_dir}/{name}", file_name_suffix=".jsonl"))
            else:
                good | f"Load {name}" >> beam.io.WriteToBigQuery(
                    table=f"{dataset}.{feed['table']}",
                    schema=feed["bq_schema"],
                    write_disposition=getattr(beam.io.BigQueryDisposition, o.write_mode),
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                )
            dead_all.append(parsed.dead)

        dead = dead_all | "Merge dead letters" >> beam.Flatten()
        if o.output_dir:
            (dead | "Dead json" >> beam.Map(json.dumps)
             | "Write dead local" >> beam.io.WriteToText(f"{o.output_dir}/dead_letter", file_name_suffix=".jsonl"))
        else:
            dead | "Load dead letters" >> beam.io.WriteToBigQuery(
                table=f"{dataset}.pipeline_dead_letter",
                schema="feed:STRING,raw_line:STRING,error:STRING,loaded_at:DATETIME",
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
