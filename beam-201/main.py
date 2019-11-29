import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
import apache_beam.transforms.trigger as trigger
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import datetime

import json
import string


def parse_message(message):
  parsed_message = json.loads(message)
  created_at = parsed_message['created_at']
  return window.TimestampedValue(
    (parsed_message['id'], parsed_message),
    datetime.timestamp(datetime.fromisoformat(created_at))
  )

def sum_value(msg_groups):
  (key, msgs) = msg_groups
  value_list = sum([msg['value'] for msg in msgs])
  return (key, sum(value_list))

def format_for_bigquery(tuple):
  return {
    "key": tuple[0],
    "sum": tuple[1]
  }

def run(argv=None, save_main_session=True):
  """Main entry point; defines and runs the wordcount pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      required=True,
                      help='Input Pub/Sub subscription to read from.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output BigQuery table to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
  p = beam.Pipeline(options=pipeline_options)

  # Read the text file[pattern] into a PCollection.
  (p | 'read' >> ReadFromPubSub(subscription=known_args.input)
     | 'parse event time' >> beam.Map(parse_message)
     | 'window per minute' >> beam.WindowInto(
                                window.FixedWindows(1),
                                trigger=trigger.DefaultTrigger(),
                                accumulation_mode=trigger.AccumulationMode.DISCARDING)
     | 'group by id' >> beam.GroupByKey()
     | 'sum_value' >> beam.Map(sum_value)
     | 'format for bq' >> beam.Map(format_for_bigquery)
     | 'write to bigquery' >> WriteToBigQuery(
                                table=known_args.output,
                                schema='key:INTEGER,sum:FLOAT',
                                create_disposition='CREATE_IF_NEEDED',
                                write_disposition='WRITE_APPEND'))

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()