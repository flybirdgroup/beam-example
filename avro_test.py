
from __future__ import print_function
import logging
import apache_beam as beam
import pandas as pd
import pandavro as pdx
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.avroio import WriteToAvro,ReadFromAvro,ReadAllFromAvro
import avro.schema, csv, codecs


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


input_filename = "./data/account_id_schema_new.avro"
output_filename = "./data/output.csv"


options = PipelineOptions()
# gcloud_options = options.view_as(GoogleCloudOptions)
# gcloud_options.job_name = "test-job"



options.view_as(StandardOptions).runner = "direct"


# class Split(beam.DoFn):
#
#     def process(self, element,columns):
#         """
#         Splits each row on commas and returns a dictionary representing the
#         row
#         """
#         country, duration, user = element.split(",")
#         return [{
#             'country': country,
#             'duration': float(duration),
#             'user': user
#         }]


class Split(beam.DoFn):

    def process(self, element):
        """
        Splits each row on commas and returns a dictionary representing the
        row
        """
        country, duration, user = element.split(",")

        return [{
            'country': country,
            'duration': float(duration),
            'user': user
        }]


class Collectfield_1(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing country and duration
        """

        result = [
            (element['ACNO'], element['FIELD_1'])
        ]
        return result


class Collectfield_2(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing country and user name
        """
        result = [
            (element['ACNO'], element['FIELD_2'])
        ]
        return result


class WriteToCSV(beam.DoFn):

    def process(self, element):
        """
        Prepares each row to be written in the csv
        """
        result = [
            "{},{},{}".format(
                element[0],
                element[1]['FIELD_1'],
                element[1]['FIELD_2']
            )
        ]
        return result


with beam.Pipeline(options=options) as p:
    rows = (
            p | 'Read' >> beam.io.ReadFromAvro(input_filename)
              | beam.Filter(lambda x: (x["FIELD_1"] > 5000))
              | beam.Map(lambda x: x["FIELD_1"]/5)
        | WriteToText(output_filename)
    )





# if __name__ == '__main__':
#     # schema = avro.schema.parse(open(input_filename, "rb").read())
#     # print(schema)
#     # print(schema)
#     df = pdx.read_avro(input_filename)
#     with beam.Pipeline(options=options) as p:
#         rows = (
#                 p | 'Read' >> beam.io.ReadFromAvro(input_filename)
#                   | beam.Filter(lambda x: x['FIELD_1'] < 5000)
#                   | 'Write' >> beam.io.WriteToText(output_filename)
#         )


        #
        # timings = (
        #         rows |
        #         beam.ParDo(CollectTimings()) |
        #         "Grouping timings" >> beam.GroupByKey() |
        #         "Calculating average" >> beam.CombineValues(
        #     beam.combiners.MeanCombineFn()
        # )
        # )
        #
        # users = (
        #         rows |
        #         beam.ParDo(CollectUsers()) |
        #         "Grouping users" >> beam.GroupByKey() |
        #         "Counting users" >> beam.CombineValues(
        #     beam.combiners.CountCombineFn()
        # )
        # )

        # to_be_joined = (
        #         {
        #             'timings': timings,
        #             'users': users
        #         } |
        #         beam.CoGroupByKey() |
        #         beam.ParDo(WriteToCSV()) |
        #         WriteToText(output_filename)
        # )

