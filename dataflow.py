import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)




dataflow_options = ['--project=query-11','--job_name=amaz','--temp_location=gs://dataflow_s/tmp','--region=us-central1']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# gcloud_options.job_name = "test-job"


options.view_as(StandardOptions).runner = "dataflow"


input_filename = "gs://dataflow_s/input.csv"
output_filename = "gs://dataflow_s/output.csv"


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


class CollectTimings(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing country and duration
        """

        result = [
            (element['country'], element['duration'])
        ]
        print(result)
        return result


class CollectUsers(beam.DoFn):

    def process(self, element):
        """
        Returns a list of tuples containing country and user name
        """
        result = [
            (element['country'], element['user'])
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
                element[1]['users'][0],
                element[1]['timings'][0]
            )
        ]
        with open(output_filename,'a') as f:
            f.write(result[0]+"\n")
        return result



if __name__ == '__main__':
    with beam.Pipeline(options=options) as p:
        rows = (
                p |
                ReadFromText(input_filename) |
                beam.ParDo(Split())
        )

        timings = (
                rows |
                beam.ParDo(CollectTimings()) |
                "Grouping timings" >> beam.GroupByKey() |
                "Calculating average" >> beam.CombineValues(
            beam.combiners.MeanCombineFn()
        )
        )

        users = (
                rows |
                beam.ParDo(CollectUsers()) |
                "Grouping users" >> beam.GroupByKey() |
                "Counting users" >> beam.CombineValues(
            beam.combiners.CountCombineFn()
        )
        )

        to_be_joined = (
                {
                    'timings': timings,
                    'users': users
                } |
                beam.CoGroupByKey() |
                beam.ParDo(WriteToCSV())
                |WriteToText(output_filename)
        )

# import pandavro as pdx
# import pandas as pd
#
# df = pd.read_csv(output_filename)
# pdx.to_avro('gs://dataflow_s/output.avro',df)
