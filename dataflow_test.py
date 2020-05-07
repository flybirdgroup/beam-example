import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.avroio import WriteToAvro,ReadFromAvro,ReadAllFromAvro
import avro.schema, csv, codecs


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


input_filename = "gs://zz_bucket/avro/account_id_schema_new.avro"
output_filename = "gs://zz_bucket/avro/account_id_schema_out.avro"


dataflow_options = ['--project=query-11','--job_name=test-job','--temp_location=gs://dataflow_s/tmp']
dataflow_options.append('--staging_location=gs://dataflow_s/stage')
options = PipelineOptions(dataflow_options)
gcloud_options = options.view_as(GoogleCloudOptions)

# gcloud_options.job_name = "test-job"


options.view_as(StandardOptions).runner = "dataflow"


with beam.Pipeline(options=options) as p:
    rows = (
            p | 'Read' >> beam.io.ReadFromAvro(input_filename)
              | beam.Filter(lambda x: (x["FIELD_1"] > 5000))
              | beam.Map(lambda x: x)
        | WriteToText(output_filename)
    )



