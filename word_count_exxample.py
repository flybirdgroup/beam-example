
from __future__ import print_function
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io.textio import ReadFromText, WriteToText



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


input_filename = "./input.txt"
output_filename = "./output.txt"


options = PipelineOptions()
# gcloud_options = options.view_as(GoogleCloudOptions)
# gcloud_options.job_name = "test-job"



options.view_as(StandardOptions).runner = "direct"


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
with beam.Pipeline(options=PipelineOptions()) as p:
    lines = p | 'Create' >> beam.Create([{'cat dog':'b', 'snake cat':'c', 'dog':'b' }],
                                         )
    counts = (
        lines
        | 'Split' >> (beam.ParDo(lambda x: x))
        | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        | 'GroupAndSum' >> beam.CombinePerKey(sum)
    )
    counts | 'Write' >> WriteToText(output_filename)
