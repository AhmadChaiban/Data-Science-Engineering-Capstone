from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from .operators.FeatureExtractorOperator import FeatureExtractorOperator
from .operators.FeatureLabelOperator import FeatureLabelOperator
from .operators.FeatureVerificationOperator import FeatureVerificationOperator
from .operators.CsvVerificationOperator import CsvVerificationOperator
from .operators.s3UploadOperator import s3UploadOperator
import operators
# import helpers

# Defining the plugin class
class imagePipelinePlugin(AirflowPlugin):
    name = "image_pipeline_plugin"
    operators = [
        FeatureExtractorOperator,
        FeatureLabelOperator,
        FeatureVerificationOperator,
        CsvVerificationOperator,
        s3UploadOperator
    ]
    # helpers = [
    #     helpers.SqlQueries
    # ]
