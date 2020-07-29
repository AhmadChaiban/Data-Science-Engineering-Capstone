from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin
from .operators.FeatureExtractorOperator import FeatureExtractorOperator
from .operators.FeatureLabel import FeatureLabelOperator
import operators
# import helpers

# Defining the plugin class
class imagePipelinePlugin(AirflowPlugin):
    name = "image_pipeline_plugin"
    operators = [
        FeatureExtractorOperator,
        FeatureLabelOperator
    ]
    # helpers = [
    #     helpers.SqlQueries
    # ]
