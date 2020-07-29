from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import FeatureExtractorOperator
from airflow.operators import FeatureLabelOperator

default_args = {
    'owner': 'ahmad-chaiban',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email': ['ahmadchaiban@gmail.com'],
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'image_pipeline',
    default_args=default_args,
    description='Extract features from images, create, load, transform and upload to DB',
    schedule_interval='0 * * * *'
)

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

extract_img_features_plant = FeatureExtractorOperator(
    task_id = 'feature_extraction_plant',
    dag = dag,
    path_features = '../../../../capstone data/imgFeatures/*/*.*',
    path_images = '../../../../capstone data',
    path_output = '../../../../capstone data/imgFeatures/',
    category = 'plant'
)
extract_img_features_animal = FeatureExtractorOperator(
    task_id = 'feature_extraction_animal',
    dag = dag,
    path_features = '../../../../capstone data/imgFeatures/*/*.*',
    path_images = '../../../../capstone data',
    path_output = '../../../../capstone data/imgFeatures/',
    category = 'animal'
)
extract_img_features_human = FeatureExtractorOperator(
    task_id = 'feature_extraction_human',
    dag = dag,
    path_features = '../../../../capstone data/imgFeatures/*/*.*',
    path_images = '../../../../capstone data',
    path_output = '../../../../capstone data/imgFeatures/',
    category = 'human'
)

create_csv = FeatureLabelOperator(
    task_id = 'feature_labeler',
    dag = dag,
    main_path = '../../../../capstone data/imgFeatures',
    output_path = '../../../../capstone data/imgFeatures'
)

start_operator >> [extract_img_features_plant, extract_img_features_animal, extract_img_features_human]

[extract_img_features_plant, extract_img_features_animal, extract_img_features_human] >> create_csv

### don't forget the two data quality checks

# create_csv >> [load_user_dimension_table, load_song_dimension_table,
#                          load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
#
# run_quality_checks >> end_operator








