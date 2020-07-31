from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import FeatureExtractorOperator
from airflow.operators import FeatureLabelOperator
from airflow.operators import FeatureVerificationOperator
from airflow.operators import CsvVerificationOperator
from airflow.operators import s3UploadOperator

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

preVerificationStandbyOperator = DummyOperator(
    task_id='Verification_Standby',
    dag=dag
)

Feature_verify_plant = FeatureVerificationOperator(
    task_id = 'feature_verify_plant',
    dag = dag,
    path = '../../../../capstone data/imgFeatures',
    category = 'plant'
)

Feature_verify_animal = FeatureVerificationOperator(
    task_id = 'feature_verify_animal',
    dag = dag,
    path = '../../../../capstone data/imgFeatures',
    category = 'animal'
)

Feature_verify_human = FeatureVerificationOperator(
    task_id = 'feature_verify_human',
    dag = dag,
    path = '../../../../capstone data/imgFeatures',
    category = 'human'
)

preDataStorageStandbyOperator = DummyOperator(
    task_id='CSV_Storage_Standby',
    dag=dag
)

create_csv_plant = FeatureLabelOperator(
    task_id = 'feature_labeler_plant',
    dag = dag,
    main_path = '../../../../capstone data/imgFeatures',
    category = 'plant'
)

create_csv_animal = FeatureLabelOperator(
    task_id = 'feature_labeler_animal',
    dag = dag,
    main_path = '../../../../capstone data/imgFeatures',
    category = 'animal'
)

create_csv_human = FeatureLabelOperator(
    task_id = 'feature_labeler_human',
    dag = dag,
    main_path = '../../../../capstone data/imgFeatures',
    category = 'human'
)

preCSVVerificationStandbyOperator = DummyOperator(
    task_id='CSV_Verification_Standby',
    dag=dag
)

csv_verification_plant = CsvVerificationOperator(
    task_id = 'plant_csv_verification',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'plant'

)

csv_verification_animal = CsvVerificationOperator(
    task_id = 'animal_csv_verification',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'animal'

)

csv_verification_human = CsvVerificationOperator(
    task_id = 'human_csv_verification',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'human'

)

preUploadStandbyOperator = DummyOperator(
    task_id = 'Pre_upload_Standby',
    dag = dag
)

s3_upload_plant = s3UploadOperator(
    task_id = 'plant_upload',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'plant',
    bucket_name = 'capstone-project-2187'
)

s3_upload_animal = s3UploadOperator(
    task_id = 'animal_upload',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'animal',
    bucket_name = 'capstone-project-2187'
)

s3_upload_human = s3UploadOperator(
    task_id = 'human_upload',
    dag = dag,
    path = '../../../dataFunctions/final_data',
    category = 'human',
    bucket_name = 'capstone-project-2187'
)

end_operator = DummyOperator(
    task_id='end_execution',
    dag=dag
)

start_operator >> [extract_img_features_plant, extract_img_features_animal, extract_img_features_human] >> preVerificationStandbyOperator

preVerificationStandbyOperator >> [Feature_verify_plant, Feature_verify_animal, Feature_verify_human] >> preDataStorageStandbyOperator

preDataStorageStandbyOperator >> [create_csv_plant, create_csv_animal, create_csv_human] >> preCSVVerificationStandbyOperator

preCSVVerificationStandbyOperator >> [csv_verification_plant, csv_verification_animal, csv_verification_human] >> preUploadStandbyOperator

preUploadStandbyOperator >> [s3_upload_plant, s3_upload_animal, s3_upload_human] >> end_operator







