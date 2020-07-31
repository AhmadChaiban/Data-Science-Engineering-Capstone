from glob import glob as globlin
from airflow.models import BaseOperator
import logging
import tensorflow as tf
import tensorflow_hub as hub
import numpy as np
from airflow.utils.decorators import apply_defaults

class FeatureExtractorOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 path_features,
                 path_images,
                 path_output,
                 category,
                 *args, **kwargs):

        super(FeatureExtractorOperator, self).__init__(*args, **kwargs)
        self.feature_names = globlin(path_features)
        self.category = category
        self.cateogry_list = self.get_data_files(path_images)
        self.output_path = path_output

    def get_data_files(self, main_directory):
        """
        Using glob to get the paths of all the file names
        to be uploaded
        :param main_directory: the main directory to begin searching
        :return: list of log file paths,
                 list of song file paths
        """
        category_list = globlin(main_directory + f'/{self.category}/*.*', recursive=True)
        # animal_files_list = globlin(main_directory + '/animal/*.*', recursive=True)
        # human_files_list = globlin(main_directory + '/human/*.*', recursive=True)

        return category_list

    def check_if_file_name_exists(self, outputPath):
        for feature_name in self.feature_names:
            if outputPath in feature_name:
                return True
        return False

    def load_image(self, path):
        try:
            # reads image as string
            img = tf.io.read_file(path)
            # Decode the img to a W x H x 3 tensor of type uint8
            img = tf.io.decode_jpeg(img, channels=3)
            # Resize image to 224 x 224 x 3
            img = tf.image.resize_with_pad(img, 128, 128)
            # Converts the data type of uint8 to float32 by adding a new axis
            # img becomes 1 x 224 x 224 x 3 tensor with data type of float32
            # This is required for the mobilenet model
            img = tf.image.convert_image_dtype(img,tf.float32)[tf.newaxis, ...]
        except:
            return None
        return img
    def user_feedback(self, message, task = 'main'):
        if task == 'main':
            print('***************************************')
            print(message)
            print('***************************************')

        elif task == 'sub':
            print('-----------------------')
            print(message)
            print('-----------------------')

    def user_feedback_progress(self, message, task = 'main'):
        if task == 'main':
            print(message, end="\r")

        elif task == 'sub':
            print(message, end="\r")

    def get_image_feature_vectors(self, set_of_images):
        # Definition of module with using tfhub.dev
        module_handle = "https://tfhub.dev/google/imagenet/mobilenet_v1_050_128/feature_vector/4"
        self.user_feedback(f"MobileNetv2 URL {module_handle}")
        # Loads the module
        module = hub.load(module_handle)
        self.user_feedback("Extracting Features...")
        # Loops through all images in a local folder
        index = 1
        for filename in set_of_images:
            id = filename.split('/')[-1].split('.')[0]
            outfile_name = f'{self.output_path}{filename.split("/")[-2]}/{id}.npz'
            if self.check_if_file_name_exists(outfile_name) == True:
                index += 1
                self.user_feedback_progress(f"Features for image {index}/{len(set_of_images)} already exist -- {id}", 'sub')
                continue
            self.user_feedback_progress(f"Extracting Features for image {index}/{len(set_of_images)} -- {id}", 'sub')
            # Loads and pre-process the image
            try:
                img = self.load_image(filename)
                # Calculate the image feature vector of the img
                features = module(img)
                # Remove single-dimensional entries from the 'features' array
                feature_set = np.squeeze(features)
                # Saves the image feature vectors into a file for later use
                # Saves the 'feature_set' to a text file
                np.savetxt(outfile_name, feature_set, delimiter=',')
            except:
                print(f'\nFile may not exist, please verify {id}')
            index += 1

    def extract(self, context):
        self.get_image_feature_vectors(self.cateogry_list)
