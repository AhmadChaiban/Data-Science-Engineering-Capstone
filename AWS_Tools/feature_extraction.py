import tensorflow as tf
import tensorflow_hub as hub
from misc import user_feedback, user_feedback_progress
import numpy as np
from s3_upload import get_data_files
from glob import glob as globlin ## The 7bb globlin

def load_image(path):
    try:
        # reads image as string
        img = tf.io.read_file(path)
        # Decode the img to a W x H x 3 tensor of type uint8
        img = tf.io.decode_jpeg(img, channels=3)
        # Resize image to 224 x 224 x 3
        # img = tf.image.resize_with_pad(img, 224, 224)
        img = tf.image.resize_with_pad(img, 128, 128)
        # Converts the data type of uint8 to float32 by adding a new axis
        # img becomes 1 x 224 x 224 x 3 tensor with data type of float32
        # This is required for the mobilenet model
        img = tf.image.convert_image_dtype(img,tf.float32)[tf.newaxis, ...]
    except:
        return None
    return img

def check_if_file_name_exists(outputPath):
    for feature_name in feature_names:
        if outputPath in feature_name:
            return True
    return False

def get_image_feature_vectors(set_of_images):
    # Definition of module with using tfhub.dev
    module_handle = "https://tfhub.dev/google/imagenet/mobilenet_v1_050_128/feature_vector/4"
    ## https://tfhub.dev/google/imagenet/inception_v1/feature_vector/4
    ## https://tfhub.dev/google/imagenet/mobilenet_v2_100_224/feature_vector/4
    ## https://tfhub.dev/google/imagenet/mobilenet_v1_050_128/feature_vector/4
    ## https://tfhub.dev/google/imagenet/mobilenet_v2_140_224/feature_vector/4
    user_feedback(f"MobileNetv2 URL {module_handle}")
    # Loads the module
    module = hub.load(module_handle)
    user_feedback("Extracting Features...")
    # Loops through all images in a local folder
    index = 1
    for filename in set_of_images:
        id = filename.split('/')[-1].split('.')[0]
        outfile_name = f'../../capstone data/imgFeatures/{filename.split("/")[-2]}/{id}.npz'
        if check_if_file_name_exists(outfile_name) == True:
            index += 1
            user_feedback_progress(f"Features for image {index}/{len(set_of_images)} already exist -- {id}", 'sub')
            continue
        user_feedback_progress(f"Extracting Features for image {index}/{len(set_of_images)} -- {id}", 'sub')
        # Loads and pre-process the image
        try:
            img = load_image(filename)
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

if __name__ == '__main__':
    feature_names = globlin('../../capstone data/imgFeatures/*/*.*')
    plant_files_list, animal_files_list, human_files_list = get_data_files('../../capstone data')
    # get_image_feature_vectors(plant_files_list)
    # get_image_feature_vectors(animal_files_list)
    get_image_feature_vectors(human_files_list)
