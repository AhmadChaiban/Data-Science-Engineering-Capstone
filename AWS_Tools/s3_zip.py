from zipfile import ZipFile
import os
from os.path import basename
from misc import user_feedback

def create_zip_file(path, zip_name):
    """
    Creating a zip file for each category
    :param path: the path of the folder to be compressed
    :param zip_name: the name of the output zip file
    :return: None
    """
    with ZipFile(zip_name, 'w') as zipObj:
        for folderName, subfolders, filenames in os.walk(path):
            user_feedback('Compressing --- ' + folderName, task = 'main')
            for index, filename in enumerate(filenames):
                print('Adding file --- ' + filename + ' --- ' + str(index + 1) + '/' + str(len(filenames)), end = '\r')
                filePath = os.path.join(folderName, filename)
                zipObj.write(filePath, basename(filePath))

if __name__ == '__main__':
    main_path = '../../capstone data/imgFeatures'
    zip_path = '../../capstone data/compressed/'
    create_zip_file(main_path + '/plant', zip_path + 'plant.zip')
    create_zip_file(main_path + '/animal', zip_path + 'animal.zip')
    create_zip_file(main_path + '/human', zip_path + 'human.zip')