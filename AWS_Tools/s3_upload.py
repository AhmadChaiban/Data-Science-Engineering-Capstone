from s3_multi_upload import multi_part_upload_with_s3
from glob import glob as globlin ## the 7bb globlin

def get_data_files(main_directory):
    """
    Using glob to get the paths of all the file names
    to be uploaded

    :param main_directory: the main directory to begin searching
    :return: list of log file paths,
             list of song file paths

    """
    plant_files_list = globlin(main_directory + '/plant/*.*', recursive=True)
    animal_files_list = globlin(main_directory + '/animal/*.*', recursive=True)
    human_files_list = globlin(main_directory + '/human/*.*', recursive=True)

    return plant_files_list, animal_files_list, human_files_list

def log_upload_progress(index, path):
    f = open(path, 'w')
    f.write("Index to continue from: " + str(index))
    f.close()

def read_index_to_continue_from(path):
    with open(path, 'r') as file:
        data = file.read().replace('\n', '')
        return int(data.split(' ')[-1])

if __name__ == '__main__':
    plant_files_list, animal_files_list, human_files_list = get_data_files('../../capstone data')
    full_list_directories = plant_files_list #+ animal_files_list + human_files_list
    index_to_continue = read_index_to_continue_from('log.txt')
    for index, file in enumerate(full_list_directories):
        if index < index_to_continue:
            print('skipping -- ' + str(index) + '/' + str(index_to_continue-1), end="\r")
            continue
        print('\n')
        key_path = 'raw_data' + '/' + file.split('/')[-2] + '/' + file.split('/')[-1]
        multi_part_upload_with_s3(file, key_path)
        log_upload_progress(index, 'log.txt')