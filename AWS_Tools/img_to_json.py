import json
import base64
from s3_upload import get_data_files

def convert_img_latin(path, output_path):
    output_file_name = path.split('/')[-1].split('.')[0]
    with open(path, mode='rb') as file:
        img = file.read()
    s = base64.encodebytes(img).decode("utf-8")
    with open(output_path + output_file_name + ".json", "w") as file:
        json.dump(s, file)

def save_multiple_json(output_path, category_list):
    for file in category_list:
        print('***********************')
        print('converting -- ' + file)
        print('***********************')
        convert_img_latin(file, output_path)

if __name__ == '__main__':
    main_directory = '../../capstone data'
    plant_files_list, animal_files_list, human_files_list = get_data_files(main_directory)
    save_multiple_json('../../capstone data/json/plant_json/', plant_files_list)
    save_multiple_json('../../capstone data/json/animal_json/', animal_files_list)
    save_multiple_json('../../capstone data/json/human_json/', human_files_list)



