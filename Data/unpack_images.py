import shutil
from glob import glob as globlin ## The 7bb globlin

def user_feedback(message):
    print("*********************************")
    print(message)
    print("*********************************")


def getFileList(cateogry_path):
    user_feedback('Unpacking '+ cateogry_path + ' folders...')
    image_paths = globlin('D:/capstone data/' + cateogry_path + '/**/*.*', recursive=True)
    target_path = 'D:/capstone data/' + cateogry_path + '/' + cateogry_path
    move_files(image_paths, target_path)
    user_feedback('Completed')

def move_files(image_paths, target_path):
    index = 0
    for single_path in image_paths:
        print('processing: ' + single_path)
        save_path = target_path + '_id_' + str(index) + '.'+single_path.split('.')[-1]
        shutil.move(single_path, save_path)
        print('moved as: ' + save_path)
        index += 1


# if __name__ == '__main__':
#     getFileList('animal')
#     getFileList('human')
#     getFileList('plant')


