from multiprocess import Pool
import numpy as np
from s3_create import upload_files_s3, get_key_secret, create_bucket, get_data_files, get_bucket

def get_process_number(category_list):
    if len(category_list)%30000 == 0:
        additional_process = 0
    else:
        additional_process = 1
    return int(np.floor(len(category_list)/30000) + additional_process)

def get_parallel_process_list(pool, number_of_processes, category_list):
    parallel_processes = []
    index = 0
    factor = 30000
    for i in range(1, number_of_processes+1):
        last_index = i*factor
        if len(category_list) - last_index < 30000:
            last_index = len(category_list)%30000
        parallel_processes.append(
            pool.apply_async(upload_files_s3, [category_list[index: last_index],
                                               'capstone-project-2187',
                                               s3_bucket])
        )
        index += last_index
    return parallel_processes

def finalize_processes(list_of_processes):
    for process in list_of_processes:
        process.get()


def execute_parallel_uploads(plant_files_list, animal_files_list, human_files_list):
    num_processes = get_process_number(plant_files_list) + get_process_number(animal_files_list) + get_process_number(human_files_list)
    print(num_processes)
    pool = Pool(processes= num_processes)

    plants = get_parallel_process_list(pool,
                                       get_process_number(plant_files_list),
                                       plant_files_list)

    animals = get_parallel_process_list(pool,
                                       get_process_number(animal_files_list),
                                       animal_files_list)

    humans = get_parallel_process_list(pool,
                                       get_process_number(human_files_list),
                                       human_files_list)

    pool.close()
    pool.join()

    finalize_processes(plants)
    finalize_processes(animals)
    finalize_processes(humans)

if __name__ == '__main__':

    KEY, SECRET = get_key_secret()

    main_directory = "../../capstone data"
    plant_files_list, animal_files_list, human_files_list = get_data_files(main_directory)

    s3_client = create_bucket('capstone-project-2187',
                              KEY,
                              SECRET,
                              'us-west-2')

    s3_resource = get_bucket('capstone-project-2187',
                                  KEY,
                                  SECRET,
                                  'us-west-2')

    s3_bucket = s3_resource.Bucket('capstone-project-2187')


    execute_parallel_uploads(plant_files_list,
                             animal_files_list,
                             human_files_list)