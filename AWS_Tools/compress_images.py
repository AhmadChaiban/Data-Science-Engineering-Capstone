from PIL import Image
import numpy as np
from s3_upload import get_data_files
from multiprocessing import Pool

def load_image(path):
    """ Load image from path. Return a numpy array """
    image = Image.open(path)
    return np.asarray(image) / 255

def initialize_K_centroids(X, K):
    """ Choose K points from X at random """
    m = len(X)
    return X[np.random.choice(m, K, replace=False), :]

def find_closest_centroids(X, centroids):
    m = len(X)
    c = np.zeros(m)
    for i in range(m):
        # Find distances
        distances = np.linalg.norm(X[i] - centroids, axis=1)

        # Assign closest cluster to c[i]
        c[i] = np.argmin(distances)

    return c

def compute_means(X, idx, K):
    _, n = X.shape
    centroids = np.zeros((K, n))
    for k in range(K):
        examples = X[np.where(idx == k)]
        mean = [np.mean(column) for column in examples.T]
        centroids[k] = mean
    return centroids

def find_k_means(X, K, max_iters=10):
    centroids = initialize_K_centroids(X, K)
    previous_centroids = centroids
    for _ in range(max_iters):
        idx = find_closest_centroids(X, centroids)
        centroids = compute_means(X, idx, K)
        if (centroids == previous_centroids).all():
            # The centroids aren't moving anymore.
            return centroids
        else:
            previous_centroids = centroids

    return centroids, idx

def compress_image(path):
    print('compressing -- ' + path)
    image = load_image(path)
    folder_and_filename = path.split('/')[-2] + '/' + path.split('/')[-1]
    w, h, d = image.shape
    # print('Image found with width: {}, height: {}, depth: {}'.format(w, h, d))
    X = image.reshape((w * h, d))
    K = 20 # the desired number of colors in the compressed image
    colors, _ = find_k_means(X, K, max_iters=20)
    idx = find_closest_centroids(X, colors)
    idx = np.array(idx, dtype=np.uint8)
    X_reconstructed = np.array(colors[idx, :] * 255, dtype=np.uint8).reshape((w, h, d))
    compressed_image = Image.fromarray(X_reconstructed)
    compressed_image.save('../../capstone data/compressed/' + folder_and_filename)

def bulk_compress(list_of_paths):
    for path in list_of_paths:
        print('compressing -- ' + path)
        compress_image(path)

def distribute_compression(list_of_paths, pool_size = 20):
    with Pool(processes=pool_size) as pool:
        pool.map(compress_image, list_of_paths)

if __name__ == '__main__':
    main_directory = '../../capstone data'
    plant_files_list, animal_files_list, human_files_list = get_data_files('../../capstone data')
    bulk_compress(plant_files_list)
    bulk_compress(animal_files_list)
    bulk_compress(human_files_list)