from glob import glob as globlin ## The 7bb globlin

def get_feature_file_list(path):
    feature_list = globlin(path)
    if len(feature_list) >= 1:
        return True, len(feature_list)
    else:
        return False

if __name__ == '__main__':
    category = 'plant'
    print(get_feature_file_list(f'../../capstone data/imgFeatures/{category}/*.*'))
