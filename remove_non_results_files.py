import os
import glob


def remove_non_results_files():
    # with open('results.csv') as f:
    #     # count the total number of lines in the file
    #     total_lines = sum(1 for line in f)
    #     print('Total number of lines in the results.csv file: ', total_lines)
    for file in glob.glob('*.csv'):
        if file == 'results.csv':
            continue
        os.remove(file)
    for file in glob.glob('cached_mutations_*'):
        os.remove(file)
    for file in glob.glob('posts_*.csv'):
        os.remove(file)


if __name__ == '__main__':
    remove_non_results_files()
