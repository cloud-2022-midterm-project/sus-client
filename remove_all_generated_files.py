import os
import glob


def remove_all_gen_files():
    for file in glob.glob('*.csv'):
        os.remove(file)
    for file in glob.glob('cached_mutations_*'):
        os.remove(file)
    for file in glob.glob('posts_*.csv'):
        os.remove(file)


if __name__ == '__main__':
    remove_all_gen_files()
