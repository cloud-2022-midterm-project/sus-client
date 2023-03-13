import os
import glob
import app
app.sync()
for file in glob.glob('cached_mutations_*'):
    os.remove(file)
os.path.exists('cached_posts.csv') and os.remove('cached_posts.csv')
for file in glob.glob('posts_*.csv'):
    os.remove(file)
