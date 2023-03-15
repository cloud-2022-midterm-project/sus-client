import argparse
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import uuid
from dotenv import load_dotenv
import os


def generate_messages(num_xl, num_small, base_url, workers):
    with open('./Data/52.json', 'r') as f:
        big = json.load(f)
    with open('./Data/3.json', 'r') as f:
        small = json.load(f)

    with ThreadPoolExecutor(max_workers=workers) as executor:
       # submit each task to the thread pool
        for _ in range(num_xl):
            executor.submit(post, big, base_url)
        for _ in range(num_small):
            executor.submit(post, small, base_url)


def post(message, base_url):
    payload = {**message, 'uuid': str(uuid.uuid4())}
    r = requests.post(base_url, json=payload)
    if r.status_code != 201:
        print('Error: ', r.status_code, r.text)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-xl', '--num_xl', type=int, default=250,
        help='Number of extra-large messages to generate (default: 250)')
    parser.add_argument(
        '-sm', '--num_small', type=int, default=1000,
        help='Number of small messages to generate (default: 1000)')
    args = parser.parse_args()

    load_dotenv()  # this will load environment variables from the .env file
    base_url = os.getenv('BASE_URL')
    if base_url is None:
        exit('BASE_URL environment variable is not set')

    workers = os.getenv('GENERATE_WORKERS')
    if workers is None:
        exit('GENERATE_WORKERS environment variable is not set')
    workers = int(workers)

    generate_messages(args.num_xl, args.num_small, base_url, workers)
