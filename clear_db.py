import requests
from dotenv import load_dotenv
import os


def main():
    load_dotenv()  # this will load environment variables from the .env file
    base_url = os.getenv('BASE_URL')
    if base_url is None:
        exit('BASE_URL environment variable is not set')
    r = clear(base_url)
    print(r.status_code, r.text)


def clear(base_url: str):
    return requests.patch(base_url)


if __name__ == '__main__':
    main()
