import glob
from dotenv import load_dotenv
import os
import requests
from clear_db import clear
import app


def wait(prompt="Press any key to continue..."):
    # Check if the operating system is Windows
    if os.name == 'nt':
        # For Windows, use the msvcrt module to get a keypress
        import msvcrt
        print(prompt)
        msvcrt.getch()
    else:
        # For Unix-based systems, use a simple terminal command
        prompt += '\n'
        os.system(f"read -n 1 -s -r -p '{prompt}'")


ids = {
    0: "00000000-0000-0000-0000-000000000000",
    1: "11111111-1111-1111-1111-111111111111",
    2: "22222222-2222-2222-2222-222222222222",
    3: '33333333-3333-3333-3333-333333333333',
    4: '44444444-4444-4444-4444-444444444444',
}

flow_1 = [
    # create new messages
    ({
        "uuid": ids[4],
        "author": "author 4",
        "message": "message 4",
        "likes": 4,
        "imageUpdate": True,
        "image": 'image4'
    }, 'post', 201),
    ({
        "uuid": ids[1],
        "author": "author 1",
        "message": "message 1",
        "likes": 1,
        "imageUpdate": True,
        "image": 'image1'
    }, 'post', 201),
    ({
        "uuid": ids[2],
        "author": "author 2",
        "message": "message 2",
        "likes": 2,
        "imageUpdate": True,
        "image": None
    }, 'post', 201),
    ({
        "uuid": ids[0],
        "author": "author 0",
        "message": "message 0",
        "likes": 0,
        "imageUpdate": True,
        "image": 'image 0'
    }, 'post', 201),

    # not found update
    ({
        "uuid": 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx',
        "author": "author 0",
        "message": "message 0",
        "likes": 0,
        "imageUpdate": True,
        "image": 'image 0'
    }, 'put', 404),

    # not found delete
    ({"uuid": 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'}, 'delete', 404),

    # conflict post (same uuid)
    ({
        "uuid": ids[2],
        "author": "author2",
        "message": "message 2",
        "likes": 2,
        "imageUpdate": True,
        "image": None
    }, 'post', 409),

    # update message 0 with a new message
    ({
        "uuid": ids[0],
        "author": "author 0",
        "message": "new message 0",
        "likes": 0,
        "imageUpdate": False,
        "image": 'image 0'
    }, 'put', 204),

    # remove the image 1, add a message
    ({
        "uuid": ids[1],
        "message": "new message 1",
        "imageUpdate": True,
        "image": None,
        "author": "author 1",
        "likes": 1,
    }, 'put', 204),

    # delete the third message
    ({"uuid": ids[2]}, 'delete', 204),
]
checkpoint_1 = \
    """00000000-0000-0000-0000-000000000000,author 0,new message 0,0,image 0
11111111-1111-1111-1111-111111111111,author 1,new message 1,1,
44444444-4444-4444-4444-444444444444,author 4,message 4,4,image4"""

flow_2 = [
    # add new message
    ({
        "uuid": ids[3],
        "author": "author 3",
        "message": "message 3",
        "likes": 3,
        "imageUpdate": True,
        "image": None
    }, 'post', 201),

    # update the image of message 0
    ({
        "uuid": ids[0],
        "imageUpdate": True,
        "image": 'new image 0',
        "author": "author 0",
        "message": "new message 0",
        "likes": 0,
    }, 'put', 204),

    # update the message of message 0
    ({
        "uuid": ids[0],
        "imageUpdate": False,
        "image": 'new image 0',
        "author": "author 0",
        "message": "new new message 0",
        "likes": 0,
    }, 'put', 204),

    # update message 4's message
    ({
        "uuid": ids[4],
        "author": "author 4",
        "message": "new message 4",
        "likes": 4,
        "imageUpdate": False,
        "image": 'image4'
    }, 'put', 204),

    # delete message 1
    ({"uuid": ids[1]}, 'delete', 204),
]
checkpoint_2 = \
    """00000000-0000-0000-0000-000000000000,author 0,new new message 0,0,new image 0
33333333-3333-3333-3333-333333333333,author 3,message 3,3,
44444444-4444-4444-4444-444444444444,author 4,new message 4,4,image4"""


def main():
    load_dotenv()  # this will load environment variables from the .env file
    base_url = os.getenv('BASE_URL')
    if base_url is None:
        exit('BASE_URL environment variable is not set')

    # clear db before testing
    print('Clearing database... ', end='', flush=True)
    clear(base_url)
    print('Done')

    # remove all files from previous runs
    print('Removing files from previous runs... ', end='', flush=True)
    for file in glob.glob('*.csv'):
        os.remove(file)
    for file in glob.glob('cached_mutations_*'):
        os.remove(file)
    print('Done')

    tests(base_url, [flow_1, flow_2])


def tests(base_url, flows):
    for i, flow in enumerate(flows, 1):
        send_flow(base_url, flow)
        app.sync()
        if i != len(flows):
            wait(
                f'Checkpoint {i} reached, check result, press any key to continue...')
        else:
            print('Finished testing')


def send_flow(base_url, flow):
    for data, op, expected_status_code in flow:
        if op == 'post':
            r = requests.post(base_url, json=data)
            if r.status_code != expected_status_code:
                print(
                    f'Expected {expected_status_code}, got {r.status_code}. \
{op=}\n{data}\n{r.text}')
        elif op == 'put':
            r = requests.put(f'{base_url}/{data["uuid"]}', json=data)
            if r.status_code != expected_status_code:
                print(
                    f'Expected {expected_status_code}, got {r.status_code}. \
{op=}\n{data}\n{r.text}')
        elif op == 'delete':
            r = requests.delete(f'{base_url}/{data["uuid"]}')
            if r.status_code != expected_status_code:
                print(
                    f'Expected {expected_status_code}, got {r.status_code}. \
{op=}\n{data}\n{r.text}')
        else:
            print(f'Unknown operation: {op}')


if __name__ == '__main__':
    main()