#For Digital Authentication Tokens

import json
import os
from dotenv import load_dotenv

class Config:
    load_dotenv()
    def __init__(self):
        load_dotenv()
        self.USERNAME=os.getenv('K_USERNAME')
        print(self.USERNAME)

    def get_api_keys(self):
        with open('/Users/kiranchavadi/.secret/credentials.json', 'r') as f:
            credentials = json.load(f)

        username = credentials['username']
        password = credentials['password']
        return username, password

    #Variables shoudl be nouns
    #Function & Method shoudl be verbs


if __name__ == '__main__':
    config = Config()
    print(config.USERNAME)

    # username,password = config.get_api_keys()
    # print(username, password)