from bardapi import Bard
import os
from os import listdir
import pandas as pd
import re
import json5
import concurrent.futures
import sys

token = 'aAjyjlJG7ez7eovHRcHlNEy4n2ejSil7tISvG7QqU1_Gf7vmG-ekYD0zSdGiftgqJoew5Q.'
bard = Bard(token=token)

folder_dir = "input"

if os.path.exists("output.csv"):
    df = pd.read_csv("output.csv")
else:
    df = pd.DataFrame(columns=['file_name', 'answer_1', 'answer_2', 'answer_3', 'answer_4', 'answer_5'])

error_data_frame = pd.DataFrame(columns=['file_name', 'exception', 'step', 'bard_response'])

def process_image(file_name):
    if file_name in df.values:
        print(f"skipping the {file_name}")
        return

    image = open(f"input/{file_name}", 'rb').read()
    try:
        response = bard.ask_about_image("Your role is to describe this image to someone who hasn't seen the image in the form of 5 sentences. Each subsequent sentence should answer the question \"Tell me more about the image\". Write the sentences in the form of a JSON array containing only strings. Always write valid JSON without dangling commas. Don't repeat yourself, try to always generate unique sentences and don't make stuff up that doesn't exist in the image.", image)
    except Exception as e:
        print('Error with Bard')
        print(e)

        error_data_frame.loc[len(error_data_frame)] = [file_name, str(e), 'bard', '']
        error_data_frame.to_csv('errors.csv')
        sys.exit()

    try:
        unescaped = response['content'].encode("utf-8").decode('unicode-escape')
        print(f"received response for {file_name}: {unescaped}")
        codeblock = re.search(r"`{3}([\w]*)\n([\S\s]+?)\n`{3}", unescaped).group(0)
        jsonText = codeblock.replace('`json', '').replace('`', '')
        responses_parsed = json5.loads(jsonText)

        result = [file_name] + responses_parsed

        df.loc[len(df)] = result
        df.to_csv('output.csv', index=False)
    except Exception as e:
        print(f'Error while parsing response for image {file_name}:')
        print(e)

        error_data_frame.loc[len(error_data_frame)] = [file_name, str(e), 'parse', response]
        error_data_frame.to_csv('errors.csv')
        return
    
image_files = [file_name for file_name in os.listdir(folder_dir) if file_name.endswith(".jpg")]

with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
    executor.map(process_image, image_files)
