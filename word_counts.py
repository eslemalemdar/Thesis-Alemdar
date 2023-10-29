import pandas as pd
from itertools import repeat
from multiprocessing import cpu_count
from multiprocessing import Pool
import tqdm

current_file_name=None

def process(input, df, fix_df):
    index = input[0]
    row = input[1]

    count = len(df[(df["file_name"] == row['file_name']) & (df['part_of_speech_simple'] != 'PUNCT')])
    df.at[index, 'total_word_count'] = count

    # RE-MENTIONED LOGIC
    mentions = df[(df['file_name'] == row['file_name']) & (df['singLabel'] == row['singLabel']) & (str(df['bb']) != 'nan')]

    mentions_count = len(mentions)
    df.at[index, 're_mentions_count'] = mentions_count
    is_re_mentioned = mentions_count > 1
    df.at[index, 'is_re_mentioned'] = is_re_mentioned

    if len(mentions) > 0:
        first_mention = mentions[mentions['sentenceLevel'] == mentions['sentenceLevel'].min()]
        first_mention_sentence_level = first_mention.iloc[0]['sentenceLevel']
        df.at[index, 'first_remention_sentence_level'] = first_mention_sentence_level
    
        first_mention_word_index = first_mention[first_mention['word_index'] == first_mention['word_index'].min()].iloc[0]['word_index']
    
        df.at[index, 'first_remention_word_index'] = first_mention_word_index

        # JOINING WITH FIXDATA LOGIC
        fix_df.loc[fix_df['image_path'].str.contains(str(row['file_name'])) & fix_df['objectLabel'].str.contains(str(row['label'])), [
            'mentions_count',
            'is_re_mentioned',
            'first_mention_sentence_level',
            'first_mention_word_index'
        ]] = [
            mentions_count,
            is_re_mentioned,
            first_mention_sentence_level,
            first_mention_word_index
        ]

def main():
    df = pd.read_csv("final_data.csv", header=0)
    fix_df = pd.read_csv("fixdata_table.csv", header=0)

    pool = Pool(16)
    pool.starmap(process, tqdm.tqdm(zip(df.iterrows(), repeat(df), repeat(fix_df)), total=len(df)))

    df.to_csv('with_word_count.csv', index=False)
    fix_df.to_csv('fix_data_merged.csv', index=False)

if __name__ == '__main__':
    main()

#word index -- should start from 1 not 0
