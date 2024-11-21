import pandas as pd
from itertools import repeat
from multiprocessing import cpu_count
from multiprocessing import Pool
import tqdm

def process(input, df):
    index = input[0]
    row = input[1]

    count = len(df[(df["file_name"] == row['file_name']) & (df['part_of_speech_simple'] != 'PUNCT')])

    # RE-MENTIONED LOGIC
    mentions = df[(df['file_name'] == row['file_name']) & (df['singLabel'] == row['singLabel']) & (str(df['bb']) != 'nan')]

    mentions_count = len(mentions)
    is_re_mentioned = mentions_count > 1

    if len(mentions) > 0:
        first_mention = mentions[mentions['sentenceLevel'] == mentions['sentenceLevel'].min()]
        first_mention_sentence_level = first_mention.iloc[0]['sentenceLevel']
    
        first_mention_word_index = first_mention[first_mention['word_index'] == first_mention['word_index'].min()].iloc[0]['word_index']
    
    return [
        index,
        [
            count,
            mentions_count,
            is_re_mentioned,
            first_mention_sentence_level,
            first_mention_word_index,
        ]
    ]

def main():
    df = pd.read_csv("final_data.csv", header=0)
    fix_df = pd.read_csv("fixdata_table.csv", header=0)

    pool = Pool(24)
    results = pool.starmap(process, tqdm.tqdm(zip(df.iterrows(), repeat(df)), total=len(df)))

    final_data_results = [item[1] for item in results]
    counts = [item[0] for item in final_data_results]
    mentions_counts = [item[1] for item in final_data_results]
    is_re_mentioneds = [item[2] for item in final_data_results]
    first_mention_sentence_levels = [item[3] for item in final_data_results]
    first_mention_word_indexes = [item[4] for item in final_data_results]

    df['count'] = counts
    df['mentions_count'] = mentions_counts
    df['is_re_mentioned'] = is_re_mentioneds
    df['first_mention_sentence_level'] = first_mention_sentence_levels
    df['first_mention_word_indexes'] = first_mention_word_indexes

    fix_df_with_filenames = fix_df;
    fix_df_with_filenames['file_name'] = fix_df_with_filenames['image_path'].str.split('/').str[-1].str.join('')

    deduplicated_df = df.drop_duplicates(subset=['file_name', 'label'])
    print(len(deduplicated_df))

    deduplicated_df = deduplicated_df.dropna(subset=['bb'])
    print(deduplicated_df[['bb']])

    fix_df_merged = pd.merge(
        fix_df_with_filenames,
        deduplicated_df[['mentions_count', 'is_re_mentioned', 'first_mention_sentence_level', 'first_mention_word_indexes', 'file_name', 'label']],
        how='left',
        left_on=['file_name', 'objectLabel'],
        right_on=['file_name', 'label']
    )

    df.to_csv('with_word_count.csv', index=False)
    fix_df_merged.to_csv('fix_data_merged.csv', index=False)

if __name__ == '__main__':
    main()
