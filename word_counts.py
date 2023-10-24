import pandas as pd
df = pd.read_csv("final_data.csv", header=0)

fix_df = pd.read_csv("fixdata_table.csv", header=0)

count=0
current_file_name= None

for index, row in df.iterrows():
    # COUNT LOGIC
    if current_file_name is None:
        current_file_name = row["file_name"]

    if current_file_name == row["file_name"]:
        if row["part_of_speech_simple"] != 'PUNCT':
            count = count + 1
    else:
        df.loc[df["file_name"] == current_file_name, "total_word_count"] = count
        count = 0
        current_file_name = None

    # RE-MENTIONED LOGIC
    current_singular_label = row['singLabel']
    re_mentions = df[(df['file_name'] == current_file_name) & (df['singLabel'] == current_singular_label)]
    number_of_rementions = len(re_mentions)
    df.at[index, 're_mentions_count'] = number_of_rementions
    is_re_mentioned = number_of_rementions > 1
    df.at[index, 'is_re_mentioned'] = is_re_mentioned

    if is_re_mentioned:
        first_remention_sentence_level = re_mentions[re_mentions['sentenceLevel'] == re_mentions['sentenceLevel'].min()]
        df.at[index, 'first_remention_sentence_level'] = first_remention_sentence_level.iloc[0]['sentenceLevel']

        first_remention_word_index = first_remention_sentence_level[first_remention_sentence_level['word_index'] == first_remention_sentence_level['word_index'].min()]
        df.at[index, 'first_remention_word_index'] = first_remention_word_index.iloc[0]['word_index']

    # JOINING WITH FIXDATA LOGIC
    corresponding_fixdata = fix_df[current_file_name in fix_df['image_path'] & fix_df['objectLabel'] == row['label']]

    # ?? There could be multiple matching fixdata rows, which one should we pick

    
print(df.head())

df.to_csv('with_word_count.csv', index=False)
