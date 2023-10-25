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
    re_mentions_count = len(re_mentions)
    df.at[index, 're_mentions_count'] = re_mentions_count
    is_re_mentioned = re_mentions_count > 1
    df.at[index, 'is_re_mentioned'] = is_re_mentioned

    if is_re_mentioned:
        first_remention = re_mentions[re_mentions['sentenceLevel'] == re_mentions['sentenceLevel'].min()]
        first_remention_sentence_level = first_remention.iloc[0]['sentenceLevel']
        df.at[index, 'first_remention_sentence_level'] = first_remention_sentence_level

        first_remention_word_index = first_remention[first_remention['word_index'] == first_remention['word_index'].min()].iloc[0]['word_index']
        df.at[index, 'first_remention_word_index'] = first_remention_word_index

    # JOINING WITH FIXDATA LOGIC
    fix_df.loc[fix_df['image_path'].str.contains(str(current_file_name)) & fix_df['objectLabel'].str.contains(str(row['label'])), ['re_mentions_count','is_re_mentioned','first_remention_sentence_level','first_remention_word_index']] = [
        re_mentions_count,
        is_re_mentioned,
        first_remention_sentence_level,
        first_remention_word_index
    ]

df.to_csv('with_word_count.csv', index=False)
fix_df.to_csv('fix_data_merged.csv', index=False)
