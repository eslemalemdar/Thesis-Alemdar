import pandas as pd
import spacy

output_df = pd.DataFrame(columns=['file_name', 'sentence_no','word_index', 'text', 'base_form', 'part_of_speech_simple', 'part_of_speech_detailed', 'syntatic_dependency','shape','is_alpha','is_stop'])

nlp = spacy.load("en_core_web_sm")

def process_answer(number: int):
    result = nlp(df.loc[i, f"answer_{number}"])

    for token in enumerate(result):
        index = token[0]
        value = token[1]

        output_df.loc[len(output_df)] = [
            df.loc[i, "file_name"],
            number,
            index,
            value.text,
            value.lemma_,
            value.pos_,
            value.tag_,
            value.dep_,
            value.shape_,
            value.is_alpha,
            value.is_stop,
        ]

df = pd.read_csv("output.csv")
for i in range(len(df)):
    for num in range(5):
        process_answer(num + 1)


output_df.to_csv('tagged_output.csv', index=False)
