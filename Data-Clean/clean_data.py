import pandas as pd

df = pd.read_csv('mapping_words.csv')
df.pop('Unnamed: 15') 
df.pop('Unnamed: 14')


def determine_split(file_name):
    if 'ADE_train' in file_name:
        return 'training'
    elif 'ADE_val' in file_name:
        return 'validation'
    else:
        return None

df['split'] = df['file_name'].apply(determine_split)

# Reorder columns to place 'split' right next to 'file_name'
cols = df.columns.tolist()
cols = cols[:1] + ['split'] + cols[1:-1]
df = df[cols]

print(df.head)  
df.to_csv('modified_data.csv', index=False)



def add_category_and_level(main_file, secondary_file, output_file):
    # Read datasets into pandas DataFrames
    df_main = pd.read_csv(main_file)
    df_secondary = pd.read_csv(secondary_file)

    # Create a dictionary from the secondary dataframe for faster lookup
    category_dict = df_secondary.set_index('filename')['category'].to_dict()
    level_dict = df_secondary.set_index('filename')['level'].to_dict()

    # Fetch 'category' and 'level' values based on 'file_name'
    df_main['category'] = df_main['file_name'].apply(lambda x: category_dict.get(x, None))
    df_main['level'] = df_main['file_name'].apply(lambda x: level_dict.get(x, None))

    # Save the modified DataFrame to the specified output file
    df_main.to_csv(output_file, index=False)

# Usage
add_category_and_level('modified_data.csv', 'bb_postprocessed.csv', 'final_data.csv')