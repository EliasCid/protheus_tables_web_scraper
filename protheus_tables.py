#%%
import os
import glob
import pandas as pd
from datetime import datetime

# Function to identify latest files on folder
def get_latest_file(file_list, keyword):
    filtered = [f for f in file_list if keyword in os.path.basename(f)]
    if filtered:
        return max(filtered, key=os.path.getctime)
    return None

# Function to load latest file from folder
def load_latest_file(file):
    main_folder = os.path.dirname(os.getcwd())
    protheus_tables_folder = os.path.join(main_folder, 'protheus_web_scraper')
    protheus_tables_files = glob.glob(os.path.join(protheus_tables_folder, "**", "*.parquet"))
    df_path = get_latest_file(protheus_tables_files, file)
    file = pd.read_parquet(df_path)
    return file

# Function to save files in csv
def save_dataframes(dataframe, name, desired_tables, timestamp):
    filtered_dataframe = dataframe[dataframe['Tabela'].isin(desired_tables)]
    filename = f'{name}_{timestamp}.csv'
    filtered_dataframe.to_csv(filename, index=False, encoding='utf-8')
    return filename

# Load data
files = ['df_all_fields', 'df_all_index', 'df_all_relationships', 'df_tables']
loaded_dfs = {}

for file in files:
    loaded_dfs[file] = load_latest_file(file)
    
df_fields = loaded_dfs['df_all_fields']
df_index = loaded_dfs['df_all_index']
df_relationships = loaded_dfs['df_all_relationships']
df_tables = loaded_dfs['df_tables']

# Define desired tables
desired_tables = [
    'CG1',
    'CT2',
    'CTT',
    'SA1',
    'SA2',
    'SA3',
    'SB1',
    'SB5',
    'SB8',
    'SC5',
    'SC6',
    'SC7',
    'SC9',
    'SD1',
    'SD2',
    'SE4',
    'SF2',
    'SF4',
    'SZ9',
    'SZK',
    'SZT',
    'SZU',
    'ZA9',
    'ZP1',
    'ZP8'
]

# Save files
dataframes = [df_fields, df_index, df_relationships]
names = ["df_fields", "df_index", "df_relationships"]
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

for df, name in zip(dataframes, names):
    save_dataframes(df, name, desired_tables, timestamp)

filtered_df_tables = df_tables[df_tables['Table'].isin(desired_tables)]    
filtered_df_tables.to_csv(f'df_tables_{timestamp}.csv', index=False, encoding='utf-8')
# %%
