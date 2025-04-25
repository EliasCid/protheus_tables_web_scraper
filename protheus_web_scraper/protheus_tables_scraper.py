#%%
import os
import math
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# Function to get site content
def get_content(url):
    headers = {
        'sec-ch-ua-platform': '"Windows"',
        'Referer': 'https://terminaldeinformacao.com/wp-content/tabelas/a00.php',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'sec-ch-ua': '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        'sec-ch-ua-mobile': '?0',
    }
    
    response = requests.get(url, headers=headers)
    return response
# Function to get all available tables on the site
def get_tables(soup):
    li_items = soup.select('#tabsUL li')

    tables = {}
    for li in li_items:
        parts = li.text.split('-', 1)  # split only on the first hyphen
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            tables[key] = value
    
    return tables

# Function to get all links to review
def get_links():
    url1 = 'https://terminaldeinformacao.com/wp-content/tabelas/a00.php'

    # Gather information from site
    response = get_content(url1)

    # First identify all tables available on the site
    soup = BeautifulSoup(response.text, features='html.parser')
    tables = get_tables(soup)
    
    # Turn into df
    df_tables = pd.DataFrame(tables.items(), columns=['Table', 'Name'])
    
    #Get links
    url2 = 'https://terminaldeinformacao.com/wp-content/tabelas/'
    tables_ids = [key.lower() for key in tables.keys()]
    links = [f'{url2}{id}.php' for id in tables_ids]
    return links, tables, df_tables
            
# Function to extract tables from site
def extract_tables(table):
    # Extract headers
    th = table.find_all('th')[0]
    headers = th.text.split('\n')[:-1]

    # Extract rows
    rows = []
    for tr in table.find_all('tr')[1:]:
        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
        if cells:
            # Fill the row to match header length
            if len(cells) < len(headers):
                cells += [None] * (len(headers) - len(cells))
            rows.append(cells)

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=headers)

    return df

# General function to extract information from site:
def get_protheus_tables(url):
    try:
        response = get_content(url)
        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, features='html.parser')
        table_name = soup.find('h1').text.split(' ')[1]

        #Fields data
        df_fields = extract_tables(soup.find_all('table')[0])
        df_fields['Tabela'] = table_name

        #Index data
        df_index = extract_tables(soup.find_all('table')[1])
        df_index['Tabela'] = table_name

        #Relationship data
        df_relationships = extract_tables(soup.find_all('table')[2])
        df_relationships['Tabela'] = table_name

        return df_fields, df_index, df_relationships
    
    except Exception as e:
        print(f'Error processing {url}: {e}')
        return None

# Scrap Protheus tables
links, tables, df_tables = get_links()

# Create folders to store intermediate results
os.makedirs('batches/fields', exist_ok=True)
os.makedirs('batches/index', exist_ok=True)
os.makedirs('batches/relationships', exist_ok=True)

# Timestamp for filenames
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# Save df_tables
df_tables.to_parquet(f'df_tables_{timestamp}.parquet', index=False)

# Number of batches
n_batches = 100
batch_size = math.ceil(len(links) / n_batches)

for batch_num in range(n_batches):
    print(f'Processing batch {batch_num+1}/{n_batches}')
    
    batch_links = links[batch_num * batch_size:(batch_num + 1) * batch_size]

    batch_fields = []
    batch_index = []
    batch_relationships = []

    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(get_protheus_tables, url): url for url in batch_links}
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f'Batch {batch_num+1}'):
            result = future.result()
            if result:
                df_fields, df_index, df_relationships = result
                batch_fields.append(df_fields)
                batch_index.append(df_index)
                batch_relationships.append(df_relationships)

    # Save each batch to separate files with timestamp
    if batch_fields:
        pd.concat(batch_fields).to_parquet(f'batches/fields/fields_batch_{batch_num}_{timestamp}.parquet', index=False)
    if batch_index:
        pd.concat(batch_index).to_parquet(f'batches/index/index_batch_{batch_num}_{timestamp}.parquet', index=False)
    if batch_relationships:
        pd.concat(batch_relationships).to_parquet(f'batches/relationships/relationships_batch_{batch_num}_{timestamp}.parquet', index=False)

# Final step: concatenate all batch results into final files
def concatenate_parquet(folder_path):
    files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.endswith('.parquet')]
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

df_all_fields = concatenate_parquet('batches/fields')
df_all_index = concatenate_parquet('batches/index')
df_all_relationships = concatenate_parquet('batches/relationships')

# Save final merged files with timestamp
df_all_fields.to_parquet(f'df_all_fields_{timestamp}.parquet', index=False)
df_all_index.to_parquet(f'df_all_index_{timestamp}.parquet', index=False)
df_all_relationships.to_parquet(f'df_all_relationships_{timestamp}.parquet', index=False)

# %%