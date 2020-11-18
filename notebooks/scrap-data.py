# %% [markdown]
# # Scraping the 2020 elections data
# Author: Danilo Lessa Bernardineli (danilo.lessa [at] gmail.com)
# Notebook for scraping the elections data from all Brazil municipalities

# %%
# Dpeendences
import pandas as pd
import requests as req
from joblib import Parallel, delayed
from tqdm.auto import tqdm

# Template for the CDN URI
URI_TEMPLATE = "https://resultados.tse.jus.br/oficial/ele2020/divulgacao/"
URI_TEMPLATE += "oficial/426/dados-simplificados/"
URI_TEMPLATE += "{uf}/{uf}{town_id}-c{type_id}-e000426-r.json"

# 0011: Vereador / 0013: Mayor
TYPES = ['0011', '0013']

# JSON containing the TSE codes for all municipalities
# + correspondences with IBGE
# Thanks @betafcc! (https://github.com/betafcc/Municipios-Brasileiros-TSE)
TOWN_CODE_URI = 'https://raw.githubusercontent.com/betafcc/Municipios-Brasileiros-TSE/fe2694220eb7280a3df01e985641f34078989aa8/municipios_brasileiros_tse.json'


def download_data(town: dict, type_id: str) -> dict:
    """
    Get a concatenated dict representing the municipality data for a 
    given (town, type) combination.
    """
    # Format parameters for the URI_TEMPLATE
    params = {'uf': town['uf'].lower(),
              'town_id': str(town['codigo_tse']).zfill(5),
              'type_id': type_id}
    uri = URI_TEMPLATE.format(**params)

    # Send request
    r = req.get(uri)

    # Try to loda the response
    try:
        json_data = r.json()
    except:
        json_data = {}
        print(f"Error on {town['codigo_tse']}")

    # Concatenate the params, town info and election data into a single dict
    iter_data = {**params, **town, **json_data}
    return iter_data


# Retrieve a list of dicts that represents the town infos
r = req.get(TOWN_CODE_URI)
towns = r.json()

# Create a cartesian product of towns X possible election types
combinations = [(town, type_id) for town in towns for type_id in TYPES]

# Wrap the iterator in a progress bar
iterator = tqdm(combinations, desc='Download election data')

# Iterate on download_data through parallel processing
# On my PC for 10 jobs, It took about 10min.
results = Parallel(n_jobs=10, prefer='threads')(
    delayed(download_data)(town, type_id) for (town, type_id) in iterator)

# The 'cand' key on each results element is a list of dicts, so we have
# a nested object. The solution for having a neat dataframe is to
# expand it.
# %%
# Container object where each element will be a row on a DataFrame
output = []

# Iterate on each election result, and on each element on the 'cand' list
for result in tqdm(results, desc='Cleaning results'):
    for candidate_data in result.get('cand', {}):
        # Create a dict containing the original election result data
        # plus the kvs inside each candidate
        row_data = {**result, **candidate_data}

        # Remove the nested object from the new dict
        row_data.pop('cand')

        # Append the cleaned dict to the output list
        output.append(row_data)

# %%
# Now our data is ready to be consumed neatly by pandas.
# We create a DataFrame and then export it to a csv on the data folder.
df = pd.DataFrame(output)
df.to_csv('../data/raw_data.csv.gz', compression='gzip')