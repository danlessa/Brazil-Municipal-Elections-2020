# %% [markdown]
# # Cleaning the 2020 elections data
# Author: Danilo Lessa Bernardineli (danilo.lessa [at] gmail.com)
# Notebook for cleaning the elections data from all Brazil municipalities

# %%
import pandas as pd

# %%
DATA_PATH = '../data/raw_data.csv.gz'

DROP_COLS = ['Unnamed: 0',
             'sqcand',
             'town_id',
             'ele',
             'tpabr',
             'cdabr',
             'carper',
             't',
             'f',
             'dt',
             'ht',
             'dv',
             'tf',
             'esae',
             'mnae',
             's',
             'st',
             'pst',
             'psnt',
             'si',
             'psi',
             'sni',
             'psni',
             'sa',
             'psa',
             'sna',
             'psna',
             'ea',
             'pea',
             'ena',
             'pena',
             'pesi',
             'esni',
             'pesni',
             'pc',
             'pa',
             'vvc',
             'pvvc',
             'vscv',
             'pvb',
             'ptvn',
             'pvnt',
             'pvp',
             'pvv',
             'pvl',
             'pvan',
             'pvansj',
             'pvnom',
             'seq',
             'pvap',
             'snt',
             'esi',
             'pvn',
             'vp'
             ]

RENAME_COLS = {
    'v': 'job_count',
    'e': 'elector_count',
    'c': 'elector_presence',
    'a': 'absentees',
    'vl': 'legend_votes',
    'vb': 'blank_votes',
    'vnom': 'nominal_votes',
    'tvn': 'total_null_votes',
    'vnt': 'technical_null_votes',
    'vn': 'null_votes',
    'tv': 'vote_count',
    'vv': 'valid_votes',
    'van': 'nulled_votes',
    'vansj': 'judically_nulled_votes',
    'cc': 'candidate_coligation',
    'nv': 'candidate_vice_name',
    'dvt': 'candidate_vote_destination',
    'nm': 'candidate_name',
    'vap': 'candidate_vote_count',
    'n': 'candidate_number'
}

TYPE_MAP = {13: 'vereador', 11: 'prefeito'}


def get_main_party(df) -> str:
    return df.candidate_coligation.str.split(" ").map(lambda x: x[0])


df = (pd.read_csv(DATA_PATH, compression='gzip')
        .drop(columns=DROP_COLS)
        .rename(columns=RENAME_COLS)
        .assign(job=lambda df: df.type_id.map(TYPE_MAP))
        .assign(main_party=lambda df: get_main_party(df))
      )

df.to_csv('../data/clean_data.csv.gz', compression='gzip')

# %%
