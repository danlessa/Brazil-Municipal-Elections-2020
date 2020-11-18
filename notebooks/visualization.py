# %% [markdown]
# # Visualizing the 2020 elections data
# Author: Danilo Lessa Bernardineli (danilo.lessa [at] gmail.com)
# Notebook for visualizing the elections data from all Brazil municipalities

# %%
import pandas as pd
import plotly.express as px
pd.options.plotting.backend = "plotly"


DATA_PATH = '../data/clean_data.csv.gz'

df = pd.read_csv(DATA_PATH, compression='gzip')
# %%
y_1 = (df.query('job == "prefeito"')
       .groupby('main_party')
       .candidate_vote_count
       .sum()
       .sort_values())
y_1.name = 'mayor_votes_per_party'
y_1.plot.bar()

# %%
y_2 = (df.query('job == "vereador"')
       .groupby('main_party')
       .candidate_vote_count
       .sum()
       .sort_values())
y_2.name = 'councilor_votes_per_party'
y_2.plot.bar()
# %%
fig_df = pd.DataFrame([y_1, y_2]).T
px.scatter(fig_df,
           x='mayor_votes_per_party',
           y='councilor_votes_per_party')
