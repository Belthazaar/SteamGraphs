import streamlit as st
import pandas as pd
import datetime
from pymongo import MongoClient
import plotly.express as px


cm_cache_detail = [
    {'region': 'South_America', 'code': 'ARG', 'cm': 'eze1', 'cache': '',       'cell_id': 116, 'city': 'Buenos Aires'},
    {'region': 'Oceania',       'code': 'AUS', 'cm': 'syd1', 'cache': 'syd1',   'cell_id':  52, 'city': 'Sydney'},
    {'region': 'Europe',        'code': 'AUT', 'cm': 'vie1', 'cache': 'vie1',   'cell_id':  92, 'city': 'Vienna'},
    {'region': 'South_America', 'code': 'BRA', 'cm': 'gru1', 'cache': 'gru1',   'cell_id':  25, 'city': 'São Paulo'},
    {'region': 'South_America', 'code': 'CHL', 'cm': 'scl1', 'cache': '',       'cell_id': 117, 'city': 'Santiago'},
    {'region': 'Europe',        'code': 'FRA', 'cm': 'par1', 'cache': 'par1',   'cell_id':  14, 'city': 'Paris'},
    {'region': 'Europe',        'code': 'DEU', 'cm': 'fra1', 'cache': 'fra1',   'cell_id':   5, 'city': 'Frankfurt'},
    {'region': 'Europe',        'code': 'DEU', 'cm': 'fra2', 'cache': 'fra2',   'cell_id':   5, 'city': 'Frankfurt'},
    {'region': 'Asia',          'code': 'HKG', 'cm': 'hkg1', 'cache': 'hkg1',   'cell_id':  33, 'city': 'Hong Kong'},
    {'region': 'Asia',          'code': 'JPN', 'cm': 'tyo1', 'cache': 'tyo1',   'cell_id':  32, 'city': 'Tokyo'},
    {'region': 'Asia',          'code': 'JPN', 'cm': 'tyo2', 'cache': 'tyo2',   'cell_id':  32, 'city': 'Tokyo'},
    {'region': 'Europe',        'code': 'NLD', 'cm': 'ams1', 'cache': 'ams1',   'cell_id':  15, 'city': 'Amsterdam'},
    {'region': 'South_America', 'code': 'PER', 'cm': 'lim1', 'cache': '',       'cell_id': 118, 'city': 'Lima'},
    {'region': 'Europe',        'code': 'POL', 'cm': 'waw1', 'cache': 'waw1',   'cell_id':  38, 'city': 'Warsaw'},
    {'region': 'Asia',          'code': 'SGP', 'cm': 'sgp1', 'cache': 'sgp1',   'cell_id':  35, 'city': 'Singapore'},
    {'region': 'Asia',          'code': 'SGP', 'cm': 'sgp1', 'cache': 'reuse',  'cell_id':  35, 'city': 'Singapore'},
    {'region': 'Africa',        'code': 'ZAF', 'cm': 'jnb1', 'cache': 'jnb1',   'cell_id':  26, 'city': 'Johannesburg'},
    {'region': 'Asia',          'code': 'KOR', 'cm': 'seo1', 'cache': '',       'cell_id':   8, 'city': 'Seoul'},
    {'region': 'Europe',        'code': 'ESP', 'cm': 'mad1', 'cache': 'mad1',   'cell_id':  40, 'city': 'Madrid'},
    {'region': 'Europe',        'code': 'SWE', 'cm': 'sto1', 'cache': 'sto1',   'cell_id':  66, 'city': 'Stockholm'},
    {'region': 'Europe',        'code': 'SWE', 'cm': 'sto2', 'cache': 'sto2',   'cell_id':  66, 'city': 'Stockholm'},
    {'region': 'Europe',        'code': 'GBR', 'cm': 'lhr1', 'cache': 'lhr1',   'cell_id':   4, 'city': 'London'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'atl1', 'cache': 'atl1',   'cell_id':  50, 'city': 'Atlanta'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'atl2', 'cache': 'atl2',   'cell_id':  50, 'city': 'Atlanta'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'atl3', 'cache': 'atl3',   'cell_id':  50, 'city': 'Atlanta'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'atl4', 'cache': 'atl4',   'cell_id':  50, 'city': 'Atlanta'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'dfw1', 'cache': 'dfw1',   'cell_id':  65, 'city': 'Dallas/Fort Worth'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'dw1',  'cache': 'dw1',    'cell_id':  65, 'city': 'Dallas/Fort Worth'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'iad1', 'cache': 'iad1',   'cell_id':  63, 'city': 'Ashburn'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'lax1', 'cache': 'lax1',   'cell_id':  64, 'city': 'Los Angeles'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'ord1', 'cache': 'ord1',   'cell_id':   1, 'city': 'Chicago'},
    {'region': 'North_America', 'code': 'USA', 'cm': 'sea1', 'cache': 'sea1',   'cell_id':  31, 'city': 'Seattle'}
]

cell_id_to_city = {x['cell_id']: x['city'] for x in cm_cache_detail}

region_to_city = {x['region']: set() for x in cm_cache_detail}
for x in cm_cache_detail:
    if x['cache'] != '':
        region_to_city[x['region']].add(x['city'])

region_to_city = {k: list(v) for k, v in region_to_city.items()}

@st.cache_resource
def init_connection():
    
    return MongoClient(**st.secrets["mongo"])

client = init_connection()

@st.cache_data(ttl=1800)
def get_data():
    db = client.steam
    items = list(db.global_bandwidth.find({},{'_id': 0}))
    return items

@st.cache_data(ttl=3000)
def cache_city_heatmap(df):

    mask = df.columns.values != df.index.to_numpy()[:, None]  
    max_value = df.where(mask).max().max() * 1.2
    fig = px.imshow(df, labels=dict(x="Query Origin", 
                                        y="Cache City",
                                        color="Cache count"), 
                            text_auto=True, 
                            height=600,
                            aspect='auto',
                            color_continuous_scale='Hot_r',
                            zmax=max_value)    

    fig.update_coloraxes(showscale=False)
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=3000)
def cache_city_query(start, end):
    db = client.steam
    start = datetime.datetime.combine(start, datetime.time())
    end = datetime.datetime.combine(end, datetime.time()) + datetime.timedelta(days=1)

    items = list(db.cache.find({'timestamp': {"$gte": start, "$lte": end}},{'_id': 0, 'city': 1, 'timestamp': 1, 'query_id': 1}))
    df = pd.DataFrame(items)
    df = df.groupby(['timestamp', 'query_id']).head(5)
    df = df.groupby(['query_id', 'city']).size().unstack().fillna(0)
    df.index = df.index.map(lambda x: cell_id_to_city[x])
    df.sort_index(inplace=True)
    region_dfs = {k: df[v] for k, v in region_to_city.items()}
    for r in region_dfs:
        region_dfs[r] = region_dfs[r].loc[(region_dfs[r] != 0).any(axis=1)]
        region_dfs[r] = region_dfs[r].T
        region_dfs[r] = region_dfs[r].reindex(index=region_dfs[r].index.union(region_dfs[r].columns), columns=region_dfs[r].index.union(region_dfs[r].columns), fill_value=0)
        region_dfs[r] = region_dfs[r].sort_index(axis=1)
        region_dfs[r] = region_dfs[r].sort_index(axis=0)
    return region_dfs


all_df = pd.DataFrame(get_data())
all_df.set_index('timestamp', inplace=True)
all_df.sort_index(inplace=True)

st.header("Cache Load Heatmap")
col1, col2, col3 = st.columns(3)
with col1:
    region = st.selectbox('Select region', set(x['region'].replace('_', ' ') for x in cm_cache_detail if x['cache'] ), key="heatmap_region", index=None)
with col2:
    start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(days=7), min_value=datetime.datetime(2023, 8, 8), max_value=all_df.index[-1].to_pydatetime(), key="heatmap_start")
with col3:
    end = st.date_input('End data', value=all_df.index[-1], min_value=datetime.datetime(2023, 8, 8), max_value=all_df.index[-1].to_pydatetime(), key="heatmap_end")
cache_query = cache_city_query(start, end)
if region:
    cache_city_heatmap(cache_query[region.replace(' ', '_')])

st.toggle("Show all regions", key="heatmap_show_all")
if st.session_state.heatmap_show_all:
    col1, col2 = st.columns(2)
    with col1:
        st.header('North America')
        cache_city_heatmap(cache_query['North_America'])
    with col2:
        st.header('Europe')
        cache_city_heatmap(cache_query['Europe'])
    col3, col4 = st.columns(2)
    with col3:
        st.header('Asia')
        cache_city_heatmap(cache_query['Asia'])
    with col4:
        st.header('South America')
        cache_city_heatmap(cache_query['South_America'])
    col5, col6 = st.columns(2)
    with col5:
        st.header('Oceania')
        cache_city_heatmap(cache_query['Oceania'])
    with col6:
        st.header('Africa')
        cache_city_heatmap(cache_query['Africa'])
