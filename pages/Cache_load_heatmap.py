import streamlit as st
import pandas as pd
import datetime
from pymongo import MongoClient
import plotly.express as px
import json

st.set_page_config(page_title="Steam cache load heatmap", page_icon=":video_game:", layout="wide")


cell_id_to_region = {}
with open('CellMap.json', 'r', encoding='utf-8') as file:
    cid_to_region = json.load(file)
    cell_id_to_region = {int(k): v for k, v in cid_to_region.items()}

cm_cache_detail = [{
        'cell_id': int(k),
        'cm': v['cm'],
        'cache': v['cache'],
        'code': v['code'],
        'region': v['region'],
        'city': v['city']
    } for k, v in cell_id_to_region.items()]

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
    if len(items) == 0:
        st.warning('There are no cache details for this timeframe.\nThis is most likely due data missing during collection.')
        st.stop()
    df = pd.DataFrame(items)
    df = df.groupby(['timestamp', 'query_id']).head(5)
    df = df.groupby(['query_id', 'city']).size().unstack().fillna(0)
    df.index = df.index.map(lambda x: cell_id_to_region[x]['city'])
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

st.header("Load distribution of cache queries between cities")
txt = ("This page examines how the distribution of caches varies based on the Steam client's query location, aiming to "
       "determine Steam's load balancing practices across cities within the same region.\n\n"
       "The X-axis of the heatmap displays the cities that query the Steam client, while the Y-axis shows the locations "
       "of the caches. By analyzing the color intensity of the heatmap, you can determine the frequency of caches "
       "appearing in the top 5 entries, based on the query's origin location. We organize the data by region and you "
       "can filter it by date for detailed analysis.\n\n"
       "During periods without significant updates or new game releases, the heatmap may appear sparse, reflecting "
       "the Steam client’s preference for utilizing caches closest to the user. However, during times of major "
       "updates or releases, the heatmap will show increased activity, showing Steam using multiple cache locations "
       "for efficient load distribution.")
st.markdown(txt)
st.subheader("Cache Load Heatmap")
col1, col2, col3 = st.columns(3)
with col1:
    region = st.selectbox('Select region', set(x['region'].replace('_', ' ') for x in cm_cache_detail if x['cache'] ), key="heatmap_region", index=None)
with col2:
    start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(days=7), min_value=datetime.datetime(2023, 8, 8), max_value=all_df.index[-1].to_pydatetime(), key="heatmap_start")
with col3:
    end = st.date_input('End data', value=all_df.index[-1], min_value=datetime.datetime(2023, 8, 8), max_value=all_df.index[-1].to_pydatetime(), key="heatmap_end")
if start > end:
    st.error('Start date must be before end date.')
    st.stop()
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
