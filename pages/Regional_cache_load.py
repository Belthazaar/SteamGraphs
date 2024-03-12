import streamlit as st
import pandas as pd
import datetime
import json
from pymongo import MongoClient
from plotly.subplots import make_subplots

st.set_page_config(page_title="Steam cache load per region", page_icon=":video_game:", layout="wide")

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

@st.cache_data(ttl=600)
def get_data():
    db = client.steam
    items = db.global_bandwidth.find({},{'_id': 0})
    items = list(items)  # make hashable for st.cache_data
    return items

all_df = pd.DataFrame(get_data())
all_df.set_index('timestamp', inplace=True)
all_df.sort_index(inplace=True)

colnames = list(all_df.columns)
now = datetime.datetime.utcnow()

@st.cache_data(ttl=1800)
def mean_region_cache_load(start, end, region):
    region = region.replace(' ', '_')
    db = client.steam
    start = datetime.datetime.combine(start, datetime.time())
    end = datetime.datetime.combine(end, datetime.time()) + datetime.timedelta(days=1)
    items = list(db.cache.find({'timestamp': {"$gte": start, "$lte": end}, 'region': region, 'type': 'SteamCache'},{'timestamp': 1, 'host': 1, 'load': 1, '_id': 0}))
    df = pd.DataFrame(items)
    df = df.groupby(['timestamp', 'host'])['load'].mean().unstack().fillna(100)
    return df

@st.cache_data(ttl=1800)
def mean_regions_cache_load(start, end):
    db = client.steam
    start = datetime.datetime.combine(start, datetime.time())
    end = datetime.datetime.combine(end, datetime.time()) + datetime.timedelta(days=1)
    items = list(db.cache.find({'timestamp': {"$gte": start, "$lte": end}},{'_id': 0, 'time_stamp': 0, 'rounded_timestamp': 0}))
    df = pd.DataFrame(items)
    df = df.groupby(['timestamp', 'region'])['load'].mean().unstack().fillna(100)
    return df

@st.cache_data(ttl=1800)
def mean_cache_load_graph(df, overlay_traffic=False, traffic_df=None, region=None):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    dates = df.index.unique()

    if overlay_traffic:
        region_traffic = traffic_df[region.replace(' ', '_')]
        region_traffic_filtered = region_traffic[region_traffic.index.isin(dates)]
        df.sort_index(inplace=True)
        region_traffic_filtered.sort_index(inplace=True)
        fig.add_scatter(x=df.index, y=df.mean(axis=1), mode='lines', name=f'Mean Cache load')
        fig.add_scatter(x=region_traffic_filtered.index, y=region_traffic_filtered, mode='lines', name=f'Mean Traffic for {region}', secondary_y=True)
    else:
        fig.add_scatter(x=df.index, y=df.mean(axis=1), mode='lines', name='Mean Cache Load')

    fig.update_layout(
        title=f'Mean Cache Load for {region}',
        xaxis_title='Date',
        yaxis_title='Cache Server load',
        xaxis_tickformat='%y-%m-%d %H',
        yaxis_range=[0, 120],
        xaxis_range=[df.index[0], df.index[-1] + datetime.timedelta(minutes=10)],
        showlegend=True,
        legend_title_text='Hosts',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        height=600,
    )
    st.plotly_chart(fig, use_container_width=True)

st.header('Regional Cache Load')
txt = "This page shows the mean cache load for a region. The mean cache load is calculated by taking the mean of the cache load for all caches in the region. When a cache is no longer available it is assumed to have a load of 100. The graph can be overlayed with the regional traffic data to see if there is a correlation between the cache load and the traffic."
st.markdown(txt)

st.subheader("Mean Cache Load")
cache_regions = list(set(x['region'].replace('_', ' ') for x in cm_cache_detail if x['cache']))
col1, col2, col3 = st.columns(3)
with col1:
    region = st.selectbox('Select region', cache_regions, key="mean_load_region")
with col2:
    start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(hours=48), min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key="mean_load_start")
with col3:
    end = st.date_input('End data', value=all_df.index[-1], min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key="mean_load_end")
overlay_traffic = st.toggle('Overlay regional traffic data', False, key="mean_load_overlay")
mean_load = mean_region_cache_load(start, end, region)
mean_cache_load_graph(mean_load, overlay_traffic, all_df, region)


all_col, region_col = st.columns(2)
with all_col:
    show_all = st.toggle("Show all regions", key="mean_load_show_all")

with region_col:
    add_region = st.toggle("Add region", key="mean_load_add_region")

if show_all:
    st.header("Mean cache load for all regions")
    for r in cache_regions:
        st.header('Cache Load for ' + r)
        mean_load = mean_region_cache_load(start, end, r)
        overlay_t = st.toggle('Overlay regional traffic data', False, key=f"mean_load_overlay_{cache_regions.index(r)}")
        mean_cache_load_graph(mean_load, overlay_t, all_df, r)


if add_region:
    reg = st.selectbox('Select region', cache_regions, key="mean_load_add_region_select", index=None)
    if reg:
        st.header("Mean cache load for " + reg)
        mean_load = mean_region_cache_load(start, end, reg)
        overlay = st.toggle('Overlay regional traffic data', False, key="mean_load_overlay_add")
        mean_cache_load_graph(mean_load, overlay, all_df, reg)
