import streamlit as st
import pandas as pd
import datetime
import json
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Steam cache load per city", page_icon=":video_game:", layout="wide")

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

@st.cache_data(ttl=1200)
def get_data():
    db = client.steam
    items = db.global_bandwidth.find({},{'_id': 0})
    items = list(items)  # make hashable for st.cache_data
    return items


@st.cache_data(ttl=1800)
def city_load(city, start, end):
    db = client.steam

    s = datetime.datetime.combine(start, datetime.time())
    e = datetime.datetime.combine(end, datetime.time())
    caches = list(db.cache.find({"timestamp": {"$gte": s, "$lte": e}, 
                                 'city': city, 'type': 'SteamCache'}, 
                                 {'_id': 0, 'timestamp': 1, 'host':1, 'load': 1}).sort('timestamp', 1))
    if len(caches) == 0:
        return pd.DataFrame(), [], []
    df = pd.DataFrame(caches)
    dates = df.timestamp.unique()
    hosts = list(df.host.unique())
    t_df = df.groupby(['timestamp', 'host'])['load'].mean().unstack().apply(lambda x: x.fillna((hosts.index(x.name) * 2 + 105)), axis=0)
    t_df.sort_index(inplace=True)
    return t_df, dates, hosts


@st.cache_data(ttl=1800)
def city_load_scatter(t_df, dates, hosts, overlay_region=False, region=None, traffic_df=None):

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    for host in hosts:
        fig.add_trace(go.Scatter(x=t_df.index, y=t_df[host], mode='markers', name=host.split('.')[0]))
    fig.update_layout(
        xaxis_title='Date',
        yaxis_title='Cache Server load',
        xaxis_tickformat='%y-%m-%d %H',
        yaxis_range=[0, len(hosts) * 2 + 130],
        xaxis_range=[dates[0], dates[-1] + datetime.timedelta(minutes=10)],
        showlegend=True,
        legend_title_text='Hosts',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            entrywidth=100,
        ),
        height=600,
    )
    fig.add_hline(y=100, line_dash="dash", line_color="red", name='Max Load')
    if overlay_region:
        region_traffic = traffic_df[region]
        region_traffic_filtered = region_traffic[region_traffic.index.isin(dates)]
        fig.add_trace(trace=go.Scatter(x=region_traffic_filtered.index, 
                                    y=region_traffic_filtered, 
                                    mode='lines', 
                                    name=f'Traffic for {region}'), 
                                    secondary_y=True,
                                    )
    st.plotly_chart(fig, use_container_width=True)


st.header('Load distribution between caches within a city')
txt = ("This page allows you to compare the load distribution between caches within a city. Optionally, you can overlay the regional traffic data to see if there is a correlation between the cache load and the traffic in the region.\n\n")
st.markdown(txt)
st.subheader('Cache Load per City')
all_df = pd.DataFrame(get_data())
all_df.set_index('timestamp', inplace=True)
all_df.sort_index(inplace=True)
now = datetime.datetime.now()

cache_df = pd.DataFrame(cm_cache_detail)
cities_with_cache = sorted(list(cache_df[cache_df['cache'] != ''].city.unique()))
col1, col2, col3 = st.columns(3)

with col1:
    city = st.selectbox('Select city', cities_with_cache)
with col2:
    start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(hours=48), min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key="cache_start")
with col3:
    end = st.date_input('End data', value=all_df.index[-1], min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key="cache_end")

if start > end:
    st.error('Start date must be before end date.')
    st.stop()

c_df, c_dates, c_hosts =  city_load(city, start, end)
if c_df.empty:
    st.error('No data found for given timeframe')
    st.stop()
overlay_region = st.toggle('Overlay regional traffic data', False)
region = [c for c in cm_cache_detail if c['city'] == city][0]['region']
city_load_scatter(c_df, c_dates, c_hosts, overlay_region, region, all_df)

container_array = [st.empty() for i in range(10)]

for i in range(10):
    if st.toggle('Add city', False, key=f"cache_{i}"):
        col1, col2, col3 = st.columns(3)
        with col1:
            city = st.selectbox('Select city', cities_with_cache, key=f"cache_city_{i}")
        with col2:
            start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(hours=48), min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key=f"cache_start_{i}")
        with col3:
            end = st.date_input('End data', value=all_df.index[-1], min_value=all_df.index[0].to_pydatetime(), max_value=all_df.index[-1].to_pydatetime(), key=f"cache_end_{i}")
        overlay_region = st.toggle('Overlay regional traffic data', False, key=f"cache_overlay_{i}")
        region = [c for c in cm_cache_detail if c['city'] == city][0]['region']
        l_df, l_dates, l_hosts = city_load(city, start, end)
        if l_df.empty:
            st.error('No data found for given timeframe')
            st.stop()
        city_load_scatter(l_df, l_dates, l_hosts, overlay_region, region, all_df)