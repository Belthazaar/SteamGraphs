import streamlit as st
import pandas as pd
import datetime
from pymongo import MongoClient
from plotly.subplots import make_subplots

st.set_page_config(page_title="Steam cache load per region", page_icon=":video_game:", layout="wide")

cm_cache_detail = [
    {'region': 'South America', 'code': 'ARG', 'cm': 'eze1', 'cache': '',       'cell_id': 116, 'city': 'Buenos Aires'},
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
cache_regions = list(set(x['region'].replace('_', ' ') for x in cm_cache_detail if x['cache']))

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
        overlap_dates = region_traffic_filtered.index.intersection(dates)
        filtered_df = df[df.index.isin(overlap_dates)]
        filtered_df.sort_index(inplace=True)
        region_traffic_filtered.sort_index(inplace=True)
        fig.add_scatter(x=filtered_df.index, y=filtered_df.mean(axis=1), mode='lines', name='Mean Cache Load')
        fig.add_scatter(x=region_traffic_filtered.index, y=region_traffic_filtered, mode='lines', name=f'Mean Traffic for {region}', secondary_y=True)
    else:
        fig.add_scatter(x=df.index, y=df.mean(axis=1), mode='lines', name='Mean Cache Load')
    
    fig.update_layout(
        title=f'Mean Cache Load for {region}',
        xaxis_title='Date',
        yaxis_title='Cache Server load',
        xaxis_tickformat='%y-%m-%d %H',
        yaxis_range=[0, 130],
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
    # fig = px.line(df, x=df.index, y=df.columns)
    st.plotly_chart(fig, use_container_width=True)


st.header("Mean Cache Load")
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
    # col1, col2, col3 = st.columns(3)
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