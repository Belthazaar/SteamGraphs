import streamlit as st
import pandas as pd
import datetime
from pymongo import MongoClient
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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
                                 {'_id': 0, 'timestamp': 1, 'host':1, 'load': 1}).sort('timestamp', -1))
    if len(caches) == 0:
        return pd.DataFrame(), [], []
    df = pd.DataFrame(caches)
    dates = df.timestamp.unique()
    hosts = list(df.host.unique())
    t_df = df.groupby(['timestamp', 'host'])['load'].mean().unstack().apply(lambda x: x.fillna((hosts.index(x.name) * 2 + 105)), axis=0)
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



st.header('Cache Load per City')
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
    
c_df, c_dates, c_hosts =  city_load(city, start, end)
if c_df.empty:
    st.write('No data available')
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
            st.write('No data available')
            st.stop()
        city_load_scatter(l_df, l_dates, l_hosts, overlay_region, region, all_df)