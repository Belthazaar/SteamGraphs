
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from pymongo import MongoClient
import datetime
import plotly.express as px


st.set_page_config(page_title="Steam Cache Monitor", page_icon=":video_game:", layout="wide")

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


# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    
    return MongoClient(**st.secrets["mongo"])

client = init_connection()

@st.cache_data(ttl=600)
def get_data():
    db = client.steam
    items = list(db.global_bandwidth.find({},{'_id': 0}))
    return items

@st.cache_data(ttl=600)
def get_latest_data():
    db = client.steam
    items =list(db.global_bandwidth.find({},{'_id': 0}).sort([("timestamp", -1)]).limit(288))
    return items

@st.cache_data(ttl=600)
def get_traffic_data_date(start, end, region=None):
    db = client.steam
    end = end + datetime.timedelta(days=1)
    if region:
        items = list(db.global_bandwidth.find({'timestamp': {"$gte": start, "$lte": end}, "region": region},{'_id': 0}))
    else:
        items = list(db.global_bandwidth.find({'timestamp': {"$gte": start, "$lte": end}},{'_id': 0}))
    df = pd.DataFrame(items)
    if 'timesamp' in df.columns:
        df.set_index('timestamp', inplace=True)
    return df

@st.cache_data(ttl=600)
def graph_traffic_region(df, region: None, inc_global=False):
    if region:
        a_r = [c.replace(' ', '_') for c in region]
        fig = px.line(df[a_r], x=df.index, y=df[a_r].columns)
    else:
        if inc_global:
            fig = px.line(df, x=df.index, y=df.columns)
        else:
            cols = [c for c in df.columns if c != 'Global']
            fig = px.line(df, x=df.index, y=cols)

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Traffic (Gbps)",
        xaxis_tickformat='%y-%m-%d %H',
        legend_title_text='Regions',
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
            entrywidth=120,
        ),
    )

    st.plotly_chart(fig, use_container_width=True)

def graph_traffic_all(df):
    fig = px.line(df, x=df.index, y=df.columns)
    st.plotly_chart(fig, use_container_width=True)

st.header("All traffic data")
all_df = pd.DataFrame(get_data())
all_df.set_index('timestamp', inplace=True)
all_df.sort_index(inplace=True)

colnames = all_df.columns
now = datetime.datetime.utcnow()

col1, col2, col3 = st.columns(3)
with col1:
    all_region = st.multiselect('Select regions', [c.replace('_', ' ') for c in colnames])
with col2:
    start = st.date_input('Start data', value=all_df.index[-1] - datetime.timedelta(hours=48), min_value=all_df.index[0], max_value=all_df.index[-1])
with col3:
    end = st.date_input('End data', value=all_df.index[-1], min_value=all_df.index[0], max_value=all_df.index[-1])

inc_global = st.toggle('Include global traffic', False)
graph_traffic_region(all_df.loc[start:end], all_region, inc_global)
