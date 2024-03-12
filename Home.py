
import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from pymongo import MongoClient
import datetime
import plotly.express as px
import json


st.set_page_config(page_title="Steam Cache Monitor", page_icon=":video_game:", layout="wide")

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

st.header("Steam Download Statistics")
txt = "Welcome to our Steam Download Statistics page. An interactive portal for exploring Steam's download statistics as they publish [here](https://store.steampowered.com/stats/content). We gather the traffic data promoted by Steam to analyze long-term trends. The data is refreshed every 10 minutes, ensuring up-to-date information is always available."
st.markdown(txt)
st.markdown("For more information about this project and the data, please visit our project page [here](https://steam.iijlab.net/).")
st.subheader("Global Traffic")
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
if start > end:
    st.error('Start date must be before end date.')
    st.stop()
else:
    graph_traffic_region(all_df.loc[start:end], all_region, inc_global)
