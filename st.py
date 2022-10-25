from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from streamlit.components.v1 import html

@st.cache
def load_data():
    conn_string = "postgresql://tmshszilbcculx:ee95204861668778837467651b39614a5529edd88412c1fe0a8b9a21419bf437@ec2-54-246-185-161.eu-west-1.compute.amazonaws.com:5432/dd83caln9k71ig"
    engine = create_engine(conn_string)
    df = pd.read_sql('tracking_logs',con=engine)
    engine.dispose()
    return df

df = load_data()
start_date = df['date'].min()
end_date = df['date'].max()
idx = pd.date_range(str(start_date), str(end_date))
# idx = pd.date_range('2022-02-01','2022-04-18')



# transformations
df_registrations = df[df['event_type'] == "/user_api/v1/account/registration/"].copy()
df_registrations = df_registrations[['date']]

df_enrolments = df[df['event_type'] == "edx.course.enrollment.activated"].copy()
df_enrolments = df_enrolments[['date']]


def set_delta(x):
    try:
        if(x=='Past 24 hours'): td = pd.DateOffset(days=1)
        elif(x=='Past Week'): td = pd.DateOffset(weeks=1)
        elif(x=='Past Month'): td = pd.DateOffset(months=1)
        elif(x=='Past Quarter'): td = pd.DateOffset(months=3)
        elif(x=='Past Year'): td = pd.DateOffset(years=1)
        return td
    except:
        pass

# Streamlit Dashboard
st.title('Aggregate Metrics')

dashboard_sidebar = st.sidebar.selectbox('Select Dashboard', ('Aggregate Metrics', 'Course Metrics'))
with st.sidebar:
    timelines = ('Past 24 hours', 'Past Week', 'Past Month', 'Past Quarter', 'Past Year')
    timeline_radio = st.radio('Timeline', options=timelines, index=1)

time_delta = set_delta(timeline_radio)
# st.write(time_delta)


if dashboard_sidebar == 'Aggregate Metrics':
    # registrations & enrolments

    with open('style.css') as f:
        st.markdown(f'<style>{f.read()}</style>',unsafe_allow_html=True) 
        f.close()  
    col1, col2, col3, col4 = st.columns(4)
    temp = df_registrations.groupby(by='date').size()
    temp.index = pd.DatetimeIndex(temp.index)
    temp = temp.reindex(idx, fill_value=0).reset_index()
    temp.columns = ['date', 'registrations']
    with col2:
        delta = (temp['registrations'].iloc[-1] - temp['registrations'].iloc[-2])/temp['registrations'].iloc[-2]
        st.metric('Registrations', temp['registrations'].sum(), delta="{:.2%}".format(delta))
        

    temp = df_enrolments.groupby(by='date').size()
    temp.index = pd.DatetimeIndex(temp.index)
    temp = temp.reindex(idx, fill_value=0).reset_index()
    temp.columns = ['date', 'enrolments']
    with col3:
        delta = (temp['enrolments'].iloc[-1] - temp['enrolments'].iloc[-2])/temp['enrolments'].iloc[-2]
        st.metric('Course Enrolments', temp['enrolments'].sum(), delta="{:.2%}".format(delta))

    
    # users/day
    df_visitors = df[df['user_id'] != "anonymous"].copy()
    df_visitors = df_visitors[['user_id', 'date']]
    df_visitors['date'] = df_visitors['date'].apply(lambda x: x.date())
    temp = df_visitors.groupby(by='date', as_index=False).user_id.nunique()

    fig = px.bar(temp, x='date', y='user_id')
    st.write(fig)



# Course Metrics
if dashboard_sidebar == 'Course Metrics':
    pass


with open('bootstrap.html') as A:
        html(f'{A.read()}', height=500) 


    
videoplayed=df[df['event_type'].isin(['load_video','pause_video','seek_video','speed_change_video','stop_video'])]
st.write(videoplayed)