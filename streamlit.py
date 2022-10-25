import pandas as pd
import numpy as np
import streamlit as st
import psycopg2
from sqlalchemy import column, create_engine
import time
import plotly.express as px

conn_string = "postgresql://tmshszilbcculx:ee95204861668778837467651b39614a5529edd88412c1fe0a8b9a21419bf437@ec2-54-246-185-161.eu-west-1.compute.amazonaws.com:5432/dd83caln9k71ig"
engine = create_engine(conn_string)
df = pd.read_sql('tracking_logs',con=engine)
st.write(df)

#header_container=st.header_container
st.title('User/time')
Table1=df[['user_id','date']]
Table1 =Table1[Table1['user_id'] !='anonymous']
# Table1=Table1.user_id.nunique()
Table1=Table1.groupby('date').user_id.nunique()
st.write(Table1)
line_chart=st.line_chart(Table1)
#st.write(line_chart)

#User_id and Courseid 
Table2=df[['user_id','course_id','date','event_type']]
Table2 = Table2[Table2['event_type'] == 'edx.course.enrollment.activated']
st.write(Table2)
Table2['date']=Table2['date'].dt.date
#Table2=line_chart(Table2)
#experiments
st.title('Users/Course_id')
Table3 =Table2.groupby(['date'])['user_id'].nunique().reset_index()
st.write(Table3)


Table3 = px.bar(Table3, x="date", y="user_id")
st.write(Table3)
