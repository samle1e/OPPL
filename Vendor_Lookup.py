#%%
# conda activate c:\Users\SQLe\AppData\Local\miniconda3\envs\py38_env

import pandas as pd
import streamlit as st
import plotly.express as px
import re
from io import StringIO
from datetime import datetime
from snowflake.connector import connect

page_title= "SBGR Vendor Lookup"
st.set_page_config(
    page_title= page_title,
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

start_year = 2010
end_year = 2024
#%% connect to snowflake

con = connect(**st.secrets.snowflake_credentials)
cursor = con.cursor()

@st.cache_resource
def get_data (query, params=None):
    '''Takes a SQL query with optional paramters. Returns a polars dataframe.'''
    if params:
        cursor.execute(query, params)
    else: 
        cursor.execute(query)
    results = cursor.fetch_pandas_all()
    return results

#%% user input and filter
def vendor_id ():
    #user selection
    Id_select=st.sidebar.text_area("Enter UEIs or DUNS separated by commas")
    uploaded_file=st.sidebar.file_uploader("Upload a text file with UEIs or DUNS on separate lines")

    #prepare for filtering
    filter_list=[]
    if Id_select:
        Id_select=Id_select.replace(" ","")
        filter_list=re.split(",|\n",Id_select)

    #uploaded_file = ("C:\\Users\\SQLe\\OneDrive - U.S. Small Business Administration\\HUBZoneFirmsSearch.txt")   
    if uploaded_file:
        filter_list = pd.read_csv(uploaded_file, header = None).squeeze().to_list()
    
    DUNS_list=[x for x in filter_list if len(x)==9]

    UEI_list=[x for x in filter_list if len(x)==12]

    leftover=[x for x in filter_list if (len(x) != 9) & (len(x) != 12) & (len(x)>1)]

    if leftover:
        st.warning(f"These entries had the improper number of digits and will not be processed: {leftover}")

    return DUNS_list, UEI_list

def get_table (DUNS_list, UEI_list):
    if not UEI_list: UEI_list = ['']
    if not DUNS_list: DUNS_list = ['']
    
    UEI_tuple = tuple(UEI_list)
    DUNS_tuple = tuple(DUNS_list)

    results = cursor.execute('''SELECT FY, 
            IFF(TYPE_OF_SET_ASIDE IS NULL or TYPE_OF_SET_ASIDE = 'NO SET ASIDE USED.', 'NONE', TYPE_OF_SET_ASIDE) as "Set Aside", 
            sum(DOLLARS_OBLIGATED) as "Dollars Obligated"
        FROM VENDOR_LOOKUP
        WHERE VENDOR_UEI in (%(UEI_list)s) OR VENDOR_DUNS_NUMBER IN (%(DUNS_list)s)
        GROUP BY FY, IFF(TYPE_OF_SET_ASIDE IS NULL or TYPE_OF_SET_ASIDE = 'NO SET ASIDE USED.', 'NONE', TYPE_OF_SET_ASIDE)
        ORDER BY 1, 2
        ''',{'UEI_list':UEI_tuple, 'DUNS_list':DUNS_tuple}
                ).fetch_pandas_all()
    return results
#%%
   

def show_FY_graph_table_set_asides (results):
    
    fig = None
    dollars_FY = results.copy()

    if st.checkbox ("Collapse Set-Asides") and ~dollars_FY.empty:
        dollars_FY = results.groupby("FY",as_index=False).sum(numeric_only=True)
        try:
            fig=px.line(dollars_FY,x="FY",y="Dollars Obligated"
                    ,color_discrete_sequence=px.colors.qualitative.Dark24, markers=True)
        except: pass
    else:
        try:
            fig=px.line(dollars_FY,x="FY",y="Dollars Obligated",color="Set Aside"
                    ,color_discrete_sequence=px.colors.qualitative.Dark24, markers=True)
        except: pass
    if fig is not None: 
        try:
            fig.update_layout(xaxis={
                'range': [dollars_FY["FY"].min(), dollars_FY["FY"].max()], 
                'tickvals': [*range(int(dollars_FY["FY"].min()), int(dollars_FY["FY"].max())+2)]
                })
            st.plotly_chart(fig)
        except: pass
    
    st.dataframe(dollars_FY.style.format({"FY":'{:.0f}',"Dollars Obligated": '${:,.0f}'}), hide_index=True)
    st.write("")
    
#%%
def download_option (DUNS_list, UEI_list, start_year:int, end_year:int):
    years = st.slider("FYs for download", min_value = start_year, max_value = end_year, value=(start_year,end_year))
    
    if st.button('Show Download Button'):
        if not UEI_list: UEI_list = ['']
        if not DUNS_list: DUNS_list = ['']
        
        UEI_tuple = tuple(UEI_list)
        DUNS_tuple = tuple(DUNS_list)

        data = cursor.execute('''SELECT *
            FROM VENDOR_LOOKUP
            WHERE (VENDOR_UEI in (%(UEI_list)s) OR VENDOR_DUNS_NUMBER IN (%(DUNS_list)s))
            and FY>=(%(year_0)s) and FY<=(%(year_1)s)
            ORDER BY FY, DATE_SIGNED
            ''',{'UEI_list':UEI_tuple, 'DUNS_list':DUNS_tuple, 'year_0':years[0], 'year_1':years[1]}
                    ).fetch_pandas_all()
        try:
            st.download_button ("Download detailed data"
                ,data.round(2).to_csv(index=False)
                ,file_name="Vendor_id_lookup.csv"
                )
        except:
            st.write("No contracts found")
#%%
    
if __name__ == '__main__':
    st.header(page_title)
    DUNS_list, UEI_list = vendor_id ()
    if any([DUNS_list, UEI_list]):
        results = get_table (DUNS_list, UEI_list)
        show_FY_graph_table_set_asides (results)
        download_option (DUNS_list, UEI_list, start_year, end_year)
    
    st.caption('''Enter DUNs or UEIs to the left, or upload a file with a list of DUNs or UEIs. 
    The app automatically matches DUNs to UEIs and vice-versa based on a crosswalk from the April 2022 switchover.
    Source: SBA Small Business Goaling Report for FY09-FY22, Preliminary SBGR for FY23, ATOM Feed for FY24''')

# %%
