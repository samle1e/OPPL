#%%
# conda activate c:\Users\SQLe\AppData\Local\miniconda3\envs\py38_env

import snowflake.snowpark as sp
import pandas as pd
import streamlit as st
import plotly.express as px
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import lit
import re
from io import StringIO
from datetime import datetime
from snowflake.snowpark.functions import max

page_title= "SBGR Vendor Lookup"
st.set_page_config(
    page_title= page_title,
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded")

#%% connect to snowflake
def connect_and_get_data ():
    connection_parameters = st.secrets.snowflake_credentials

    global session
    session = sp.Session.builder.configs(connection_parameters).create()

    data1 = session.table("SMALL_BUSINESS_GOALING")

    data2 = session.table("ATOM")
#    data2 = data2.filter(col("DATE_SIGNED") > datetime (last_fy, 9, 30, 0, 0))

    return data1, data2

data1, data2 = connect_and_get_data ()
#%% user input and filter
def vendor_id (data1, data2):
    #user selection
    Id_select=st.sidebar.text_area("Enter UEIs or DUNS separated by commas")
    uploaded_file=st.sidebar.file_uploader("Upload a text file with UEIs or DUNS separated by commas")
    st.sidebar.caption("Use UEIs for data for 2022 on. The search does not reliably match DUNS to UEIs.")

    try_UEI = st.sidebar.checkbox("Try to find UEIs for the DUNS you enter, to return data post-2021")
    try_DUNS = st.sidebar.checkbox("Try to find DUNS for the UEI you enter, to return data pre-2022")
    
    #prepare for filtering
    filter_list=[]
    if Id_select:
        Id_select=Id_select.replace(" ","")
        filter_list=re.split(",|\n",Id_select)

#    uploaded_file = ("C:\\Users\\SQLe\\OneDrive - U.S. Small Business Administration\\HUBZoneFirmsSearch.txt")   
    if uploaded_file:
      #  with open(uploaded_file, 'r') as file:
      #      string_data = file.read()
        stringio = StringIO(uploaded_file.getvalue().decode("utf-8"))
        string_data = stringio.read()
        string_format=string_data.replace(" ","")
        filter_list=re.split(",|\n|\r",string_format)
    
    DUNS_list=[x for x in filter_list if len(x)==9]

    UEI_list=[x for x in filter_list if len(x)==12]

    leftover=[x for x in filter_list if (len(x) != 9) & (len(x) != 12) & (len(x)>1)]

    if leftover:
        st.warning(f"These entries had the improper number of digits and will not be processed: {leftover}")

    if try_DUNS or try_UEI:
        #attempt to match DUNS and UEIs if the user requested
        DUNS_UEI_match = pd.read_csv(f"{datalake}/Mapping Files/DUNS_UEI_crosswalk.csv",converters=({"DUNS":str}))

        def match_id (id_type, lst):        
            match_df = DUNS_UEI_match.loc[DUNS_UEI_match[id_type].isin(lst)
                    ].drop_duplicates()
            if match_df.shape[0]>0:
                st.success(f"These {id_type} were successfully matched:")
                st.table(match_df)

                lst.extend(match_df[id_type])
            else:
                st.warning(f"No {id_type} were matched.")
            return lst

        if try_UEI:
            UEI_list=match_id("DUNS",DUNS_list)       
        if try_DUNS:
            DUNS_list=match_id("UEI",UEI_list)

    #filter the datasets
    if (len(UEI_list)>0) | (len(DUNS_list)>0):
        last_fy = data1.select(max(col("FISCAL_YEAR"))).to_pandas().iloc[0,0]
        last_fy = int(last_fy)

        df_for_in = session.create_dataframe(UEI_list + DUNS_list, schema=["col1"])
        data1=data1.filter((data1["VENDOR_UEI"].isin(df_for_in)) | 
                     (data1["VENDOR_DUNS_NUMBER"].isin(df_for_in)))


        data2=data2.filter((col("DATE_SIGNED") > datetime (last_fy, 9, 30, 0, 0)) &
                            (data2["VENDOR_UEI_NUMBER"].isin(df_for_in)))
    else:
        data1 = None
        data2 = None
    return data1, data2
#%%

def show_FY_graph_table_set_asides (data_filter1, data_filter2):
    #
    set_aside_dict = {'SBP':'Partial Small Business Set-Aside',
                    'SBA':'Small Business Set Aside',
                    '8AN':'8(a) Sole Source',
                    'HS3':'8(a) with HUB Zone Preference',
                    '8A':'8(a) Competed',
                    'HZC':'HUBZone Set-Aside',
                    'HZS':'HUBZone Sole Source',
                    'SDVOSBS':'SDVOSB Sole Source',
                    'SDVOSBC':'Service Disabled VOSB Set-Aside',
                    'WOSB':'Women Owned Small Business',
                    'EDWOSB':'EDWOSB Sole Source',
                    'WOSBSS':'Women Owned Small Business Sole Source',
                    'EDWOSBSS':'EDWOSB Sole Source',

                    'BI':'Buy Indian',
                    'IEE':'Indian Economic Enterprise',
                    'ISBEE':'Indian Small Business Economic Enterprise',
                    'ESB':'Emerging Small Business Set-Aside',
                    'HMP':'HBCU or MI Set-Aside -- Partial',
                    'HMT':'HBCU or MI Set-Aside -- Total',
                    'HMT':'HBCU or MI Set-Aside -- Total',
                    'NONE':'No set aside',
                    'VSB':'Very Small Business Set Aside',
                    'VSA':'Veteran Set Aside',
                    'VSS':'Veteran Sole Source',
 }
    set_aside_dict.setdefault('missing_key', 'No set aside')
    SBA_set_asides = list(set_aside_dict.keys())[:12]
    SBA_socio_asides = SBA_set_asides[2:]
    
    dollars_df = data_filter1.group_by(["TYPE_OF_SET_ASIDE","IDV_TYPE_OF_SET_ASIDE","FISCAL_YEAR"]).sum("DOLLARS_OBLIGATED")
    dollars_df2 = data_filter2.select(["TYPE_OF_SET_ASIDE","IDV_TYPE_OF_SET_ASIDE","DOLLARS_OBLIGATED","DATE_SIGNED"])

    dollars_FY=dollars_df.to_pandas()
    dollars_FY.rename(columns={"SUM(DOLLARS_OBLIGATED)":"DOLLARS_OBLIGATED"},inplace=True)

    dollars_ATOM=dollars_df2.to_pandas()

    #Combine Fiscal Years
    dollars_FY["FISCAL_YEAR"] = dollars_FY["FISCAL_YEAR"].astype(int)
    dollars_ATOM["FISCAL_YEAR"] = [x.year if x.month<10 else x.year + 1 for x in dollars_ATOM["DATE_SIGNED"]]
    
    dollars_ATOM_gp = dollars_ATOM.groupby(["TYPE_OF_SET_ASIDE","IDV_TYPE_OF_SET_ASIDE","FISCAL_YEAR"] ,as_index=False ,dropna=False
                                           ).sum()

    dollars_FY = pd.concat([dollars_FY,dollars_ATOM_gp],ignore_index=True)

    dollars_FY["set_aside"] = [x if x in SBA_socio_asides else y if y in SBA_set_asides else x 
                               for x,y in zip(dollars_FY["TYPE_OF_SET_ASIDE"],dollars_FY["IDV_TYPE_OF_SET_ASIDE"]) ]

    dollars_FY = dollars_FY.drop(["TYPE_OF_SET_ASIDE","IDV_TYPE_OF_SET_ASIDE"],axis=1
                                 ).fillna("NONE")
    dollars_FY.loc[dollars_FY["set_aside"]=="N/A","set_aside"] = "NONE"

    dollars_FY = dollars_FY.groupby(["FISCAL_YEAR","set_aside"],as_index=False).sum()
    dollars_FY = dollars_FY.sort_values(["FISCAL_YEAR","set_aside"]).rename(
        columns={"FISCAL_YEAR":"FY","DOLLARS_OBLIGATED":"Dollars Obligated","set_aside":"Set Aside"}
        ).reset_index(drop=True)
    dollars_FY["Set Aside"] = dollars_FY["Set Aside"].map(set_aside_dict).fillna("No set aside")

    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
    fig = None

    if st.checkbox ("Collapse Set-Asides"):
        dollars_FY = dollars_FY.groupby("FY",as_index=False).sum()
        try:
            fig=px.line(dollars_FY,x="FY",y="Dollars Obligated"
                    ,color_discrete_sequence=pal)
        except: pass
    else:
        try:
            fig=px.line(dollars_FY,x="FY",y="Dollars Obligated",color="Set Aside"
                    ,color_discrete_sequence=pal)
        except: pass
    if fig:    
        st.plotly_chart(fig)
    st.dataframe(dollars_FY.style.format({"Dollars Obligated": '${:,.0f}'}))
    st.write("")
    
#%%
@st.cache_data
def download_option (data_filter1, data_filter2):
    vendorcols1 = ["VENDOR_DUNS_NUMBER","VENDOR_NAME","VENDOR_UEI","UEI_NAME"]
    vendorcols2 = ["VENDOR_UEI_NUMBER","VENDOR_NAME"]
    agencycols = ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"FUNDING_OFFICE_NAME"]
    contract_cols= ['PIID','IDV_PIID','MODIFICATION_NUMBER','DATE_SIGNED','IDV_TYPE_OF_SET_ASIDE','TYPE_OF_SET_ASIDE','PRINCIPAL_NAICS_CODE',
                   "PRINCIPAL_NAICS_DESCRIPTION","PRODUCT_OR_SERVICE_CODE",'PRODUCT_OR_SERVICE_DESCRIPTION',]
    dolcols=["DOLLARS_OBLIGATED"]

    if data_filter1.count()>0:
        data1=data_filter1.select(vendorcols1 + agencycols + contract_cols + dolcols)
        data_df = data1.to_pandas()
        data_df["VENDOR_NAME"] = data_df["VENDOR_NAME"].fillna(data_df["UEI_NAME"])

    if data_filter2.count()>0:
        data2 = data_filter2.select(vendorcols2 + agencycols + contract_cols + dolcols)
        data_df2 = data2.to_pandas().rename(columns={"VENDOR_UEI_NUMBER":"VENDOR_UEI"})

        if data_filter1.count()>0:
            data_df = pd.concat([data_df, data_df2]).sort_values("DATE_SIGNED")
        else:
            data_df = data_df2.sort_values("DATE_SIGNED")

    st.download_button ("Download detailed data"
           ,data_df.to_csv(index=False)
	       ,file_name="Vendor_id_lookup.csv"
	    )
#%%
    
if __name__ == '__main__':
    st.header(page_title)

    data1, data2 = connect_and_get_data()

    data1, data2 = vendor_id (data1, data2)
    if any([data1, data2]):
        show_FY_graph_table_set_asides (data1, data2)
        download_option (data1, data2)
    
    st.caption("Source: SBA Small Business Goaling Report for FY09-FY22; ATOM Feed for later data")