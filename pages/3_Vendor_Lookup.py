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


#%% connect to snowflake
def connect_and_get_data ():
    datalake="C:\\Users\\SQLe\\U.S. Small Business Administration\\Office of Policy Planning and Liaison (OPPL) - Data Lake\\"

    credentials = open(f"{datalake}/Credentials/Snowflake.txt","r").read()
    myaccount=credentials.split(sep="\n")[0].split(":")[2].split(".")[0].strip("//")
    username=credentials.split(sep="\n")[1].split(":")[1]
    mypassword=credentials.split(sep="\n")[2].split(":")[1]

    connection_parameters = {
        "account": myaccount,
        "user": username,
        "password": mypassword,
        "role": "SYSADMIN",  
        "warehouse": "SBA_US-EAST-1_DEMO_WAREHOUSE", 
        "database": "SBA_US-EAST-1_DEMO_RAW_DB",  
        "schema": "DATA_HUB",  
    }  

    global session
    session = sp.Session.builder.configs(connection_parameters).create()

    data1 = session.table("SMALL_BUSINESS_GOALING")

    from snowflake.snowpark.functions import max
    last_fy = data1.select(max(col("FISCAL_YEAR"))).to_pandas().iloc[0,0]
    last_fy = int(last_fy)

    data2 = session.table("ATOM")
    data2 = data2.filter(col("DATE_SIGNED") > datetime (last_fy, 9, 30, 0, 0))

    return data1, data2

connect_and_get_data()

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

    if uploaded_file:
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
        df_for_in = session.create_dataframe(UEI_list + DUNS_list, schema=["col1"])
        data1=data1.filter((data1["VENDOR_UEI"].isin(df_for_in)) | 
                     (data1["VENDOR_DUNS_NUMBER"].isin(df_for_in)))
        data2=data2.filter(data2["VENDOR_UEI_NUMBER"].isin(df_for_in))
    else:
        data1 = None
        data2 = None
    return data1, data2

def show_FY_graph_table_set_asides (data_filter1, data_filter2):
    #
    SBA_set_asides=["SBA", "SBP","RSB","8AN", "SDVOSBC" ,"8A", "HZC","WOSB","SDVOSBS","HZS","EDWOSB"
    ,"WOSBSS","ESB","HS3","EDWOSBSS"]
    SBA_socio_asides = SBA_set_asides[3:]

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

    dollars_FY = pd.concat([dollars_FY,dollars_ATOM_gp],ignore_index=True).sort_values(["FISCAL_YEAR"])

    dollars_FY["set_aside"] = [x if x in SBA_socio_asides else y if y in SBA_set_asides else x 
                               for x,y in zip(dollars_FY["TYPE_OF_SET_ASIDE"],dollars_FY["IDV_TYPE_OF_SET_ASIDE"]) ]

    dollars_FY.drop(["TYPE_OF_SET_ASIDE","IDV_TYPE_OF_SET_ASIDE"],axis=1, inplace=True)

    # set_aside_table=set_aside_table.with_columns(
# 	[pl.when(pl.col('TYPE_OF_SET_ASIDE').is_in(SBA_socio_asides))
#           .then(pl.col('TYPE_OF_SET_ASIDE'))
#           .when(pl.col('IDV_TYPE_OF_SET_ASIDE').is_in(SBA_set_asides))
#           .then(pl.col('IDV_TYPE_OF_SET_ASIDE'))
#           .otherwise(pl.col('TYPE_OF_SET_ASIDE'))
#           .alias("set_aside")
#    dollars_FY=dollars_FY.rename(columns=doldict).set_index("FY").sort_index().round(0).apply(pd.to_numeric, downcast='integer',errors='ignore')

    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

    try:
        fig=px.line(dollars_FY,x=dollars_FY.index,y=dollars_FY.columns
                    ,color_discrete_sequence=pal
                    ,labels={"value":"$","variable":"Type"})
        st.plotly_chart(fig)
    except:
        pass
    st.table(dollars_FY)


#%%
def download_option (data_filter1, data_filter2):
    vendorcols = ["VENDOR_DUNS_NUMBER","VENDOR_NAME","VENDOR_UEI","UEI_NAME"]
    agencycols = ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"FUNDING_OFFICE_NAME"]
    contract_cols= ['PIID','IDV_PIID','MODIFICATION_NUMBER','FISCAL_YEAR','DATE_SIGNED','IDV_TYPE_OF_SET_ASIDE','TYPE_OF_SET_ASIDE','PRINCIPAL_NAICS_CODE',
                   "PRINCIPAL_NAICS_DESCRIPTION","PRODUCT_OR_SERVICE_CODE",'PRODUCT_OR_SERVICE_DESCRIPTION',]
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

    allcols = vendorcols + agencycols + contract_cols + dolcols

    if data_filter.count()>0:
        data=data_filter.select(allcols)
        data_df=data.to_pandas()
    
    st.download_button ("Download detailed data"
           ,data_df.to_csv()
	       ,file_name="Vendor_id_lookup.csv"
	    )
#%%
    
if __name__ == '__main__':
    page_title= "SBGR Vendor Lookup"
    st.set_page_config(
        page_title= page_title,
        page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
        layout="wide",
        initial_sidebar_state="expanded")
    st.header(page_title)

    data1, data2 = connect_and_get_data()
    #st.write([x for x in data.columns if "DOLLARS" in x])

    data1, data2 = vendor_id (data1, data2)
    if any([data1, data2]):
        show_FY_graph_table_set_asides (data1, data2)
#        download_option (data1, data2)

# enable USASpending API lookup?
#%%
#query the ATOM feed

