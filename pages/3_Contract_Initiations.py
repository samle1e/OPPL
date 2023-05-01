#%%
# conda activate c:\Users\SQLe\AppData\Local\miniconda3\envs\py38_env

import snowflake.snowpark as sp
import pandas as pd
import streamlit as st
import plotly.express as px
from snowflake.snowpark.functions import col
import snowflake.snowpark.functions as spf
from datetime import datetime

page_title= "Contract Initiations Explorer"

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

#%%
def connect_and_get_data ():
    connection_parameters = st.secrets.snowflake_credentials
    global session
    session = sp.Session.builder.configs(connection_parameters).create()
    data1 = session.table("SMALL_BUSINESS_GOALING")
    data2 = session.table("ATOM")
    return [data1, data2]

#%%
def filter_data (data_lst, dict):
    for k in dict:
         df_for_in = session.create_dataframe(dict[k], schema=["col1"])
         data_lst = [x.filter(x[k].isin(df_for_in)) for x in data_lst]
    return data_lst

@st.cache_data
def get_choices_agency (_data):
    agencycols = ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME']
    all_choices = _data[0].select(agencycols).distinct().to_pandas().sort_values(agencycols)
    return all_choices

#%%
def filter_agency (data):
    choices = get_choices_agency (data)

    def user_choose_agency (choices):
        selection = {}
        department_choices = choices["FUNDING_DEPARTMENT_NAME"].unique()
        department_select = st.sidebar.multiselect("Funding Department (can combine)"
                                           ,department_choices)
        agency_choices = choices.loc[choices["FUNDING_DEPARTMENT_NAME"].isin(department_select)
                                         ,"FUNDING_AGENCY_NAME"].to_list()
        agency_select = st.sidebar.multiselect("Funding Agency (can combine)", agency_choices
                                       ,disabled = (len(department_select) != 1))

        if department_select:
            selection.update({"FUNDING_DEPARTMENT_NAME":department_select})
        if agency_select:
            selection.update({"FUNDING_AGENCY_NAME":agency_select})
        return selection

    selection = user_choose_agency (choices)
    if len(selection) != 0:
        data = filter_data (data, selection)
    
    return data

#%%
def filter_set_aside_type (data):
    set_aside_dict = {'SBA':'Small Business Set Aside',
                    'SBP':'Partial Small Business Set-Aside',
                    '8AN':'8(a) Sole Source',
                    '8A':'8(a) Competed',
                    'HS3':'8(a) with HUB Zone Preference',
                    'HZC':'HUBZone Set-Aside',
                    'HZS':'HUBZone Sole Source',
                    'SDVOSBS':'SDVOSB Sole Source',
                    'SDVOSBC':'Service Disabled VOSB Set-Aside',
                    'WOSB':'Women Owned Small Business Set-Aside',
                    'WOSBSS':'Women Owned Small Business Sole Source',
                    'EDWOSB':'EDWOSB Set-Aside',
                    'EDWOSBSS':'EDWOSB Sole Source',
                    }
    set_aside_dict_rev = {v:k for k,v in set_aside_dict.items()}
    choices = list(set_aside_dict.values())
    setaside_select = st.sidebar.multiselect("Set Asides (can combine)"
                                           ,choices)
    setaside_select_rev = [set_aside_dict_rev[x] for x in setaside_select]

    if len(setaside_select) != 0:
        df_for_in = session.create_dataframe(setaside_select_rev, schema=["col1"])
        data = [x.filter((x["TYPE_OF_SET_ASIDE"].isin(df_for_in)) | (x["IDV_TYPE_OF_SET_ASIDE"].isin(df_for_in))) 
                    for x in data]    
    return data

#%%
def filter_NAICS (data):
    @st.cache_data
    def get_NAICS_choices ():
        NAICSnames=[None]*3
        NAICSnames[0]=pd.read_excel("https://www.census.gov/naics/2012NAICS/2-digit_2012_Codes.xls")
        NAICSnames[1]=pd.read_excel("https://www.census.gov/naics/2017NAICS/2-6%20digit_2017_Codes.xlsx")
        NAICSnames[2]=pd.read_excel("https://www.census.gov/naics/2022NAICS/2-6%20digit_2022_Codes.xlsx")

        for i in range(0,3):
            NAICSnames[i]=NAICSnames[i].filter(regex="Code|Title")
            NAICSnames[i]=NAICSnames[i].set_index(NAICSnames[i].columns[0])

        combined=NAICSnames[0].join([NAICSnames[1],NAICSnames[2]],how="outer")
        combined=combined.loc[combined.index.dropna()]
        combined.index=combined.index.astype("str")
        combined["Title"]=combined.iloc[:,0].combine_first(combined.iloc[:,1]
                                            ).combine_first(combined.iloc[:,2])
        NAICS_names=combined.sort_index().loc[:,"Title"].squeeze().to_dict()
        return NAICS_names

    NAICS_names = get_NAICS_choices()
    choices = [f"{k}: {v}" for k,v in NAICS_names.items()]

    NAICS_select = st.sidebar.multiselect(label="NAICS (can combine)"
                                ,options=choices)
    
    NAICS_select_short=[x.split(": ")[0] for x in NAICS_select]

    NAICS_6 = [x for x in NAICS_select_short if len(x)==6]

    for i in NAICS_select_short:
        if len(i)<6:
            NAICS_6.extend([x for x in NAICS_names.keys() if (len(x)==6) & (x.startswith(i))])

    if len(NAICS_6) != 0:
        data = filter_data (data, {"PRINCIPAL_NAICS_CODE":NAICS_6})
    return data
#%%
def filter_PSC (data):
    @st.cache_data
    def get_PSC_choices ():
        PSCnames=pd.read_excel(
            "https://www.acquisition.gov/sites/default/files/manual/PSC%20April%202022.xlsx")

        PSC_names=PSCnames.drop_duplicates("PSC CODE",keep="first").set_index(
            "PSC CODE").filter(regex="NAME")
        PSC_names.index=PSC_names.index.astype("str")
        PSC_names["Title"]=PSC_names.iloc[:,1].combine_first(PSC_names.iloc[:,0])
        PSC_names=PSC_names.sort_index().loc[:,"Title"].squeeze()
        return PSC_names
    
    PSC_select = get_PSC_choices()
    options = [f"{k}: {v}" for k, v in PSC_select.items()]

    PSC_pick=st.sidebar.multiselect(label="Product Service Codes (can combine)"
                            ,options = options)
    PSC_pick_short=[x.split(": ")[0] for x in PSC_pick]

    PSC_4 = [x for x in PSC_pick_short if len(x)==4]

    for i in PSC_pick_short:
        if len(i)<4:
            PSC_4.extend([x for x in PSC_select.index() 
                          if (len(x)==4) & (x.startswith(i))])

    if len(PSC_4) != 0:
        data = filter_data (data, {"PRODUCT_OR_SERVICE_CODE":PSC_4})
    return data
#%%
def filter_bundled_consolidated (data):
    choices = ["All Actions",
               "Bundled Actions Only", 
               "Consolidated Actions Only",
               "Bundled or Consolidated Actions"]
    
    BC_select = st.sidebar.radio ("Bundled and Consolidated Actions", options = choices)

    BC_choices_dict = {
        "bundled": ["A", "B", "C", "E", "F", "G"],
        "consolidated": ["A", "B", "C", "Y"],
    }

    if BC_select == choices[1]:
        data = filter_data (data, {"BUNDLED_CONTRACT_EXCEPTION":BC_choices_dict["bundled"]})
    elif BC_select == choices[2]:
        data = filter_data (data, {"CONSOLIDATED_CONTRACT":BC_choices_dict["consolidated"]})
    elif BC_select == choices[3]:
        df_for_in = session.create_dataframe(BC_choices_dict["bundled"] + BC_choices_dict["consolidated"], schema=["col1"])
        data = [x.filter((x["BUNDLED_CONTRACT_EXCEPTION"].isin(df_for_in)) | (x["CONSOLIDATED_CONTRACT"].isin(df_for_in))) 
                    for x in data]  
    return data
#%%
def filter_award_type (data):
    st.sidebar.write("<b>Common adjustments</b>",unsafe_allow_html = True)
    choices = ["Exclude all orders and BPA calls",
               "Exclude initial load of multiple-award contracts",
               "Exclude initial load of all indefinite-delivery contracts"]
    
    award_type = [True] * 4
    for i, x in enumerate(choices):
        award_type[i] = st.sidebar.checkbox(x)

    if award_type[0]:
        data = filter_data (data, {"AWARD_IDV_TYPE_DESCRIPTION":["DEFINITIVE CONTRACT"]})
    elif award_type[1]:
        data = [x.filter(x["MULTIPLE_OR_SINGLE_AWARD_IDC"] != "MULTIPLE AWARD") for x in data]
    elif award_type[2]:
        try:
            data = filter_data (data, {"AWARD_OR_IDV":["AWARD"]})
        except:
            data = filter_data (data, {"AWARD_IDV_TYPE_DESCRIPTION":
                                       ["DEFINITIVE CONTRACT","DELIVERY ORDER","PURCHASE ORDER","BPA CALL"]})
    return data    
#%%
def get_summary_stats (data, disable_size = False, range = 0):
     
    modzero = [x.filter(x["MODIFICATION_NUMBER"]=="0") for x in data]
 
    if (disable_size) & (range > 0):
        FPDS_gb = modzero[0].with_column("Range", modzero[0]["ULTIMATE_CONTRACT_VALUE"] > range).group_by(["FISCAL_YEAR", "Range"])
    elif disable_size:
        FPDS_gb = modzero[0].group_by(["FISCAL_YEAR"])
    elif (range > 0):
        FPDS_gb = modzero[0].with_column("Range", modzero[0]["ULTIMATE_CONTRACT_VALUE"] > range).group_by(["FISCAL_YEAR", "CO_BUS_SIZE_DETERMINATION", "Range"])
    else:
        FPDS_gb = modzero[0].group_by(["FISCAL_YEAR", "CO_BUS_SIZE_DETERMINATION"])

    FPDS_stats = FPDS_gb.agg(spf.count("ULTIMATE_CONTRACT_VALUE"),
                            spf.sum("ULTIMATE_CONTRACT_VALUE"),
                            spf.max("ULTIMATE_CONTRACT_VALUE"),
                            spf.avg("ULTIMATE_CONTRACT_VALUE"),
                            spf.median("ULTIMATE_CONTRACT_VALUE"),
                            spf.avg("NUMBER_OF_OFFERS_RECEIVED")
                            ).to_pandas()
    FPDS_stats = FPDS_stats.rename(columns = {"FISCAL_YEAR":"FY", "CO_BUS_SIZE_DETERMINATION": "Size", "RANGE":"Range",
             'COUNT(ULTIMATE_CONTRACT_VALUE)': "No. of Contracts Initiated",'SUM(ULTIMATE_CONTRACT_VALUE)': "Aggregate Contract Value"
             , 'MAX(ULTIMATE_CONTRACT_VALUE)':"Max Contract Value",
       'AVG(ULTIMATE_CONTRACT_VALUE)':"Average Contract Value", 'MEDIAN(ULTIMATE_CONTRACT_VALUE)': "Median Contract Value",
       'AVG(NUMBER_OF_OFFERS_RECEIVED)':"Average No. Offers"})
    FPDS_stats["FY"] = pd.to_numeric(FPDS_stats["FY"])
    FPDS_stats = FPDS_stats.loc[(FPDS_stats["FY"] > 2010)].sort_values(["FY"])

    last_FY = int(FPDS_stats["FY"].max())
    atom_pd = modzero[1].filter((col("DATE_SIGNED") > datetime (last_FY, 9, 30, 0, 0))).select(
        ["DATE_SIGNED","CO_BUS_SIZE_DETERMINATION","ULTIMATE_CONTRACT_VALUE", "NUMBER_OF_OFFERS_RECEIVED"]).to_pandas()
    atom_pd["FISCAL_YEAR"] = [x.year if x.month<10 else x.year + 1 for x in atom_pd["DATE_SIGNED"]]

    if (range > 0):
        atom_pd["Range"] = atom_pd["ULTIMATE_CONTRACT_VALUE"] > range

    if (disable_size) & (range > 0):
        atom_gb = atom_pd.groupby(["FISCAL_YEAR", "Range"], as_index=False, dropna=False)
    elif disable_size:
        atom_gb = atom_pd.groupby(["FISCAL_YEAR"], as_index=False, dropna=False)
    elif (range > 0):
        atom_gb = atom_pd.groupby(["FISCAL_YEAR", "CO_BUS_SIZE_DETERMINATION", "Range"], as_index=False, dropna=False)
    else:
        atom_gb = atom_pd.groupby(["FISCAL_YEAR", "CO_BUS_SIZE_DETERMINATION"], as_index=False, dropna=False)

    atom_stats = atom_gb.agg(
        **{"No. of Contracts Initiated": pd.NamedAgg('ULTIMATE_CONTRACT_VALUE', 'count'),
        "Aggregate Contract Value": pd.NamedAgg('ULTIMATE_CONTRACT_VALUE', 'sum'),   
        "Max Contract Value" : pd.NamedAgg('ULTIMATE_CONTRACT_VALUE', 'max'),
        "Average Contract Value" : pd.NamedAgg('ULTIMATE_CONTRACT_VALUE', 'mean'),
        "Median Contract Value" : pd.NamedAgg('ULTIMATE_CONTRACT_VALUE', 'median'),
        "Average No. Offers" : pd.NamedAgg('NUMBER_OF_OFFERS_RECEIVED', 'mean'),
        })
    atom_stats = atom_stats.rename(columns = {"FISCAL_YEAR":"FY", "CO_BUS_SIZE_DETERMINATION": "Size"})

    stats_pd = pd.concat([FPDS_stats, atom_stats])

    if disable_size:
        stats_pd = stats_pd.sort_values("FY").reset_index(drop=True)
    else:
        from pandas.api.types import CategoricalDtype
        cattype = CategoricalDtype(categories=["SMALL BUSINESS","OTHER THAN SMALL BUSINESS"], ordered=True)
        stats_pd['Size'] = stats_pd['Size'].astype(cattype)
        stats_pd = stats_pd.loc[stats_pd["Size"].notnull()].sort_values(["FY", "Size"]).reset_index(drop=True)


    if range:
        stats_pd["Range"] = stats_pd["Range"].map({True: "Above", False:"Below or Equal to"}, na_action='ignore')
        stats_pd = stats_pd.sort_values(["FY","Range", "Size"])
        stats_pd = stats_pd.rename(columns = {"Range": f"Above/Below ${range:,}"})
        
    stats_pd.index += 1

    return stats_pd
#%%
def allow_more_filtering (stats):
    st.sidebar.subheader("More options")
    disable_size = st.sidebar.checkbox("Do not split out size status")

    max_value = stats["Max Contract Value"].max()
    range = st.sidebar.number_input("Split at selected dollar level", min_value=0, max_value=int(max_value), step = 50000)
    
    return disable_size, range
#%%
def graph_and_display_summary_stats (summary_stats):
    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
    options = summary_stats.columns.to_list()[-6:]

    graph = st.selectbox("Metric to graph", options= options)
    
    if summary_stats.columns[1] == "Size":
        fig = px.bar(summary_stats, x="FY", y=graph, color="Size", orientation = 'v', barmode='group', color_discrete_sequence=pal)
    else:
        fig = px.bar(summary_stats, x="FY", y=graph, orientation = 'v', color_discrete_sequence=pal)
    fig.update_xaxes(type='category')

    st.plotly_chart(fig)
    
    st.caption("Contract Value includes Base value plus Options")
    st.caption("Source: SBA Small Business Goaling Report for FY09-FY22; ATOM Feed for later data")

    st.dataframe(summary_stats.style.format(
            {"FY":'{:.0f}',
            "No. of Contracts Initiated": '{:,.0f}',
            "Aggregate Contract Value": '${:,.0f}',   
            "Max Contract Value" : '${:,.0f}',   
            "Average Contract Value" : '${:,.0f}',   
            "Median Contract Value" : '${:,.0f}',  
            "Average No. Offers" : '{:.1f}'
            }, na_rep = "NA")
        )
    st.download_button ("Download this table"
        ,summary_stats.round(2).to_csv(index=False)
        ,file_name="Contract_Initiations_summary.csv"
        )
    
    
#%%
def histogram_and_download_option (data, summary_stats): 
    min_value = int(summary_stats["FY"].min())

    max_value = int(summary_stats["FY"].max())    
    
    year = st.slider("Select year for histogram and download", min_value = min_value, max_value = max_value
        , value = max_value - 1)

    modzero = [x.filter(x["MODIFICATION_NUMBER"]=="0") for x in data]

    vendorcols1 = ["VENDOR_DUNS_NUMBER","VENDOR_NAME","VENDOR_UEI","UEI_NAME"]
    vendorcols2 = ["VENDOR_UEI_NUMBER","VENDOR_NAME"]
    agencycols = ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"FUNDING_OFFICE_NAME"]
    contract_cols= ["AWARD_OR_IDV", "MULTIPLE_OR_SINGLE_AWARD_IDC", 'PIID','IDV_PIID','MODIFICATION_NUMBER','DATE_SIGNED','IDV_TYPE_OF_SET_ASIDE',
                    'TYPE_OF_SET_ASIDE','PRINCIPAL_NAICS_CODE',"PRINCIPAL_NAICS_DESCRIPTION",
                    "PRODUCT_OR_SERVICE_CODE",'PRODUCT_OR_SERVICE_DESCRIPTION', "BUNDLED_CONTRACT_EXCEPTION",
                   "CONSOLIDATED_CONTRACT", "AWARD_IDV_TYPE_DESCRIPTION", "CO_BUS_SIZE_DETERMINATION"]
    dolcols=['NUMBER_OF_OFFERS_RECEIVED',"ULTIMATE_CONTRACT_VALUE", "DOLLARS_OBLIGATED"]

    data_filt = modzero[0].filter(modzero[0]["FISCAL_YEAR"]==str(year))
    data_filt2 = modzero[1].filter((col("DATE_SIGNED") > datetime (year-1, 9, 30, 0, 0)) & (
            (col("DATE_SIGNED") < datetime (year, 10, 1, 0, 0)) ))

    data_df = pd.DataFrame(None)

    if (data_filt.count() > 1000000) | (data_filt2.count() > 1000000):
        count = data_filt.count() + data_filt2.count() 
        st.write(f"Found {count:,} transactions. Filter to 1 million or less to reveal histogram and show download option.")
    elif (data_filt.count() > 0):
        with st.spinner (f"Processing {data_filt.count()} transactions"):
            data_df = data_filt.select(
                vendorcols1 + agencycols + contract_cols + dolcols).to_pandas()
        data_df["VENDOR_NAME"] = data_df["VENDOR_NAME"].fillna(data_df["UEI_NAME"])
        data_df.drop(["UEI_NAME"],axis=1,inplace=True)
    elif (data_filt2.count() > 0):
        if (data_filt.count() <= 1000000):
            with st.spinner (f"Processing {data_filt2.count()} transactions"):
                data_df = modzero[1].select(vendorcols2 + agencycols + contract_cols + dolcols
                    ).to_pandas().rename(columns={"VENDOR_UEI_NUMBER":"VENDOR_UEI"})
    else:
        st.write("No contracts found")

    if len(data_df)>0:
        pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
        histogram = px.histogram(data_df, title = "Distribution of Total Contract Value",
            x="ULTIMATE_CONTRACT_VALUE", labels = {"ULTIMATE_CONTRACT_VALUE": "Total Contract Value"},
            color_discrete_sequence=pal,
            log_y= True, nbins = 50)
        histogram.update_traces (xbins = dict(start = 0))

        st.plotly_chart (histogram)
        st.caption ("Y-axis is logarithmic. Expand graph to see counts.")
        if len(data_df) < 1000000:
            st.download_button ("Download detailed data"
                ,data_df.round(2).to_csv(index=False)
                ,file_name="Contract_Initiations_detailed.csv"
                )
#%%
if __name__ == '__main__':
    st.header(page_title)

    data = connect_and_get_data()
    data = filter_agency (data)
    data = filter_set_aside_type (data)
    data = filter_NAICS (data) 
    data = filter_PSC (data)
    data = filter_bundled_consolidated (data)
    data = filter_award_type (data)

    summary_stats = get_summary_stats (data)
    
    disable_size, range = allow_more_filtering (summary_stats)
    if (disable_size | (range > 0)):
        summary_stats = get_summary_stats (data, disable_size, range)

    graph_and_display_summary_stats (summary_stats)

    histogram_and_download_option (data, summary_stats)

# %%
