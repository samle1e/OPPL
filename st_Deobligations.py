# %%
#import polars as pl
import pandas as pd
import streamlit as st
import pyarrow.parquet as pq
import pyarrow.compute as pc
import os
os.chdir("C:/Users/SQLe/U.S. Small Business Administration/Office of Policy Planning and Liaison (OPPL) - Data Lake/")


# %%
st.set_page_config(
    page_title="SBA Deobligations Dashboard",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

# %% 
@st.cache_data
def get_data(year_to_run):
#year_to_run="2022"
    PY_pos=pq.ParquetDataset("./SBGR_parquet/SBGR_FY"+str(int(year_to_run)-1),
                    filters=[('TOTAL_SB_ACT_ELIGIBLE_DOLLARS','>',0)])
    CY_pos=pq.ParquetDataset("./Double_Credit/FY_22.parquet"
                    ,filters=[('TOTAL_SB_ACT_ELIGIBLE_DOLLARS','>',0)])
    CY_neg=pq.ParquetDataset("./Double_Credit/FY_22.parquet"
                    ,filters=[('TOTAL_SB_ACT_ELIGIBLE_DOLLARS','<',0)])

    matchcols=["IDV_PIID","PIID","FUNDING_DEPARTMENT_ID"]
    detailcols=["FUNDING_DEPARTMENT_NAME","FUNDING_AGENCY_NAME","FUNDING_AGENCY_ID"]
    dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]
    PYposDF=PY_pos.read(columns=matchcols).to_pandas()
    CYposDF=CY_pos.read(columns=matchcols+detailcols+dolcols).to_pandas()
    CYnegDF=CY_neg.read(columns=matchcols+detailcols+dolcols).to_pandas()

    compareDF=CYposDF[matchcols].merge(PYposDF,how="outer",indicator=True).drop_duplicates()
    compareDF["match"]=compareDF["_merge"].replace({'left_only':'2022+','right_only':'2021+'},)
    compareDF.drop("_merge",axis=1,inplace=True)

    CYnegDF=CYnegDF.merge(compareDF,how="left",on=matchcols,indicator=True,copy=False)

    CYnegDF["STATUS"]=CYnegDF["_merge"].replace({'left_only':'exclude','both':'include'})
    CYnegDF.drop("_merge",axis=1,inplace=True)

    #CYnegDF.to_parquet("CYnegDF.parquet")
    CYpossum=CYposDF.groupby(["FUNDING_DEPARTMENT_NAME","FUNDING_DEPARTMENT_ID","FUNDING_AGENCY_NAME","FUNDING_AGENCY_ID"]
                            ,as_index=False)[dolcols].sum()
    #CYpossum.to_parquet("CYpossum.parquet")
    return CYnegDF, CYpossum
    

# %% 
#User input - year
st.title("Deobligations Dashboard")
year_to_run=st.sidebar.selectbox(label="Fiscal Year",options=(['2022'])) #enable more years here

CY_neg,CY_pos =get_data(year_to_run)

# %% 
#User input - Department - Agency
Department=st.sidebar.selectbox(label="Department",options=('GOV-WIDE'
          ,'AGENCY FOR INTERNATIONAL DEVELOPMENT', 'AGRICULTURE, DEPARTMENT OF', 'COMMERCE, DEPARTMENT OF'
          ,'DEPT OF DEFENSE', 'EDUCATION, DEPARTMENT OF', 'ENERGY, DEPARTMENT OF'
          ,'ENVIRONMENTAL PROTECTION AGENCY', 'GENERAL SERVICES ADMINISTRATION', 'HEALTH AND HUMAN SERVICES, DEPARTMENT OF'
          ,'HOMELAND SECURITY, DEPARTMENT OF', 'HOUSING AND URBAN DEVELOPMENT, DEPARTMENT OF', 'INTERIOR, DEPARTMENT OF THE'
          ,'JUSTICE, DEPARTMENT OF', 'LABOR, DEPARTMENT OF', 'NATIONAL AERONAUTICS AND SPACE ADMINISTRATION'
          ,'NATIONAL SCIENCE FOUNDATION', 'NUCLEAR REGULATORY COMMISSION', 'OFFICE OF PERSONNEL MANAGEMENT'
          ,'SMALL BUSINESS ADMINISTRATION', 'SOCIAL SECURITY ADMINISTRATION', 'STATE, DEPARTMENT OF'
          ,'TRANSPORTATION, DEPARTMENT OF', 'TREASURY, DEPARTMENT OF THE', 'VETERANS AFFAIRS, DEPARTMENT OF'))
if Department != 'GOV-WIDE':
    Agency_select=pd.concat([pd.Series("DEPT-WIDE")
                             ,CY_pos[CY_pos['FUNDING_DEPARTMENT_NAME']==Department][
        "FUNDING_AGENCY_NAME"].drop_duplicates().sort_values()])
    Agency=st.sidebar.selectbox(label="Agency",options=Agency_select)

# %%
#Calculate Tables
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

if Department == 'GOV-WIDE':
    SR_pos=CY_pos[dolcols].sum()
    SR_neg_all=CY_neg[dolcols].sum()
    SR_neg_incl=CY_neg[CY_neg['STATUS']=="include"][dolcols].sum()
elif Agency == 'DEPT-WIDE':
    SR_pos=CY_pos[CY_pos["FUNDING_DEPARTMENT_NAME"]==Department][dolcols].sum()
    SR_neg_all=CY_neg[CY_neg["FUNDING_DEPARTMENT_NAME"]==Department][dolcols].sum()
    SR_neg_incl=CY_neg[CY_neg["FUNDING_DEPARTMENT_NAME"]==Department][CY_neg['STATUS']=="include"][dolcols].sum()
else: 
    SR_pos=CY_pos[(CY_pos["FUNDING_AGENCY_NAME"]==Agency) & (CY_pos["FUNDING_DEPARTMENT_NAME"]==Department)][dolcols].sum()
    SR_neg_all=CY_neg[(CY_neg["FUNDING_AGENCY_NAME"]==Agency) & (CY_neg["FUNDING_DEPARTMENT_NAME"]==Department)][dolcols].sum()
    SR_neg_incl=CY_neg[(CY_neg["FUNDING_AGENCY_NAME"]==Agency) & (CY_neg["FUNDING_DEPARTMENT_NAME"]==Department)][CY_neg['STATUS']=="include"][dolcols].sum()

SR_all=SR_pos + SR_neg_all
SR_DX=SR_pos + SR_neg_incl


# %%
#Add Pct to table
pctcols=dolcols[1:]

def get_pct_DF(SR):
    pct=SR[pctcols].div(SR['TOTAL_SB_ACT_ELIGIBLE_DOLLARS'])
    DF=pd.concat([SR,pct],axis=1)
    DF.columns=["Dollars","Percentage"]
    DF.index=DF.index.str.replace("_DOLLARS|CER_","",regex=True).str.replace(
        "SRDVOB","SDVOSB").str.replace("E_SB","E")
    DF=DF.transpose()
    return DF

DF_all=get_pct_DF(SR_all)
DF_DX=get_pct_DF(SR_DX)


# %%
def styler(DF):
    idx = pd.IndexSlice
    slice1=idx[idx['Dollars'], :]
    slice2=idx[idx['Percentage'], :]
    DF.columns=DF.columns.str.replace("_"," ")
    DF_style=DF.style.format(
        '$ {:,.0f}',subset=slice1).format(
        '{:.2%}',na_rep="",subset=slice2)
    return DF_style

st.write("No Deobligations Excluded (pre-FY22 treatment)")
st.write(styler(DF_all).to_html(),unsafe_allow_html=True)

st.write(" ")
st.write(" ")
st.write("With Deobligations Excluded based on [SBA's June 2022 Federal Register notice](https://www.federalregister.gov/documents/2022/06/22/2022-13287/procurement-scorecard-program-treatment-of-deobligations)")
st.write(styler(DF_DX).to_html(),unsafe_allow_html=True)

#%%
#%%
st.caption("     Department: " + Department)
if Department != 'GOV-WIDE':
    st.caption("     Agency: " + Agency)
    if Agency != 'DEPT-WIDE':download=CY_neg[(CY_neg["FUNDING_AGENCY_NAME"]==Agency) & (CY_neg["FUNDING_DEPARTMENT_NAME"]==Department)]
    else: download=CY_neg[(CY_neg["FUNDING_DEPARTMENT_NAME"]==Department)]

    includeDL=download[download["STATUS"]=="include"].to_csv(index=False)
    excludeDL=download[download["STATUS"]=="exclude"].to_csv(index=False)
    st.download_button(label="Download Included Deobligations"
    ,data=includeDL
    ,file_name=(Department+"-"+Agency+"-included_deobligations.csv"))
    st.download_button(label="Download Excluded Deobligations"
    ,data=excludeDL
    ,file_name=(Department+"-"+Agency+"-excluded_deobligations.csv"))
# %%
