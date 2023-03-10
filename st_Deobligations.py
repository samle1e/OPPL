# %%
#import polars as pl
import pandas as pd
import streamlit as st
#import pyarrow.dataset as ds

# %%
st.set_page_config(
    page_title="SBA Deobligations Dashboard",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

# %% 
#datasets and functions

@st.cache_data
def get_CY_double(year_to_run):
    # arrowds2=ds.dataset("./Double_Credit/FY"+year_to_run.replace("20","")+"_Double_Credit.parquet",format="parquet")
    # SBGR_DC=pl.scan_ds(arrowds2)
    # CY_double=SBGR_DC.select(
    #     "FUNDING_DEPARTMENT_NAME","FUNDING_AGENCY_NAME"
    #     ,"TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS")
    # CY_double_sum=CY_double.groupby(["FUNDING_DEPARTMENT_NAME","FUNDING_AGENCY_NAME"]).sum()
    # return CY_double_sum.collect().to_pandas()
    return pd.read_parquet("CY_double.parquet")

@st.cache_data
def get_CY_adjust(year_to_run):
    # arrowds=ds.dataset("./SBGR_parquet/SBGR_FY"+str(int(year_to_run)-1),format="parquet")
    # SBGR=pl.scan_ds(arrowds)
    # arrowds2=ds.dataset("./Double_Credit/FY"+year_to_run.replace("20","")+"_Double_Credit.parquet",format="parquet")
    # SBGR_DC=pl.scan_ds(arrowds2)
    # PY_pos=SBGR.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")>0).select(
    #     "IDV_PIID","PIID","FUNDING_DEPARTMENT_ID").unique().sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).fill_null("~")
    # CY_pos=SBGR_DC.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")>0).select(
    #     "IDV_PIID","PIID","FUNDING_DEPARTMENT_ID").unique().sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).fill_null("~")
    # CY_neg=SBGR_DC.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")<0).select(
    #     "IDV_PIID","PIID"
    #     ,"FUNDING_DEPARTMENT_NAME","FUNDING_DEPARTMENT_ID","FUNDING_AGENCY_NAME"
    #     ,"TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"
    #     ).sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).fill_null("~")
    # CY_neg_anti=CY_neg.join(PY_pos,how="anti",on=["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"])
    # CY_neg_anti=CY_neg_anti.join(CY_pos,how="anti",on=["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"])
    # CY_neg_sum=CY_neg_anti.select(pl.col(["FUNDING_DEPARTMENT_NAME","FUNDING_AGENCY_NAME"
    #                                         ,"TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"
    #                                         ])).groupby(["FUNDING_DEPARTMENT_NAME","FUNDING_AGENCY_NAME"]).sum()
    # return CY_neg_sum.collect().to_pandas()
    return pd.read_parquet("CY_adjust.parquet")
    

# %% 
#User input - year
st.title("Deobligations Dashboard")
year_to_run=st.sidebar.selectbox(label="Fiscal Year",options=(['2022'])) #enable more years here

CY_double=get_CY_double(year_to_run)
#CY_double.to_parquet("./Streamlit/Deobligations/CY_double.parquet") #COMMENT OUT
CY_adjust=get_CY_adjust(year_to_run)
#CY_adjust.to_parquet("./Streamlit/Deobligations/CY_adjust.parquet") #COMMENT OUT


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
                             ,CY_double[CY_double['FUNDING_DEPARTMENT_NAME']==Department][
        "FUNDING_AGENCY_NAME"].drop_duplicates().sort_values()])
    Agency=st.sidebar.selectbox(label="Agency",options=Agency_select)

# %%
#Calculate Tables
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"]

if Department == 'GOV-WIDE':
    SR_all=CY_double[dolcols].sum()
    SR_DX=SR_all.subtract(CY_adjust[dolcols].sum())
elif Agency == 'DEPT-WIDE':
    SR_all=CY_double[CY_double["FUNDING_DEPARTMENT_NAME"]==Department][dolcols].sum()
    SR_DX=SR_all.subtract(
    CY_adjust[CY_adjust["FUNDING_DEPARTMENT_NAME"]==Department][dolcols].sum()
    )
else: 
    SR_all=CY_double[(CY_double["FUNDING_AGENCY_NAME"]==Agency) & (CY_double["FUNDING_DEPARTMENT_NAME"]==Department)][dolcols].sum()
    SR_DX=SR_all.subtract(
        CY_adjust[(CY_adjust["FUNDING_AGENCY_NAME"]==Agency) & (CY_adjust["FUNDING_DEPARTMENT_NAME"]==Department)][dolcols].sum()
        )


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
@st.cache_data
def download(Department, Agency,year_to_run):
    download=pd.read_parquet("download.parquet")
    download=download[download["FUNDING_DEPARTMENT_NAME"]==Department]
    if Agency!="DEPT-WIDE":
        download=download[download["FUNDING_AGENCY_NAME"]==Agency]
    return download

    # arrowds=ds.dataset("./SBGR_parquet/SBGR_FY"+str(int(year_to_run)-1),format="parquet")
    # SBGR=pl.scan_ds(arrowds)
    # arrowds2=ds.dataset("./Double_Credit/FY"+year_to_run.replace("20","")+"_Double_Credit.parquet",format="parquet")
    # SBGR_DC=pl.scan_ds(arrowds2)

    # PY_pos=SBGR.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")>0
    #                     #).filter(pl.col("FUNDING_DEPARTMENT_NAME")==Department
    #     ).select(
    #     "IDV_PIID","PIID","FUNDING_DEPARTMENT_ID","FUNDING_AGENCY_NAME").unique().sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"])
    # CY_pos=SBGR_DC.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")>0
    #                         #).filter((pl.col("FUNDING_DEPARTMENT_NAME")==Department)
    #                     ).select(
    #     "IDV_PIID","PIID","FUNDING_DEPARTMENT_ID","FUNDING_AGENCY_NAME").unique().sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"])
    # CY_neg=SBGR_DC.filter(pl.col("TOTAL_SB_ACT_ELIGIBLE_DOLLARS")<0
    #                         #).filter(pl.col("FUNDING_DEPARTMENT_NAME")==Department
    #                     ).select(
    #     "IDV_PIID","PIID","DATE_SIGNED"
    #     ,"FUNDING_DEPARTMENT_NAME","FUNDING_DEPARTMENT_ID","FUNDING_AGENCY_NAME","FUNDING_AGENCY_ID"
    #     ,"TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS"
    #     ).sort(["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"])

    # if Agency!="DEPT-WIDE":
    #     PY_pos=PY_pos.filter(pl.col("FUNDING_AGENCY_NAME")==Agency)
    #     CY_pos=CY_pos.filter(pl.col("FUNDING_AGENCY_NAME")==Agency)
    #     CY_neg=CY_neg.filter(pl.col("FUNDING_AGENCY_NAME")==Agency)
            
    # CY_neg_anti=CY_neg.join(PY_pos,how="anti",on=["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).sort("DATE_SIGNED")
    # CY_neg_anti=CY_neg_anti.join(CY_pos,how="anti",on=["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).sort("DATE_SIGNED").with_columns([pl.lit("Excluded").alias("STATUS")])

    # CY_neg_inner=CY_neg.join(CY_neg_anti,how="anti",on=["FUNDING_DEPARTMENT_ID","IDV_PIID","PIID"]).sort("DATE_SIGNED").with_columns([pl.lit("Included").alias("STATUS")])

    # download=pl.concat([CY_neg_inner,CY_neg_anti],how="vertical") 
    # download.collect().write_parquet("download.parquet") #COMMENT OUT 
    # return download.collect().to_pandas()

#%%
st.caption("     Department: " + Department)
if Department != 'GOV-WIDE':
    st.caption("     Agency: " + Agency)
    if st.checkbox("Show Options to Download Transactions"):
        download=download(Department, Agency,year_to_run)
        includeDL=download[download["STATUS"]=="Included"].to_csv(index=False)
        excludeDL=download[download["STATUS"]=="Excluded"].to_csv(index=False)
        st.download_button(label="Download Included Deobligations"
        ,data=includeDL
        ,file_name=(Department+"-"+Agency+"-included_deobligations.csv"))
        st.download_button(label="Download Excluded Deobligations"
        ,data=excludeDL
        ,file_name=(Department+"-"+Agency+"-excluded_deobligations.csv"))
# %%
