#%%
import pandas as pd
import polars as pl
import streamlit as st
import plotly.express as px
import pyarrow.dataset as ds
import pyarrow as pa
import os

st.set_page_config(
    page_title="SBA Set-Aside Tracker",
    page_icon="https://www.sba.gov/brand/assets/sba/img/pages/logo/logo.svg",
    layout="wide",
    initial_sidebar_state="expanded",
)

os.chdir("C:/Users/SQLe/Data/")
#%%
#define my datasets
SBGRdir="./SBGR_final/"
list=sorted([file for file in os.listdir(SBGRdir)])
#max_year=list[-1].replace("SBGR_FY","")
#%%
arrowds=ds.dataset(SBGRdir,partitioning=ds.partitioning(pa.schema([("FY", pa.int16())]),flavor="hive"))
pldata=pl.scan_pyarrow_dataset(arrowds)

#%%
SBA_set_asides=["SBA", "8AN", "SDVOSBC" ,"8A", "HZC","SBP","WOSB","SDVOSBS","RSB","HZS","EDWOSB"
,"WOSBSS","ESB","HS3","EDWOSBSS"]
basiccols=["FY",'TYPE_OF_SET_ASIDE','IDV_TYPE_OF_SET_ASIDE','FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME']
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS","EIGHT_A_PROCEDURE_DOLLARS"]
entitycols=['INDIAN_TRIBE','TRIBALLY_OWNED','ALASKAN_NATIVE_CORPORATION','AIOB_FLAG','NATIVE_HAWAIIAN_ORGANIZATION']
doublecols=['VENDOR_ADDRESS_STATE_NAME','LOCAL_AREA_SET_ASIDE','CO_BUS_SIZE_DETERMINATION']
contractctcols=['MODIFICATION_NUMBER','AWARD_OR_IDV','MULTIPLE_OR_SINGLE_AWARD_IDC','IDV_MUL_OR_SINGLE_AWARD_IDC','IDV_MUL_OR_SINGLE_COMP','ULTIMATE_CONTRACT_VALUE']

#%%
all_data=pldata.select(basiccols
		           + dolcols + entitycols + doublecols)

#%%
all_data=all_data.with_columns([pl.when(
	pl.col('VENDOR_ADDRESS_STATE_NAME').str.contains("PUERTO RICO") & (pl.col('FY')==2019))
          .then('PUERTO RICO')
          .when((pl.col('VENDOR_ADDRESS_STATE_NAME').str.contains("PUERTO RICO|GUAM|VIRGIN ISLANDS|SAMOA|MARIANAS"))& (pl.col('FY')>=2020)& (pl.col('FY')<2023))
          .then ('YES') #('TERRITORY')
          .when((pl.col('LOCAL_AREA_SET_ASIDE')=="Y") & (pl.col('CO_BUS_SIZE_DETERMINATION')=="SMALL BUSINESS") & (pl.col('FY')>=2020))
          .then('YES') #('LOCAL')
  	    .otherwise("NO")
          .alias("double")
])
				   
#%%
set_aside_table=all_data.filter(pl.col("TYPE_OF_SET_ASIDE").is_in(SBA_set_asides) | pl.col("IDV_TYPE_OF_SET_ASIDE").is_in(SBA_set_asides)).select(
      basiccols+entitycols+["double"]
		           + dolcols)
#%%
SBA_socio_asides=["8AN", "SDVOSBC" ,"8A", "HZC","WOSB","SDVOSBS","HZS","EDWOSB"
,"WOSBSS","ESB","HS3","EDWOSBSS"]

set_aside_table=set_aside_table.with_columns(
	[pl.when(pl.col('TYPE_OF_SET_ASIDE').is_in(SBA_socio_asides))
          .then(pl.col('TYPE_OF_SET_ASIDE'))
          .when(pl.col('IDV_TYPE_OF_SET_ASIDE').is_in(SBA_set_asides))
          .then(pl.col('IDV_TYPE_OF_SET_ASIDE'))
          .otherwise(pl.col('TYPE_OF_SET_ASIDE'))
          .alias("set_aside")
      ,pl.when(pl.col('NATIVE_HAWAIIAN_ORGANIZATION')=="YES")
	    .then('YES') #('NHO')
          .when(pl.col('ALASKAN_NATIVE_CORPORATION')=="YES")
          .then('YES') #('ANC')
          .when((pl.col('INDIAN_TRIBE')=="YES") | (pl.col('TRIBALLY_OWNED')=="YES") | (pl.col('AIOB_FLAG')=="YES"))
          .then('YES') #('Tribe')
          .otherwise("NO")
          .alias("entity")
          ])
#%%
@st.cache_data
def get_set_aside_data():
      groupcols=["FY",'FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"double"]
      set_aside_sum=set_aside_table.groupby(["set_aside","entity"]+groupcols,maintain_order=True).sum().collect().to_pandas()
      return set_aside_sum

@st.cache_data
def get_all_sum():
      groupcols=["FY",'FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"double"]
      all_sum=all_data.select(basiccols+["double"]+ dolcols).groupby(groupcols,maintain_order=True).sum().collect().to_pandas()
      return all_sum

set_aside_sum=get_set_aside_data()

#%% dictionaries

def dict_to_list(dict):
      types=[]
      types =pd.Series([x for x in dict.values()]).drop_duplicates()
      return types

set_aside_dict={
	"SBA":"Small Business Set-Aside",
	"RSB":"Small Business Set-Aside",
	"ESB":"Small Business Set-Aside",
	"SBP":"Partial SB Set-Aside",
	"8A":"8(a) Competitive",
	"8AN":"8(a) Sole Source",
	"WOSB":"WOSB Set-Aside",
	"WOSBSS":"WOSB Sole Source",
	"EDWOSB":"EDWOSB Set-Aside",
	"EDWOSBSS":"EDWOSB Sole Source",
	"SDVOSBC":"SDVOSB Set-Aside",
	"SDVOSBS":"SDVOSB Sole Source",
	"HS3":"HUBZone Set-Aside",
	"HZC":"HUBZone Set-Aside",
	"HZS":"HUBZone Sole Source",
}
set_aside_types =dict_to_list(set_aside_dict)

dollars_dict={
	"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total Dollars",
	"SMALL_BUSINESS_DOLLARS":"Small Business Dollars",
	"SDB_DOLLARS":"SDB Dollars",
	"WOSB_DOLLARS":"WOSB Dollars",
	"CER_HUBZONE_SB_DOLLARS":"HUBZone Dollars",
	"SRDVOB_DOLLARS":"SDVOSB Dollars",
	"EIGHT_A_PROCEDURE_DOLLARS":"8(a) Dollars",
}
dollars_types =dict_to_list(dollars_dict)

#%%
set_aside_sum.loc[:,'set_aside']=set_aside_sum['set_aside'].replace(set_aside_dict)
set_aside_sum.rename(columns=dollars_dict,inplace=True)

#%%
st.header("SBA Set-Aside Tracker")
select_set_aside=st.sidebar.multiselect("Select Set-Aside Types",set_aside_types)
entity=st.sidebar.checkbox("Limit to entity-owned?")
DorP=st.sidebar.radio("Dollars or Percentage?"
			    ,('Dollars','Percentage'))

select_department=st.sidebar.selectbox(label="Department",options=('GOV-WIDE'
          ,'AGENCY FOR INTERNATIONAL DEVELOPMENT', 'AGRICULTURE, DEPARTMENT OF', 'COMMERCE, DEPARTMENT OF'
          ,'DEPT OF DEFENSE', 'EDUCATION, DEPARTMENT OF', 'ENERGY, DEPARTMENT OF'
          ,'ENVIRONMENTAL PROTECTION AGENCY', 'GENERAL SERVICES ADMINISTRATION', 'HEALTH AND HUMAN SERVICES, DEPARTMENT OF'
          ,'HOMELAND SECURITY, DEPARTMENT OF', 'HOUSING AND URBAN DEVELOPMENT, DEPARTMENT OF', 'INTERIOR, DEPARTMENT OF THE'
          ,'JUSTICE, DEPARTMENT OF', 'LABOR, DEPARTMENT OF', 'NATIONAL AERONAUTICS AND SPACE ADMINISTRATION'
          ,'NATIONAL SCIENCE FOUNDATION', 'NUCLEAR REGULATORY COMMISSION', 'OFFICE OF PERSONNEL MANAGEMENT'
          ,'SMALL BUSINESS ADMINISTRATION', 'SOCIAL SECURITY ADMINISTRATION', 'STATE, DEPARTMENT OF'
          ,'TRANSPORTATION, DEPARTMENT OF', 'TREASURY, DEPARTMENT OF THE', 'VETERANS AFFAIRS, DEPARTMENT OF'))

if select_department != 'GOV-WIDE':
      try:
          Agency_select=pd.concat([pd.Series("DEPT-WIDE")
            ,all_sum[all_sum['FUNDING_DEPARTMENT_NAME']==select_department]["FUNDING_AGENCY_NAME"].drop_duplicates().sort_values()])
      except:
          Agency_select=pd.concat([pd.Series("DEPT-WIDE")
            ,set_aside_sum[set_aside_sum['FUNDING_DEPARTMENT_NAME']==select_department]["FUNDING_AGENCY_NAME"].drop_duplicates().sort_values()])
      Agency=st.sidebar.selectbox(label="Agency",options=Agency_select)
else:
      Agency="DEPT-WIDE"
denom_select="Total Dollars"

if DorP=='Percentage':
      st.sidebar.write("")
      denom_select=st.sidebar.radio("As a percentage of what (i.e., denominator)?",(dollars_types))
      double_credit=st.sidebar.checkbox("Apply Scorecard Double Credit?")

#%%
### For testing
# select_set_aside=["HUBZone Set-Aside","HUBZone Sole Source"]
# entity=False
# DorP='Percentage'
# select_department="GOV-WIDE"
# Agency="DEPT-WIDE"

# %%
#sum for dollars
def filters(sumDF, select_department, Agency):
      #department filter
      if select_department != "GOV-WIDE":
            selectDF=sumDF[sumDF['FUNDING_DEPARTMENT_NAME'] == select_department]
      ###agency filter
            if Agency != "DEPT-WIDE":
                  selectDF=selectDF[selectDF['FUNDING_AGENCY_NAME'] == Agency]
      else: selectDF=sumDF
      return selectDF

#entity filter only for set-aside table
if entity:
      set_aside_sum_select=filters(set_aside_sum[set_aside_sum['entity'] != "NO"], select_department, Agency)
else:       
      set_aside_sum_select=filters(set_aside_sum, select_department, Agency)

#double_credit calculation for either table
def double_creditDF(selectDF,denom_select):
      mask1 = ((selectDF['double'] == "PUERTO RICO") | (selectDF['double'] == "YES"))
      mask2 = (selectDF['double'] == "YES")

      if (denom_select=="Small Business Dollars"):
            selectDF.loc[mask1, [denom_select]] = selectDF.loc[mask1, [denom_select]]*2
      else: 
            selectDF.loc[mask2, [denom_select]] = selectDF.loc[mask2, [denom_select]]*2

      return selectDF
#%%
#double_credit=True
#group and aggregate the result depending on whether we are applying double-credit
try:
      if (double_credit):
            set_aside_sum_select=double_creditDF(set_aside_sum_select,denom_select)
except: 
      pass
#%%
set_aside_sum_select=set_aside_sum_select[set_aside_sum_select['set_aside'].isin(select_set_aside)].groupby(
      ["FY"])[denom_select].sum()
set_aside_sum_select.rename("Set-Aside Dollars",inplace=True)

#%%
##tables
pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

#Calculate the denominator for percentages
if DorP=='Percentage':
      all_sum=get_all_sum()
      all_sum_select=filters(all_sum, select_department, Agency)
      all_sum_select.rename(columns=dollars_dict,inplace=True)
      try: 
            if (double_credit):
                  all_sum_select=double_creditDF(all_sum_select,denom_select)
      except:pass
      all_sum_select=all_sum_select.groupby(["FY"])[denom_select].sum()
      pct=set_aside_sum_select.div(all_sum_select,fill_value=0).multiply(100)
      set_aside_sum_select.rename(f"Set-Aside {denom_select}",inplace=True)
      pct.rename(f"% of {denom_select}",inplace=True)
      displayDF=pd.concat([set_aside_sum_select,pct], axis=1)
else:
      displayDF=set_aside_sum_select.to_frame()

#%% 
#display the table
col1=displayDF.columns[0]
try: 
      col2=displayDF.columns[1]
except: col2=displayDF.columns[0]


if DorP=='Dollars':
      fig=px.bar(displayDF,x=displayDF.index,y=col1
            ,color_discrete_sequence=pal)
else:
      fig=px.line(displayDF,x=displayDF.index,y=col2
            ,color_discrete_sequence=pal)
#%%
def stylerDollars(DF):
    DF_style=DF.style.format({"FY": '{:.0f}', denom_select: '$ {:,.0f}'})
    #.hide()      axis="index")
    return DF_style

st.plotly_chart(fig)

if DorP=='Dollars':
      st.write(displayDF.reset_index().style.format({col1: '${:,.0f}'}).hide_index().to_html(
      ),unsafe_allow_html=True)
else:
      st.write(displayDF.reset_index().style.format({col1: '${:,.0f}',col2: '{:.2f}%'}).hide_index().to_html(
      ),unsafe_allow_html=True)

# %%