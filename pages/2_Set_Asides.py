#%%
import pandas as pd
from snowflake.connector import connect
import streamlit as st
import plotly.express as px
from pyarrow.compute import field

page_title= "SBA Set-Aside Tracker"

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
basiccols=['EVALUATED_PREFERENCE','TYPE_OF_SET_ASIDE','IDV_TYPE_OF_SET_ASIDE','FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME']
dolcols=["TOTAL_SB_ACT_ELIGIBLE_DOLLARS","SMALL_BUSINESS_DOLLARS","SDB_DOLLARS","WOSB_DOLLARS","CER_HUBZONE_SB_DOLLARS","SRDVOB_DOLLARS","EIGHT_A_PROCEDURE_DOLLARS"]
entitycols=['INDIAN_TRIBE','TRIBALLY_OWNED','ALASKAN_NATIVE_CORPORATION','AIOB_FLAG','NATIVE_HAWAIIAN_ORGANIZATION']
doublecols=['VENDOR_ADDRESS_STATE_NAME','LOCAL_AREA_SET_ASIDE','CO_BUS_SIZE_DETERMINATION']
contractctcols=['MODIFICATION_NUMBER','AWARD_OR_IDV','MULTIPLE_OR_SINGLE_AWARD_IDC','IDV_MUL_OR_SINGLE_AWARD_IDC','IDV_MUL_OR_SINGLE_COMP','ULTIMATE_CONTRACT_VALUE']

#%%
@st.cache_data
def get_data ():    
    con = connect(**st.secrets['snowflake_credentials'])
    cursor = con.cursor()
    sql_query = f'''select CAST(FISCAL_YEAR as int) FY, 
    {', '.join([f"sum({x})" for x in dolcols])},
      {', '.join(basiccols + entitycols + doublecols)} from
      SMALL_BUSINESS_GOALING
      group by FISCAL_YEAR, {', '.join(basiccols + entitycols + doublecols)}'''
    cursor.execute(sql_query)
    results = cursor.fetch_arrow_all()
    cursor.close()
    return results

#%%
def filter_set_asides (data):
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
            'HZE':'HUBZone Price Evaluation Preference', #not a set-aside, is evaluated_preference
      }
      set_aside_types = list({v:k for (k,v) in set_aside_dict.items()}.keys()) #returns ordered items
      select_set_aside = st.sidebar.multiselect("Select Set-Aside Types", set_aside_types, default = set_aside_types ,key='set_aside')
      set_aside_list = [k for (k,v) in set_aside_dict.items() if v in select_set_aside]
      if set_aside_list:
            data = (data
                  .filter(field('TYPE_OF_SET_ASIDE').isin(set_aside_list) | 
                        field('IDV_TYPE_OF_SET_ASIDE').isin(set_aside_list) |
                        field('EVALUATED_PREFERENCE').isin(set_aside_list))
            )
      return data

def filter_entity_owned (data):
      if st.sidebar.checkbox("Limit to ANC/NHO/Tribal Owned?", key = 'entity'):
          data = (data.filter((field('ALASKAN_NATIVE_CORPORATION') == 'YES') | 
                  (field('ALASKAN_NATIVE_CORPORATION') == 'YES') |
                  (field('INDIAN_TRIBE') == 'YES')|
                  (field('TRIBALLY_OWNED') == 'YES')| 
                  (field('AIOB_FLAG') == 'YES'))
          )
      return data
#%%
def dept_office_list (data):
      groupcols= ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME']
      office_list = (data.group_by(groupcols)
                    .aggregate([("TYPE_OF_SET_ASIDE", "count")])
                    .to_pandas()
                    .sort_values(groupcols))
      return office_list

def filter_department (data):
      office_list = dept_office_list(data)
      #select
      dept_options = office_list['FUNDING_DEPARTMENT_NAME'].drop_duplicates().tolist()
      select_department = st.sidebar.multiselect("Select Departments", dept_options, key='dept')
      
      if (len(select_department)==1):
            agency_options = office_list.loc[office_list['FUNDING_DEPARTMENT_NAME']==select_department[0],'FUNDING_AGENCY_NAME'].drop_duplicates().tolist()
      else:
           agency_options = []
      select_agency = st.sidebar.multiselect("Select Agencies", agency_options,key='agency'
                                             , disabled=(len(select_department)!=1))
      #filter
      if (select_agency): 
            data = data.filter(field('FUNDING_DEPARTMENT_NAME').isin(select_department) & 
                              field('FUNDING_AGENCY_NAME').isin(select_agency))
      elif (select_department):
            data = data.filter(field('FUNDING_DEPARTMENT_NAME').isin(select_department))
      return data

def display_dollars (dollars_data):
      dollars_df = (dollars_data
                    .group_by(["FY"])
                    .aggregate([("SUM(TOTAL_SB_ACT_ELIGIBLE_DOLLARS)", "sum")])
                    .to_pandas()
                    .set_index("FY")
                    .sort_index()
      )
      dollars_df.columns=['Obligated Dollars']
      pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]
      #st.write(dollars_df)
      if ~dollars_df.empty:
            fig=px.bar(dollars_df,x=dollars_df.index,y=dollars_df['Obligated Dollars']
                  ,color_discrete_sequence=pal)
            st.plotly_chart(fig)
            st.write(dollars_df.reset_index().style.format({'Obligated Dollars': '${:,.0f}'}).hide(axis="index").to_html(
                  ),unsafe_allow_html=True)

def select_denominator_and_double (dollars_data, dept_data):   
      dollars_dict={
            "Total Dollars":"TOTAL_SB_ACT_ELIGIBLE_DOLLARS",
            "Small Business Dollars":"SMALL_BUSINESS_DOLLARS",
            "SDB Dollars":"SDB_DOLLARS",
            "WOSB Dollars":"WOSB_DOLLARS",
            "HUBZone Dollars":"CER_HUBZONE_SB_DOLLARS",
            "SDVOSB Dollars":"SRDVOB_DOLLARS",
            "8(a) Procedure Dollars":"EIGHT_A_PROCEDURE_DOLLARS",
      }
      select_dollars = st.sidebar.radio("As a percentage of what (i.e., denominator)?",list(dollars_dict),key = 'denominator')
      
      dollars_sum, dept_sum = (x
                    .group_by(["FY"])
                    .aggregate([(f"SUM({dollars_dict[select_dollars]})", "sum")])
                    .to_pandas()
                    .set_index("FY")
                    .sort_index() for x in (dollars_data, dept_data))

      if st.sidebar.checkbox("Apply Scorecard Double Credit?", key = 'double'):
            territories = ['NORTHERN MARIANA ISLANDS', 'GUAM', 'VIRGIN ISLANDS OF THE U.S.', 'AMERICAN SAMOA', 'PUERTO RICO']
            dollars_adjust, dept_adjust = (x
                    .filter(((field('VENDOR_ADDRESS_STATE_NAME') == 'PUERTO RICO') & (field('FY')==2019)) |
                        ((field('VENDOR_ADDRESS_STATE_NAME').isin(territories)) & (field('FY')>=2020) & (field('FY')<=2022)) |
                        (field('LOCAL_AREA_SET_ASIDE')=="Y") & (field('CO_BUS_SIZE_DETERMINATION')=="SMALL BUSINESS") & (field('FY')>=2020))
                    .group_by(["FY"])
                    .aggregate([(f"SUM({dollars_dict[select_dollars]})", "sum")])
                    .to_pandas()
                    .set_index("FY")
                    .sort_index() for x in (dollars_data, dept_data))
            dollars_sum = dollars_sum.add(dollars_adjust, fill_value = 0)
            if select_dollars !=  "Total Dollars": #we don't double credit for total dollars
                  dept_sum = dept_sum.add(dept_adjust, fill_value = 0)     
      return dollars_sum, dept_sum, select_dollars

def display_percent (dollars_sum, dept_sum, select_dollars):
      df_pct = dollars_sum.join(dept_sum, rsuffix='dept')
      df_pct['pct'] = df_pct.iloc[:,0].div(df_pct.iloc[:,1], fill_value=1)
      df_pct.columns = [f"Set-Aside {select_dollars}","Total",f"% of All {select_dollars}"]
      col1 = df_pct.columns[0]
      try: 
            col2=df_pct.columns[2]
      except: col2 = None
      pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

      if col2:
            fig=px.line(df_pct,x=df_pct.index,y=col2,color_discrete_sequence=pal, labels={'FY':'Fiscal Year'})
            st.plotly_chart(fig)

            st.write(df_pct.loc[:,[col1,col2]].reset_index().style.format({col1: '${:,.0f}',col2: '{:.2%}'}).hide(axis="index").to_html(
             ),unsafe_allow_html=True)

if __name__ == "__main__":
      st.header(page_title)
      data = get_data()
      dept_data = filter_department (data)
      dollars_data = filter_set_asides (dept_data)
      dollars_data = filter_entity_owned (dollars_data)
      DorP=st.sidebar.radio("Dollars or Percentage?",('Dollars','Percentage'), key='dollars')
      if DorP == 'Dollars':
          display_dollars (dollars_data)
      else:
          dollars_sum, dept_sum, select_dollars  = select_denominator_and_double (dollars_data, dept_data) 
          display_percent (dollars_sum, dept_sum, select_dollars)

      st.caption('''
      Source:  SBA Small Business Goaling Reports.\n
      8(a) procedure dollars are a subset of SDB Dollars. SDB Dollars includes 8(a) procedure dollars and other awards to SDBs.\n
      Abbreviations: SDB - Small Disadvantaged Business, WOSB - Women-owned small business, EDWOSB - Economically Disadvantaged women-owned small business, HUBZone - Historically Underutilized Business Zone, SDVOSB - Service-disabled veteran-owned small business
      , ANC - Alaska-Native Corporation, NHO - Native Hawaiian Organization.\n
      Total dollars are total scorecard-eligible dollars after applying the exclusions on the [SAM.gov Small Business Goaling Report Appendix](https://sam.gov/reports/awards/standard/F65016DF4F1677AE852B4DACC7465025/view) (login required). 
      ''')