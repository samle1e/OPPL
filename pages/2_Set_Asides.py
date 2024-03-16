#%%
import pandas as pd
from snowflake.connector import connect
import streamlit as st
import plotly.express as px
from pyarrow.compute import field
#import duckdb

page_title = "SBA Set-Asides"
top_caption_text = '''This report shows the set asides awarded by Federal Departments and Federal Agencies by Fiscal Year. Options include filtering by Department, Agency, or NAICS code, and viewing the results as a dollar amount, the percentage within the selected set-aside categories of all the dollars to the selected group, or the percentage to 
the selected group of all the dollars awarded within the selected set-aside categories.'''
    
bottom_caption_text = '''Source:  SBA Small Business Goaling Reports.\n  Dollars are scorecard-eligible dollars after applying the exclusions on the [SAM.gov Small Business Goaling Report Appendix](https://sam.gov/reports/awards/standard/F65016DF4F1677AE852B4DACC7465025/view) (login required). This does not reflect adjustments used solely for the SBA scorecard, such as double-credit and Department of Energy subcontracting.'''

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
con = connect(**st.secrets.snowflake_credentials)
cursor = con.cursor()

### table attributes

tb_name = 'STREAMLIT_SET_ASIDES'
linked_cols = {'FUNDING_DEPARTMENT_NAME':'FUNDING_AGENCY_NAME', 'STATE_NAME':'CD'}
dolcols_styled = ['All Awardees',
 'Small Business Concerns',
 'Small Disadvantaged Businesses',
 'Women Owned Small Businesses',
 'HUBZone Small Businesses',
 'Service Disabled Veteran Owned Small Businesses',
 'Tribally Owned Small Businesses',
 'NHO Owned Small Businesses',
 'ANC Owned Small Businesses',
 'Tribally NHO or ANC Owned Small Businesses']
dolcols={col:col.upper().replace(' ','_') for col in dolcols_styled} #needed for snowflake
group_by_col = 'FISCAL_YEAR'
special_treat = 'SET_ASIDE_TYPE'

### Connect to DUCKDB for testing
# duckdb_con = duckdb.connect()
# duckdb_con.execute('''
#     CREATE OR REPLACE TABLE streamlit_set_aside as 
#     (from read_parquet('streamlit_set_aside_test.parquet'))
# ''')
# cursor = duckdb_con.cursor()
    
@st.cache_resource
def get_data (query, params=None):
    if params:
        cursor.execute(query, params)
    else: 
        cursor.execute(query)
    results = cursor.fetch_pandas_all()
    #results = cursor.df()
    return results

@st.cache_data
def get_columns():
    query = "select COLUMN_NAME from information_schema.columns where table_name = %s"
    #query = "select COLUMN_NAME from information_schema.columns where table_name = ?"    
    cols = get_data(query, (tb_name)).squeeze().to_list()
    #cols = get_data(query, [tb_name]).squeeze().to_list()
    return cols

@st.cache_data
def get_filters(cols, linked_cols):
    filters = {}
    for col in cols:
        if col not in list(dolcols.values()) and col != group_by_col:
            if (col not in linked_cols.keys()) and (col not in linked_cols.values()):
                query = f"select distinct {col} from {tb_name}"
                options = get_data(query).squeeze().sort_values().to_list()
                filters[col]=options
            elif col in linked_cols.keys():
                query = f"select distinct {col}, {linked_cols[col]} from {tb_name}"
                options_tbl = get_data(query)
                options_dict = options_tbl.groupby(col)[linked_cols[col]].apply(list).sort_values().to_dict()
                filters[col] = options_dict
    filters = dict(sorted(filters.items()))
    return filters

def special_treatment(filt, filt_options):
    exclude = ['Non-SBA sole source', 'Non-SBA set-aside', 'Not set aside']
    default = [i for i in filt_options if i not in exclude]
    selections = st.sidebar.multiselect(
                filt.replace('_',' ').title(), filt_options, default=default)
    return selections

def filter_sidebar(filters, linked_cols, special_treat):
    st.sidebar.header("Choose Your Filters:")
    selections = {}
    selections[special_treat] = special_treatment(special_treat, filters[special_treat])

    for filt in filters.keys():
        if filt not in linked_cols.keys() and filt not in [group_by_col, special_treat]:
            display = filt.replace('_',' ').title() if filt != 'NAICS' else filt
            selections[filt] = st.sidebar.multiselect(display, filters[filt], default=[]) 
        elif filt in linked_cols.keys():
            selections[filt] = st.sidebar.multiselect(
                filt.replace('_',' ').title(), filters[filt].keys())
            if len(selections[filt]) == 1:
                options=filters[filt][selections[filt][0]]
            else: options=[]
            
            selections[linked_cols[filt]] = st.sidebar.multiselect(
                linked_cols[filt].replace('_',' ').title(), options, disabled = len(options)==0)
    return selections

def select_dollars (dolcols):
    options = list(dolcols.keys())
    dollars = st.radio('Show only awards made to:', options = options)
    return dolcols[dollars]

def select_dollars_or_percent():
    options = ['Dollars', 'Percentage of All Awards to Group Selected Above', 
               'Percentage of Awards to All Awardees']
    dollars_or_percent=st.radio('Display in:', options=options)
    return dollars_or_percent
    
def get_table(cols, selections, dollars, dollars_or_percent):
    cols_small = [col for col in cols if col not in dolcols.values() and col != group_by_col]

    filters = {}
    for col in cols_small:
        if col in selections.keys() and len(selections[col])>0:
            filters[col] = selections[col]
    if len(filters)>0: 
        filters = {k:tuple(v) for k,v in filters.items()}

    if dollars_or_percent == 'Percentage of Awards to All Awardees':
        dolcols_str = f'sum({dollars}) as {dollars}, sum(ALL_AWARDEES) as ALL_AWARDS, sum({dollars})/sum(ALL_AWARDEES) as PERCENT'
    elif dollars_or_percent == 'Percentage of All Awards to Group Selected Above':
        dolcols_str = f'''sum({dollars}) as {dollars}, 
            sum(iff(SET_ASIDE_TYPE in {filters['SET_ASIDE_TYPE']}, {dollars},0)) as {dollars}_with_Set_Aside_Type, 
            sum(iff(SET_ASIDE_TYPE in {filters['SET_ASIDE_TYPE']}, {dollars},0)) / sum({dollars}) as Percent'''
    else:
        dolcols_str = f'sum({dollars}) as {dollars}'
    
    if dollars_or_percent == 'Percentage of All Awards to Group Selected Above':
        where_str = ''.join([f'{k} in (%({k})s) and ' for k,v in filters.items() if k != 'SET_ASIDE_TYPE'])
    else:
        where_str = ''.join([f'{k} in (%({k})s) and ' for k,v in filters.items()])      
    
    query = f"select {group_by_col}, {dolcols_str} from {tb_name} where {where_str} 1=1 group by {group_by_col} order by 1"
   
    table = get_data(query, filters).set_index(group_by_col)
    return table

def display_dollars (table, dollars):
    dollars_srs = table[dollars]
    cols_styled = {v:k for k,v in dolcols.items()}
    cols_styled['FISCAL_YEAR'] = 'Fiscal Year'
    cols_styled['y'] = cols_styled[dollars]

    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

    if ~dollars_srs.empty:
        fig=px.bar(dollars_srs,x=dollars_srs.index,y=dollars_srs,color_discrete_sequence=pal, labels=cols_styled)
        st.plotly_chart(fig)
        st.write(dollars_srs.to_frame().reset_index().rename(columns=cols_styled)\
                   .style.format({cols_styled[dollars]: '${:,.0f}'}).hide(axis="index")\
                   .to_html(),unsafe_allow_html=True)
    else:
        st.write('No Data.')

def display_pct (table, dollars):  
    cols_styled = {v:k for k,v in dolcols.items()}
    cols_styled['FISCAL_YEAR'] = 'Fiscal Year'
    cols_styled['PERCENT'] = 'Percent'
    cols_styled[f'{dollars}_WITH_SET_ASIDE_TYPE'] = f'{cols_styled[dollars]} with Set Aside Type'
    cols_styled['ALL_AWARDS'] = 'All Awards with Set Aside Type'
    pal = ["#002e6d", "#cc0000", "#969696", "#007dbc", "#197e4e", "#f1c400"]

    def col_types(cols):
        dict = {'Percent':'{:.2%}'}
        for col in cols:
            if col=='Fiscal Year': pass
            elif col!='Percent':  dict[col]='${:,.0f}'
        return dict
    
    if len(table) > 0:
        fig=px.line(table,x=table.index,y=table['PERCENT'],color_discrete_sequence=pal, labels = cols_styled)
        st.plotly_chart(fig)
        st.write(table.reset_index().rename(columns=cols_styled)\
             .pipe(lambda df:df.style.format(col_types(df.columns.to_list())))\
             .hide(axis="index").to_html(),unsafe_allow_html=True)
    else:
        st.write('No Data.')

if __name__ == "__main__":
    st.header(page_title)
    st.caption(top_caption_text)
    cols=get_columns()
    filters = get_filters(cols, linked_cols)
    selections = filter_sidebar(filters, linked_cols, special_treat)
    
    dollars = select_dollars (dolcols)
    dollars_or_percent = select_dollars_or_percent()
    
    table = get_table(cols, selections, dollars, dollars_or_percent)
    if dollars_or_percent=='Dollars':
        display_dollars (table, dollars)
    else:
        display_pct (table, dollars)
    st.caption(bottom_caption_text)
