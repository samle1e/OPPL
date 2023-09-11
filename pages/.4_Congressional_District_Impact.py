# -*- coding: utf-8 -*-

# -- Sheet --

import requests
#import json
import pandas as pd
import streamlit as st 
#from streamlit_jupyter import StreamlitPatcher, tqdm
#import os
import snowflake.snowpark as sp
import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.express as px

page_title= "SBA Congressional District Impact"

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

@st.cache_data
def get_members_API (offset = 0):
    maxcount = 1
    members = []
    LIMIT = 250
    apikey = st.secrets.Congress['Congresskey']

    while offset < maxcount:
        url = f'https://api.congress.gov/v3/member?offset={offset}&limit={LIMIT}&api_key={apikey}'
        response = requests.get (url)
        data = response.json()

        df = pd.json_normalize (data['members'])
        #active = 1 if member is active

        if 'member.served.Senate' not in df:
            df['member.served.Senate'] = pd.Series(pd.NA)            

        if 'member.served.House' not in df:
            df['member.served.House'] = pd.Series(pd.NA)
            
        df['member.served.House'] = df['member.served.House'].fillna('').astype('string')
        df['member.served.Senate'] = df['member.served.Senate'].fillna('').astype('string')

        df['House-active'] = df['member.served.House'].str.count('start') - df['member.served.House'].str.count('end') 
        df['Senate-active'] = df['member.served.Senate'].str.count('start') - df['member.served.Senate'].str.count('end') 

        members.append(df.loc[(df['House-active']>0) | (df['Senate-active']>0)])

        maxcount = data['pagination']['count']
        offset += LIMIT
    
    all_members = pd.concat(members)
    all_members['body'] = ['House' if x == 1 else 'Senate' for x in all_members['House-active']]
    all_members.loc[(all_members['body'] == 'House') & (all_members['member.district'].isna()),'member.district'] = 0

    return all_members

def pick_body ():
    body_options = ['House', 'Senate']
    body_pick = st.sidebar.radio("Which body?", body_options)
    return body_pick

@st.cache_data
def state_dist_names (all_members, body_pick):
    state_dist_table = all_members.query('body == @body_pick').loc[:,['member.state','member.district']].drop_duplicates(
        ).sort_values(['member.state','member.district'], na_position='first').reset_index(drop=True)
    names = all_members.loc[all_members['body']==body_pick,'member.name'].sort_values().to_list()
    return state_dist_table, names

def pick_state_district_name (all_members, body_pick):    
    pick_dict = {'body':body_pick}
    state_dist_table, names = state_dist_names (all_members, body_pick)
    name = None

    states = state_dist_table['member.state'].drop_duplicates().to_list()
    state = st.sidebar.selectbox("State", ["No selection"]+states, disabled = (name in names))

    districts = []
    if state in states:
        pick_dict.update({'member.state':state})
        if body_pick == 'House':                
            districts = state_dist_table.loc[state_dist_table['member.state'] == state, 'member.district'].astype(int).to_list()
    district = st.sidebar.selectbox("District", districts , disabled = (body_pick == 'Senate') | (name in names))

    if district in districts:
        pick_dict.update({'member.district':district})

    name = st.sidebar.selectbox ("Name", ["No selection"] + names, disabled = (state in states))

    if name in names:
        pick_dict.update({'member.name':name})

    return pick_dict
    
def pick_members (all_members, pick_dict):
    if len(pick_dict) > 1:
        for k,v in pick_dict.items():
            if k != 'district_text':
                all_members = all_members[all_members[k] == v]
        selected = all_members.reset_index(drop=True)

        pick_dict = selected.loc[0,['member.state','member.district','member.name']
                                 ].squeeze().to_dict()
        if (pick_dict['member.district'] == pick_dict['member.district']) & (pick_dict['member.district'] > 0):
            pick_dict.update({'district_text': f" {int(pick_dict['member.district'])}"})
        else:
            pick_dict.update({'district_text':""})
            
    else:
        selected = pd.DataFrame({'A' : []})
    return selected, pick_dict


# get zip codes for district

@st.cache_data
def get_HUD (url, element):
    token = st.secrets.HUD['HUDkey']
    headers = {"Authorization": "Bearer {0}".format(token)}
    response = requests.get(url, headers = headers)
    zip_data = pd.DataFrame(response.json()["data"]["results"])	
    zip_data = zip_data.rename(columns={"geoid":element})
    return zip_data

@st.cache_data
def get_state_names():
    state_names=pd.read_csv("https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv")
    state_names=state_names.set_index("Abbreviation").squeeze().to_dict()
    terr_dict={"PR":"Puerto Rico","GU":"Guam"
                                    ,"AS":"American Samoa"	
                                    ,"MP":"Northern Mariana Islands"
                                    ,"VI":"Virgin Islands"}
    state_names.update(terr_dict)
    return state_names

@st.cache_data
def get_zip_CD ():
    zip_CD = get_HUD("https://www.huduser.gov/hudapi/public/usps?type=5&query=all","CD")
    zip_CD = zip_CD.sort_values(['bus_ratio'], ascending=False).drop_duplicates('zip', keep='first')
    state_names = get_state_names()
    zip_CD["State"] = zip_CD["state"].map(state_names)
    zip_CD['CDnum'] = zip_CD['CD'].astype('string').str[2:].astype('float')
    return zip_CD

@st.cache_data
def get_shapefile (state):
    zip_CD = get_zip_CD()
    if state == 'American Samoa':
        state_num = 60
    elif state == 'Guam':
        state_num = 66
    elif state == 'Northern Mariana Islands':
        state_num = 69
    else:
        state_num = zip_CD.loc[zip_CD['State'] == state,'CD'].reset_index(drop=True).iat[0][0:2]
    url = f"https://www2.census.gov/geo/tiger/TIGER_RD18/LAYER/CD/tl_rd22_{state_num}_cd118.zip"
    shapefile = gpd.read_file (url)
    return shapefile

def show_CD_plt (selected):
    state = selected.loc[0,'member.state']
    district = selected.loc[0,'member.district']
    
    with st.spinner ("Displaying map"):
        statemap = get_shapefile (state)
    
        fig, ax = plt.subplots (figsize=(2, 2))
        statemap.plot (ax=ax, color = 'lightgray', edgecolor= 'black')
    
        if (district > 0):
            statemap.plot (ax=ax, color = 'lightgray', edgecolor= 'black')
            distmap = statemap[statemap['CD118FP'] == str(int(district)).zfill(2)]
            distmap.plot(ax = ax, color = '#002e6d')
        else:
            statemap.plot (ax=ax, color = '#002e6d', edgecolor= 'black')
        plt.axis('off')
    st.pyplot(plt, use_container_width=False)
    return None

@st.cache_data
def get_image (selectrow):
    url = selectrow['member.depiction.imageUrl']
    return url

@st.cache_data
def get_name_affiliation_website (selectrow):
    bioid = selectrow['member.bioguideId']
    key = st.secrets.Congress['Congresskey']

    url = f'https://api.congress.gov/v3/member/{bioid}?api_key={key}'

    response = requests.get (url)
    data = response.json()
    name = data['member']['directOrderName']
    party = data['member']['partyHistory'][-1]['partyCode']
    state = data['member']['terms'][-1]['stateCode']
    affiliation = f"{party}-{state}"
    website = data['member']['officialWebsiteUrl']
    return [name, affiliation, website]

@st.cache_data
def get_bio (selectrow):
    bioid = selectrow['member.bioguideId']

    url = f"https://bioguide.congress.gov/search/bio/{bioid}.json"

    response = requests.get (url)
    bio = response.json()
    return bio['data']['profileText']

def show_image_name_bio (selected):
    for i, row in selected.iterrows():
        selectrow = row
    
        image = get_image (selectrow)
        name_affiliation_website = get_name_affiliation_website (selectrow)
        name, affiliation, website = name_affiliation_website
         
        bio = get_bio (selectrow)
    
        col1, col2 = st.columns ([1,2])
    
        with col1:
            st.image (image)
        
        with col2:
            st.subheader (f"{name} ({affiliation})")
            st.write(f"[Official Site]({website})")
            st.write (bio)

@st.cache_data
def zip_to_match (pick_dict):
    zip_CD = get_zip_CD()
    state = pick_dict['member.state']
    district = pick_dict['member.district']
    if (district == district) & (district >= 0):
        zip_to_match = zip_CD.loc[(zip_CD['State'] == state) & (zip_CD['CDnum'] == district),'zip'].to_list()
    else: 
        raise
    return zip_to_match


def connect_and_get_data ():
    connection_parameters = st.secrets.snowflake_credentials
    global session
    session = sp.Session.builder.configs(connection_parameters).create()
    data_SBG = session.table("SMALL_BUSINESS_GOALING")
    data_SAM = session.table ("SAM_PUBLIC_MONTHLY_FILTERED")
    return [data_SBG, data_SAM]


def filter_data (data, dict): #dict is the {'column to filter':'items to filter'}
    for k in dict:
         df_for_in = session.create_dataframe(dict[k], schema=["col1"])
         data = data.filter(data[k].isin(df_for_in))
    return data

#need a specific filter by ZIP column to get just the 5-digit zip
def filter_SBG_ZIP (data, ziplist):
    df_for_in = session.create_dataframe(ziplist, schema=["col1"])
    data = data.filter(data["VENDOR_ADDRESS_ZIP_CODE"].substr(1,5).isin(df_for_in))
    return data


#Quick stats:
def global_var():
        global doldict
        global dolcols
        global set_aside_dict
        global basiccols
        global allcols
        global regcols

        doldict={"TOTAL_SB_ACT_ELIGIBLE_DOLLARS":"Total Eligible Dollars",
                "SMALL_BUSINESS_DOLLARS":"Small Business Dollars",
                "SDB_DOLLARS":"SDB Dollars",
                "WOSB_DOLLARS":"Women-Owned Small Bus. Dollars",
                "SRDVOB_DOLLARS": "Serv-Disbld Vet-Owned Small Bus. Dollars",
                "CER_HUBZONE_SB_DOLLARS": "HUBZone Dollars",
                "VOSB_DOLLARS": "Veteran-Owned Small Bus. Dollars",}

        dolcols = [k for k,v in doldict.items()]

        set_aside_dict = {'SBA':'Small Business Set-Asides',
                        "RSB":"Small Business Set-Asides",
                        "ESB":"Small Business Set-Asides",
                        'SBP':'Small Business Set-Asides',
                        '8AN':'8(a) Contracts',
                        '8A':'8(a) Contracts',
                        'HS3':'8(a) Contracts',
                        'HZC':'HUBZone Set-Asides',
                        'HZS':'HUBZone Set-Asides',
                        'SDVOSBS':'Service Disabled Vet-Owned SB Set-Asides',
                        'SDVOSBC':'Service Disabled Vet-Owned SB Set-Asides',
                        'WOSB':'Women-Owned Small Bus. Set-Asides',
                        'WOSBSS':'Women-Owned Small Bus. Set-Asides',
                        'EDWOSB':'Women-Owned Small Bus. Set-Asides',
                        'EDWOSBSS':'Women-Owned Small Bus. Set-Asides'}

        basiccols=["FISCAL_YEAR",'TYPE_OF_SET_ASIDE','IDV_TYPE_OF_SET_ASIDE']

        vendorcols = ["VENDOR_DUNS_NUMBER","VENDOR_NAME","VENDOR_UEI","UEI_NAME"]
        agencycols = ['FUNDING_DEPARTMENT_NAME','FUNDING_AGENCY_NAME',"FUNDING_OFFICE_NAME"]
        contract_cols= ["AWARD_OR_IDV", "MULTIPLE_OR_SINGLE_AWARD_IDC", 'PIID','IDV_PIID','MODIFICATION_NUMBER','DATE_SIGNED',
                        'PRINCIPAL_NAICS_CODE',"PRINCIPAL_NAICS_DESCRIPTION",
                        "PRODUCT_OR_SERVICE_CODE",'PRODUCT_OR_SERVICE_DESCRIPTION', "BUNDLED_CONTRACT_EXCEPTION",
                        "CONSOLIDATED_CONTRACT", "AWARD_IDV_TYPE_DESCRIPTION", "CO_BUS_SIZE_DETERMINATION"]

        regcols=["UNIQUE_ENTITY_ID",
                "CAGE_CODE",
                "SAM_EXTRACT_CODE",
                "PURPOSE_OF_REGISTRATION", 
                "REGISTRATION_EXPIRATION_DATE",
                "LAST_UPDATE_DATE",
                "LEGAL_BUSINESS_NAME",
                "PHYSICAL_ADDRESS_LINE_1",
                "PHYSICAL_ADDRESS_LINE_2",
                "PHYSICAL_ADDRESS_CITY",
                "PHYSICAL_ADDRESS_PROVINCE_OR_STATE", 
                "PHYSICAL_ADDRESS_ZIPPOSTAL_CODE",
                "ENTITY_URL",
                "GOVT_BUS_POC_FIRST_NAME", 
                "GOVT_BUS_POC_LAST_NAME",
                "PRIMARY_NAICS",
                "NAICS_CODE_STRING", 
                "NAICS_EXCEPTION_COUNTER",    
                "NAICS_EXCEPTION_STRING",
                "BUS_TYPE_STRING", 
                "SBA_BUSINESS_TYPES_STRING",
                "PSC_CODE_STRING"
        ]
        allcols = dolcols + basiccols + vendorcols + agencycols + contract_cols
        return None

def SBdolstats (data_SBG):
    snowparkdict = {f"SUM({k})":v for k,v in doldict.items()}
    SBdolstats = data_SBG.groupBy("FISCAL_YEAR").sum(*dolcols).to_pandas().round(2).sort_values('FISCAL_YEAR'
                ).rename (columns = snowparkdict).set_index('FISCAL_YEAR')
    return SBdolstats

def set_aside_SBA (data_SBG):
    setasidestats = data_SBG.filter(data_SBG['TYPE_OF_SET_ASIDE'].is_not_null() | data_SBG['IDV_TYPE_OF_SET_ASIDE'].is_not_null()
                        ).groupBy(*basiccols).sum("TOTAL_SB_ACT_ELIGIBLE_DOLLARS").to_pandas().round(2).sort_values('FISCAL_YEAR')

    SBA_setasides = [k for k in set_aside_dict]
    SES_setasides = SBA_setasides[4:]

    setasidestats['setaside'] = [x if x in SES_setasides else y if y in SBA_setasides else x for x,y in zip(setasidestats['TYPE_OF_SET_ASIDE'], setasidestats['IDV_TYPE_OF_SET_ASIDE'])]
    setasidestats['setaside'] = setasidestats['setaside'].map(set_aside_dict)
    set_aside_SBA = setasidestats.groupby(['FISCAL_YEAR','setaside'],as_index=False).sum().fillna(0).round(2)
    try:
        set_aside_SBA = pd.pivot_table(set_aside_SBA, values = 'SUM(TOTAL_SB_ACT_ELIGIBLE_DOLLARS)', columns = 'setaside', index = 'FISCAL_YEAR')
    except:
        set_aside_SBA = pd.DataFrame(None)
    set_aside_SBA['Total SBA Set-Asides'] = set_aside_SBA.sum(axis = 1)   

    req_cols = ['Total SBA Set-Asides','Small Business Set-Asides','8(a) Contracts','Women-Owned Small Bus. Set-Asides','Service Disabled Vet-Owned SB Set-Asides','HUBZone Set-Asides']
    set_aside_SBA = set_aside_SBA.reindex(labels=req_cols, axis=1)
    set_aside_SBA = set_aside_SBA.loc[:,req_cols]
    return set_aside_SBA

#registrants

def dist_SAM (data_SAM):
    dist_SAM = data_SAM.select(regcols).to_pandas().sort_values("LEGAL_BUSINESS_NAME").reset_index(drop=True)
    dist_SAM_filt = dist_SAM.loc[dist_SAM['PURPOSE_OF_REGISTRATION'].str.contains("Z2|Z5"
                    ) & ~dist_SAM['SBA_BUSINESS_TYPES_STRING'].str.contains("CY|12|2F|2R|3I|OH|A7|2U|A8|1D"
                    )].loc[dist_SAM['NAICS_CODE_STRING'].str.contains("Y") | dist_SAM['NAICS_EXCEPTION_STRING'].str.contains("Y")].drop_duplicates("UNIQUE_ENTITY_ID")
    dist_SAM_filt['SDB'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("27")
    dist_SAM_filt['8(A)'] = dist_SAM_filt['SBA_BUSINESS_TYPES_STRING'].str.contains("A6|JT")
    dist_SAM_filt['WOSB (self-cert)'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("8W")
    dist_SAM_filt['SDVOSB (self-cert)'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("QF")
    dist_SAM_filt['VOSB (self-cert)'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("A5")
    dist_SAM_filt['HUBZone'] = dist_SAM_filt['SBA_BUSINESS_TYPES_STRING'].str.contains("XX")
    dist_SAM_filt['Minority Owned'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("23")
    dist_SAM_filt['Black American Owned'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("OY")
    dist_SAM_filt['Hispanic American Owned'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("PI")
    dist_SAM_filt['Native American Owned'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("NB")
    dist_SAM_filt['Asian-Pacific or Asian-Indian American Owned'] = dist_SAM_filt['BUS_TYPE_STRING'].str.contains("FR|QZ")
    return dist_SAM_filt

def show_quick_stats (SBdolstats, set_aside_SBA, dist_SAM):
    try:
        three_yr_SBdol = SBdolstats.drop(['Total Eligible Dollars'],axis=1).fillna(0).iloc[-3:].mean().round(2
                            ).to_frame("Business Type Dollars Obligated")
        three_yr_setaside = set_aside_SBA.fillna(0).iloc[-3:].mean().round(2).to_frame("Set Aside Dollars Obligated")
    except:
        three_yr_SBdol = pd.DataFrame(None)
        three_yr_setaside = pd.DataFrame(None)
    counts = pd.concat([dist_SAM.count().iloc[[0]], dist_SAM.iloc[:,-10:].sum()]).rename(
        {"UNIQUE_ENTITY_ID":"Registered Small Businesses"})
    counts.name = "Counts"
    col1, col2 = st.columns ([1,1])
    with col1:
        st.subheader("3-year Average Business Type Dollars")
        st.table(three_yr_SBdol.style.format('${:,.0f}'))

        st.subheader("3-year Average Set-Aside Dollars")
        st.table(three_yr_setaside.style.format('${:,.0f}'))

    with col2:
        st.subheader("Registered Small Businesses")
        st.table(counts)
        st.caption ("Registration in SAM.gov as of current month")
    return None

def fig_All_stats (SBdolstats, set_aside_SBA, pick_dict):
    state = pick_dict['member.state']
    district = pick_dict['district_text']
    All_stats = SBdolstats.drop(['Total Eligible Dollars'],axis=1).join(set_aside_SBA)
    
    #rearrange columns
    try:
        last_row = All_stats.iloc[-1].sort_values(ascending=False)
        sort_order = last_row.index.to_list()
        All_stats = All_stats.loc[:,sort_order]

        fig=px.line(All_stats,x=All_stats.index,y=All_stats.columns
            ,color_discrete_sequence=px.colors.qualitative.Dark24, markers=True
            ,title = f"SBA Dollars in {state}{district}"  ,
            labels = {"variable":"","value":"dollars","FISCAL_YEAR":"Year"})
    except:
        fig = px.line(None)
 
    st.plotly_chart(fig, use_container_width = True)
    return None

def show_tables_and_downloads (SBdolstats, set_aside_SBA, dist_SAM, pick_dict):
    state = pick_dict['member.state']
    district = pick_dict['district_text']

    All_stats = SBdolstats.drop(['Total Eligible Dollars'],axis=1).join(set_aside_SBA)
    All_stats.index.rename ("FY",inplace=True)

    st.subheader ("SBA Dollars by Fiscal Year")
    st.table (All_stats.style.format('${:,.0f}',na_rep="$0"))
    st.download_button('Download Dollars', All_stats.to_csv(), file_name = f"SBA_Doll_{state}{district}.csv")

    st.subheader ("Current Registered Small Businesses")
    st.dataframe (dist_SAM.reset_index(drop=True))
    st.download_button('Download registrants list', dist_SAM.to_csv(), file_name = f"Registrants in {state}{district}.csv")
    return None

if __name__ == '__main__':

    all_members = get_members_API()

    body_pick = pick_body()
    state_district_name = state_dist_names (all_members, body_pick)
    pick_dict = pick_state_district_name (all_members, body_pick)
    selected, pick_dict = pick_members (all_members, pick_dict)
    
    if (selected.empty):
        st.header (page_title)
        st.write ("Select a Congressional Body, and a State or Member Name")
    else:        
        st.header (f"{page_title} for {pick_dict['member.state']}{pick_dict['district_text']}")
        show_CD_plt (selected)
        show_image_name_bio (selected)
        global_var()
        SBG_SAM = connect_and_get_data()

        if (body_pick == 'House') & (pick_dict['member.district'] > 0):
            zip_to_match = zip_to_match (pick_dict)
            data_SBG = filter_SBG_ZIP (SBG_SAM[0], zip_to_match)
            data_SAM = filter_data (SBG_SAM[1], {"PHYSICAL_ADDRESS_ZIPPOSTAL_CODE":zip_to_match})
        else:
            rev_state_names = {v:k for k,v in get_state_names().items()}
            state_abbr = rev_state_names[pick_dict['member.state']]
            data_SBG = SBG_SAM[0].filter(SBG_SAM[0]["VENDOR_ADDRESS_STATE_NAME"] == pick_dict['member.state'].upper())
            data_SAM = SBG_SAM[1].filter(SBG_SAM[1]["PHYSICAL_ADDRESS_PROVINCE_OR_STATE"] == state_abbr)
     
        SBdolstats = SBdolstats (data_SBG)
        set_aside_SBA = set_aside_SBA (data_SBG)
        dist_SAM = dist_SAM (data_SAM)
           
        show_quick_stats (SBdolstats, set_aside_SBA, dist_SAM)
        fig_All_stats (SBdolstats, set_aside_SBA, pick_dict)
        show_tables_and_downloads (SBdolstats, set_aside_SBA, dist_SAM, pick_dict)
