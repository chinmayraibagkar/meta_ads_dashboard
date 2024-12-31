import requests
import csv
import time
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import functools
import plotly.express as px
import plotly.graph_objects as go
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import tempfile

# wide mode
st.set_page_config(layout="wide")

if 'uploaded_cred_file' not in st.session_state:
    st.session_state['uploaded_cred_file'] = None

if 'uploaded_json_file' not in st.session_state:
    st.session_state['uploaded_json_file'] = None

uploaded_cred_file = st.sidebar.file_uploader("Upload cred.txt", type=["txt"])
if uploaded_cred_file:
    st.session_state['uploaded_cred_file'] = uploaded_cred_file.read().decode('utf-8').strip()

uploaded_json_file = st.sidebar.file_uploader("Upload Google Sheets API credentials (JSON)", type=["json"])
if uploaded_json_file:
    st.session_state['uploaded_json_file'] = uploaded_json_file
else:
    st.warning("Please upload Service API credentials JSON file to continue.")

if st.session_state['uploaded_cred_file']:
    TOKEN = st.session_state['uploaded_cred_file']
else:
    TOKEN = None
    st.warning("Please upload cred.txt to continue.")

def get_mapping_ref(sheet_id, worksheet_title):
    if not st.session_state['uploaded_json_file']:
        st.warning("Please upload Google Sheets API credentials JSON file to continue.")
        return None

    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    
    # Create a temporary file to store the JSON content
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_json:
        json_content = st.session_state['uploaded_json_file'].getvalue().decode('utf-8')
        temp_json.write(json_content)
        temp_json_path = temp_json.name

    try:
        creds = ServiceAccountCredentials.from_json_keyfile_name(temp_json_path, scope)
        client = gspread.authorize(creds)
        st.success("Google Sheets credentials loaded successfully.")
        
        # Logging the sheet ID and worksheet title
        st.write(f"Attempting to open Google Sheet with ID: {sheet_id} and Worksheet: {worksheet_title}")
        sheet = client.open_by_key(sheet_id)

        # Open the specific worksheet by title
        worksheet = sheet.worksheet(worksheet_title)
        st.success(f"Opened the worksheet: {worksheet_title}")

        existing_data = worksheet.get_all_records()
        existing_df = pd.DataFrame(existing_data)
        return existing_df
    
    except Exception as e:
        st.error(f"Error accessing Google Sheets: {str(e)}")
        return None
    
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_json_path):
            os.unlink(temp_json_path)

@st.cache_data(ttl=86400)  # Cache for 24 hours
def parse_csv_content(csv_content):
    """Parse CSV content with proper handling of quoted fields"""
    try:
        # Split content into lines and filter out empty lines
        lines = [line.strip() for line in csv_content.split('\n') if line.strip()]
        
        if not lines:
            st.error("Received empty CSV content")
            return None
            
        # Use pandas with explicit delimiter and quote characters
        df = pd.read_csv(
            StringIO(csv_content),
            delimiter=',',
            quotechar='"',
            escapechar='\\',
            on_bad_lines='warn'
        )
        
        return df
    except Exception as e:
        st.error(f"Error parsing CSV: {str(e)}")
        return None

@st.cache_data(ttl=86400)  # Cache for 24 hours
def request_report(acnt, date_preset, time1=30):
    """Request the Facebook Ads report and get the report ID"""
    try:
        url = f'https://graph.facebook.com/v16.0/act_{acnt}/insights'
        params = {
            'level': 'ad',
            'fields': 'account_name,ad_name,adset_name,website_purchase_roas,campaign_name,impressions,clicks,spend,actions,reach,frequency',
            'date_preset': date_preset,
            'time_increment': 1,
            'access_token': TOKEN,
            'locale': 'en_US',
            'action_breakdowns': 'action_type'
        }
        
        response = requests.post(url, params=params)
        data = response.json()
        
        if 'error' in data:
            st.error(f"Facebook API Error: {data['error']['message']}")
            return None
            
        if not data.get('report_run_id'):
            st.error("No report_run_id received")
            return None
            
        csv_content = download_report(data.get('report_run_id'), time1)
        
        df = parse_csv_content(csv_content)
        
        if df is not None and not df.empty:
            # Convert numeric columns
            numeric_columns = ['impressions', 'clicks', 'spend', 'reach', 'frequency']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        return None
        
    except Exception as e:
        st.error(f"Error in request_report: {str(e)}")
        return None

@st.cache_data(ttl=86400)  # Cache for 24 hours
def download_report(report_id, time1):
    """Download the report using the report ID"""
    try:
        time.sleep(time1)  # Wait for report generation
        
        url = f'https://www.facebook.com/ads/ads_insights/export_report'
        params = {
            'report_run_id': report_id,
            'format': 'csv',
            'access_token': TOKEN,
            'locale': 'en_US'
        }
        
        response = requests.get(url, params=params)
        if response.status_code != 200:
            st.error(f"Error downloading report: Status {response.status_code}")
            return None
            
        return response.content.decode('utf-8')
        
    except Exception as e:
        st.error(f"Error in download_report: {str(e)}")
        return None

def main():
    st.title("Facebook Ads Dashboard")
    
    if TOKEN and st.session_state['uploaded_json_file']:
        # Add debug mode toggle
        # debug_mode = st.checkbox("Debug Mode")
        
        # Fetch data once and store in session state
        if 'yesterday_data' not in st.session_state:
            with st.spinner('Fetching data for the first time...'):
                # Fetch data for different time periods only once
                # Fetch data for multiple accounts and combine
                accounts = [250361314465016, 2153562184916418, 2197673197282548]
                dfs = []
                for acnt in accounts:
                    df = request_report(acnt, "last_90d", 60)
                    if df is not None:
                        dfs.append(df)
                
                # Combine all dataframes
                st.session_state.last_90d_data = pd.concat(dfs, ignore_index=True)
                
                # Convert Reporting starts column to datetime if not already
                st.session_state.last_90d_data['Reporting starts'] = pd.to_datetime(st.session_state.last_90d_data['Reporting starts'])
                
                # Get yesterday's date and 7 days ago date
                yesterday = datetime.now().date() - timedelta(days=1)
                seven_days_ago = datetime.now().date() - timedelta(days=7)
                one_month_ago = datetime.now().date() - timedelta(days=30)

                # Filter data for yesterday and last 30 days
                st.session_state.last_month_data = st.session_state.last_90d_data[
                    (st.session_state.last_90d_data['Reporting starts'].dt.date > one_month_ago) & 
                    (st.session_state.last_90d_data['Reporting starts'].dt.date <= datetime.now().date())
                ].copy()
                
                # Filter data for yesterday and last 7 days
                st.session_state.yesterday_data = st.session_state.last_month_data[
                    st.session_state.last_month_data['Reporting starts'].dt.date == yesterday
                ].copy()
                
                st.session_state.last_7_days_data = st.session_state.last_month_data[
                    (st.session_state.last_month_data['Reporting starts'].dt.date > seven_days_ago) & 
                    (st.session_state.last_month_data['Reporting starts'].dt.date <= datetime.now().date())
                ].copy()
                st.session_state.mapping_ref = get_mapping_ref("1RxPhCwzVHHU_Nc1APUlpeVc3GaARo4BDXFYY1CDYKu8", "Mapping_ref")
        
            # map the data with the mapping reference
            if all(data is not None for data in [st.session_state.yesterday_data, 
                                            st.session_state.last_7_days_data, 
                                            st.session_state.last_month_data,
                                            st.session_state.mapping_ref]):
                st.session_state.yesterday_data = pd.merge(st.session_state.yesterday_data, st.session_state.mapping_ref, on=['Account name','Campaign name','Ad Set Name','Ad name'], how='left')
                st.session_state.last_7_days_data = pd.merge(st.session_state.last_7_days_data, st.session_state.mapping_ref, on=['Account name','Campaign name','Ad Set Name','Ad name'], how='left')
                st.session_state.last_month_data = pd.merge(st.session_state.last_month_data, st.session_state.mapping_ref, on=['Account name','Campaign name','Ad Set Name','Ad name'], how='left')

        # Debug information
        # if debug_mode:
        #     if st.session_state.yesterday_data is not None:
        #         st.write("Yesterday's Data Shape:", st.session_state.yesterday_data.shape)
        #     if st.session_state.last_7_days_data is not None:
        #         st.write("Weekly Data Shape:", st.session_state.last_7_days_data.shape)
        #     if st.session_state.last_month_data is not None:
        #         st.write("Monthly Data Shape:", st.session_state.last_month_data.shape)
        #     if st.session_state.mapping_ref is not None:
        #         st.write("Mapping Reference Shape:", st.session_state.mapping_ref.shape)
        
        # Display metrics and charts only if we have valid data
        if all(data is not None for data in [st.session_state.yesterday_data, 
                                            st.session_state.last_7_days_data, 
                                            st.session_state.last_month_data,
                                            st.session_state.mapping_ref]):
            
            # change coluumn name 'Amount spent (INR)' to 'spend'
            st.session_state.yesterday_data.rename(columns={'Amount spent (INR)': 'spend'}, inplace=True)
            st.session_state.last_7_days_data.rename(columns={'Amount spent (INR)': 'spend'}, inplace=True)
            st.session_state.last_month_data.rename(columns={'Amount spent (INR)': 'spend'}, inplace=True)

            # Clicks (all) to clicks
            st.session_state.yesterday_data.rename(columns={'Link clicks': 'clicks'}, inplace=True)
            st.session_state.last_7_days_data.rename(columns={'Link clicks': 'clicks'}, inplace=True)
            st.session_state.last_month_data.rename(columns={'Link clicks': 'clicks'}, inplace=True)

            # Reporting Starts to Date
            st.session_state.yesterday_data.rename(columns={'Reporting starts': 'Date'}, inplace=True)
            st.session_state.last_7_days_data.rename(columns={'Reporting starts': 'Date'}, inplace=True)
            st.session_state.last_month_data.rename(columns={'Reporting starts': 'Date'}, inplace=True)

            # Add column "Revenue" by multiplying "Website Purchase ROAS" with "Spend" and rounding
            st.session_state.yesterday_data['Revenue'] = (st.session_state.yesterday_data['Website purchase ROAS (return on ad spend)'] * st.session_state.yesterday_data['spend']).round()
            st.session_state.last_7_days_data['Revenue'] = (st.session_state.last_7_days_data['Website purchase ROAS (return on ad spend)'] * st.session_state.last_7_days_data['spend']).round() 
            st.session_state.last_month_data['Revenue'] = (st.session_state.last_month_data['Website purchase ROAS (return on ad spend)'] * st.session_state.last_month_data['spend']).round()

            # round off spend
            st.session_state.yesterday_data['spend'] = st.session_state.yesterday_data['spend'].round()
            st.session_state.last_7_days_data['spend'] = st.session_state.last_7_days_data['spend'].round()
            st.session_state.last_month_data['spend'] = st.session_state.last_month_data['spend'].round()


            # Display raw data tables
            if st.checkbox("Show Raw Data"):
                st.subheader("Yesterday's Data")
                st.dataframe(st.session_state.yesterday_data)
                
                st.subheader("Last 7 Days Data")
                st.dataframe(st.session_state.last_7_days_data)
                
                st.subheader("Last 30 Days Data")
                st.dataframe(st.session_state.last_month_data)

                st.subheader("Last 90 Days Data")
                st.dataframe(st.session_state.last_90d_data)
            
            # Calculate and display metrics
            try:
                tab1, tab2, tab3 = st.tabs(["Home", "Comparative Views", "Product-Category View"])

                with tab1:
                    try:
                        yesterday_spend = st.session_state.yesterday_data['spend'].sum()
                        weekly_avg_spend = st.session_state.last_7_days_data['spend'].sum() / 7
                        yesterday_clicks = st.session_state.yesterday_data['clicks'].sum()
                        weekly_avg_clicks = st.session_state.last_7_days_data['clicks'].sum() / 7
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.metric(
                            "Yesterday's Spend", 
                            f"₹{int(yesterday_spend):,}", 
                            f"{((yesterday_spend - weekly_avg_spend)/weekly_avg_spend)*100:.1f}%"
                            )
                        with col2:
                            st.metric(
                            "Yesterday's Clicks", 
                            f"{int(yesterday_clicks):,}", 
                            f"{((yesterday_clicks - weekly_avg_clicks)/weekly_avg_clicks)*100:.1f}%"
                            )
                            
                        # Dropdowns for filtering data in 4 columns
                        # First row of filters
                        col1, col2, col3, col4, col5 = st.columns(5)
                        
                        with col1:
                            account_names = st.session_state.last_month_data['Account name'].unique()
                            selected_accounts = st.multiselect("Select Account Name(s)", account_names)

                        with col2:
                            campaign_filter = st.session_state.last_month_data['Account name'].isin(selected_accounts) if selected_accounts else True
                            campaign_names = st.session_state.last_month_data[campaign_filter]['Campaign name'].unique()
                            selected_campaigns = st.multiselect("Select Campaign Name(s)", campaign_names)

                        with col3:
                            adset_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts) if selected_accounts else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns) if selected_campaigns else True)
                            )
                            adset_names = st.session_state.last_month_data[adset_filter]['Ad Set Name'].unique()
                            selected_adsets = st.multiselect("Select Ad Set Name(s)", adset_names)

                        with col4:
                            ad_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts) if selected_accounts else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns) if selected_campaigns else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets) if selected_adsets else True)
                            )
                            ad_names = st.session_state.last_month_data[ad_filter]['Ad name'].unique()
                            selected_ads = st.multiselect("Select Ad Name(s)", ad_names)

                        with col5:
                            creative_types = st.session_state.last_month_data['Creative Type'].unique()
                            selected_creative_types = st.multiselect("Select Creative Type(s)", creative_types)

                        # Second row of filters
                        col6, col7, col8, col9, col10 = st.columns(5)

                        with col6:
                            creative_themes = st.session_state.last_month_data['Creative Theme'].unique()
                            selected_creative_themes = st.multiselect("Select Creative Theme(s)", creative_themes)

                        with col7:
                            product_cats = st.session_state.last_month_data['Product Cat'].unique()
                            selected_product_cats = st.multiselect("Select Product Category(s)", product_cats)

                        with col8:
                            influencer_names = st.session_state.last_month_data['Influencer Name'].unique()
                            selected_influencers = st.multiselect("Select Influencer(s)", influencer_names)

                        with col9:
                            campaign_objectives = st.session_state.last_month_data['Campaign Objective'].unique()
                            selected_objectives = st.multiselect("Select Campaign Objective(s)", campaign_objectives)

                        with col10:
                            st.write("")  # Empty column for alignment

                        # Update the filter conditions for the data
                        filtered_data = st.session_state.last_month_data[
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts) if selected_accounts else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns) if selected_campaigns else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets) if selected_adsets else True) &
                            (st.session_state.last_month_data['Ad name'].isin(selected_ads) if selected_ads else True) &
                            (st.session_state.last_month_data['Creative Type'].isin(selected_creative_types) if selected_creative_types else True) &
                            (st.session_state.last_month_data['Creative Theme'].isin(selected_creative_themes) if selected_creative_themes else True) &
                            (st.session_state.last_month_data['Product Cat'].isin(selected_product_cats) if selected_product_cats else True) &
                            (st.session_state.last_month_data['Influencer Name'].isin(selected_influencers) if selected_influencers else True) &
                            (st.session_state.last_month_data['Campaign Objective'].isin(selected_objectives) if selected_objectives else True)
                        ]

                        # Display monthly performance chart
                        # chart_data = filtered_data.groupby('Date')[['spend', 'clicks']].sum()

                        # # Pie chart of placement's contribution to spend
                        # placement_spend = filtered_data.groupby(["Platform", "Placement"])['spend'].sum()
                        # placement_spend.index = placement_spend.index.map(lambda x: f"{x[0]} - {x[1]}")
                        # sorted_spend = placement_spend.sort_values(ascending=False)
                        # top_7_spend = sorted_spend.head(7)
                        # others_spend = pd.Series({'Others': sorted_spend[7:].sum()})
                        # final_spend = pd.concat([top_7_spend, others_spend])
                        # fig = px.pie(final_spend, 
                        #         values=final_spend.values, 
                        #         names=final_spend.index, 
                        #         title='Placement Contribution to Spend (Top 7 + Others)',
                        #         hole=0.15,
                        #         height=500)
                        # fig.update_traces(textinfo='label+percent+value',
                        #         texttemplate='%{label}<br>%{percent:.1%}<br>₹%{value:,.0f}')
                        # fig.update_layout(
                        #     showlegend=True,
                        #     margin=dict(t=50, l=50, r=50, b=150),
                        #     uniformtext=dict(minsize=10, mode='hide')
                        # )

                        # Group data by Date and calculate aggregated metrics
                        daily_metrics = filtered_data.groupby('Date').agg({
                            'clicks': 'sum',
                            'Impressions': 'sum',
                            'spend': 'sum',
                            'Revenue': 'sum'
                        }).reset_index()
                        daily_metrics['Date'] = pd.to_datetime(daily_metrics['Date']).dt.date

                        # Calculate CTR & CPM on the aggregated data
                        daily_metrics['CTR'] = (daily_metrics['clicks'] / daily_metrics['Impressions']) * 100
                        daily_metrics['CPM'] = (daily_metrics['spend'] / daily_metrics['Impressions']) * 1000
                        daily_metrics['ROAS'] = (daily_metrics['Revenue'] / daily_metrics['spend'])

                        st.subheader("CTR & CPM for Last 30 Days")
                        fig = px.line(daily_metrics, x='Date', y=['CPM', 'CTR'])
                        fig.update_layout(
                            yaxis2=dict(
                            title="CTR (%)",
                            overlaying="y",
                            side="right"
                            ),
                            yaxis=dict(title="CPM (₹)"),
                            title="CPM and CTR Trends"
                        )
                        fig.data[1].update(yaxis="y2")
                        st.plotly_chart(fig)

                        st.subheader("Spend, Revenue & ROAS for Last 30 Days")
                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=daily_metrics['Date'], y=daily_metrics['spend'], name='Spend', marker_color='blue'))
                        fig.add_trace(go.Bar(x=daily_metrics['Date'], y=daily_metrics['Revenue'], name='Revenue', marker_color='violet'))
                        fig.add_trace(go.Scatter(x=daily_metrics['Date'], y=daily_metrics['ROAS'], name='ROAS', yaxis='y2', mode='lines+text', marker_color='yellow',
                                    text=daily_metrics['ROAS'].round(2),
                                    textposition='top center'))
                        fig.update_layout(
                            barmode='group',
                            yaxis=dict(title="Spend and Revenue (₹)"),
                            yaxis2=dict(title="ROAS (%)", overlaying="y", side="right"),
                            title="Spend, Revenue and ROAS Trends",
                            showlegend=True
                        )
                        fig.data[2].update(yaxis="y2")
                        st.plotly_chart(fig)

                        st.subheader("Compare CTR & CPM for 2 Date Ranges")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write("Date Range 1")
                            date1_start = st.date_input("Select Start Date 1", datetime.now() - timedelta(days=30))
                            date1_end = st.date_input("Select End Date 1", datetime.now())
                            
                            date_filter1 = (daily_metrics['Date'] >= date1_start) & (daily_metrics['Date'] <= date1_end)
                            selected_date_metrics1 = daily_metrics[date_filter1]
                            
                            fig1 = px.line(selected_date_metrics1, x='Date', y=['CPM', 'CTR'])
                            fig1.update_layout(
                            yaxis2=dict(title="CTR (%)", overlaying="y", side="right"),
                            yaxis=dict(title="CPM (₹)"),
                            title="Date Range 1: CPM and CTR Trends"
                            )
                            fig1.data[1].update(yaxis="y2")
                            st.plotly_chart(fig1)

                            fig1 = go.Figure()
                            fig1.add_trace(go.Bar(x=selected_date_metrics1['Date'], y=selected_date_metrics1['spend'], name='Spend', marker_color='blue'))
                            fig1.add_trace(go.Bar(x=selected_date_metrics1['Date'], y=selected_date_metrics1['Revenue'], name='Revenue', marker_color='violet'))
                            fig1.add_trace(go.Scatter(x=selected_date_metrics1['Date'], y=selected_date_metrics1['ROAS'], name='ROAS', yaxis='y2', mode='lines+text', marker_color='yellow',
                                    text=selected_date_metrics1['ROAS'].round(2),
                                    textposition='top center'))
                            fig1.update_layout(
                            barmode='group',
                            yaxis=dict(title="Spend and Revenue (₹)"),
                            yaxis2=dict(title="ROAS (%)", overlaying="y", side="right"),
                            title="Date Range 1: Spend, Revenue and ROAS Trends",
                            showlegend=True
                            )
                            fig1.data[2].update(yaxis="y2")
                            st.plotly_chart(fig1)

                        with col2:
                            st.write("Date Range 2")
                            date2_start = st.date_input("Select Start Date 2", datetime.now() - timedelta(days=6))
                            date2_end = st.date_input("Select End Date 2", datetime.now() - timedelta(days=3))
                            
                            date_filter2 = (daily_metrics['Date'] >= date2_start) & (daily_metrics['Date'] <= date2_end)
                            selected_date_metrics2 = daily_metrics[date_filter2]
                            
                            fig2 = px.line(selected_date_metrics2, x='Date', y=['CPM', 'CTR'])
                            fig2.update_layout(
                            yaxis2=dict(title="CTR (%)", overlaying="y", side="right"),
                            yaxis=dict(title="CPM (₹)"),
                            title="Date Range 2: CPM and CTR Trends"
                            )
                            fig2.data[1].update(yaxis="y2")
                            st.plotly_chart(fig2)

                            fig2 = go.Figure()
                            fig2.add_trace(go.Bar(x=selected_date_metrics2['Date'], y=selected_date_metrics2['spend'], name='Spend', marker_color='blue'))
                            fig2.add_trace(go.Bar(x=selected_date_metrics2['Date'], y=selected_date_metrics2['Revenue'], name='Revenue', marker_color='violet'))
                            fig2.add_trace(go.Scatter(x=selected_date_metrics2['Date'], y=selected_date_metrics2['ROAS'], name='ROAS', yaxis='y2', mode='lines+text', marker_color='yellow',
                                    text=selected_date_metrics2['ROAS'].round(2),
                                    textposition='top center'))
                            fig2.update_layout(
                            barmode='group',
                            yaxis=dict(title="Spend and Revenue (₹)"),
                            yaxis2=dict(title="ROAS (%)", overlaying="y", side="right"),
                            title="Date Range 2: Spend, Revenue and ROAS Trends",
                            showlegend=True
                            )
                            fig2.data[2].update(yaxis="y2")
                            st.plotly_chart(fig2)

                    except Exception as e:
                        st.error(f"Error in displaying metrics: {str(e)}")

                with tab2:
                    try:
                        st.subheader("Compare CTR & CPM for 2 Data Selections") 

                        col1, col2 = st.columns(2)

                        with col1:
                            st.write("Data Selection 1")
                            account_names = st.session_state.last_month_data['Account name'].unique()
                            selected_accounts1 = st.multiselect("Select Account Name(s)", account_names, key="accounts1")
                            
                            campaign_filter = st.session_state.last_month_data['Account name'].isin(selected_accounts1) if selected_accounts1 else True
                            campaign_names = st.session_state.last_month_data[campaign_filter]['Campaign name'].unique()
                            selected_campaigns1 = st.multiselect("Select Campaign Name(s)", campaign_names, key="campaigns1")

                            adset_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts1) if selected_accounts1 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns1) if selected_campaigns1 else True)
                            )
                            adset_names = st.session_state.last_month_data[adset_filter]['Ad Set Name'].unique()
                            selected_adsets1 = st.multiselect("Select Ad Set Name(s)", adset_names, key="adsets1")

                            ad_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts1) if selected_accounts1 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns1) if selected_campaigns1 else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets1) if selected_adsets1 else True)
                            )
                            ad_names = st.session_state.last_month_data[ad_filter]['Ad name'].unique()
                            selected_ads1 = st.multiselect("Select Ad Name(s)", ad_names, key="ads1")

                            creative_types = st.session_state.last_month_data['Creative Type'].unique()
                            selected_creative_types1 = st.multiselect("Select Creative Type(s)", creative_types, key="creative_types1")

                            creative_themes = st.session_state.last_month_data['Creative Theme'].unique()
                            selected_creative_themes1 = st.multiselect("Select Creative Theme(s)", creative_themes, key="creative_themes1")

                            product_cats = st.session_state.last_month_data['Product Cat'].unique()
                            selected_product_cats1 = st.multiselect("Select Product Category(s)", product_cats, key="product_cats1")

                            influencer_names = st.session_state.last_month_data['Influencer Name'].unique()
                            selected_influencers1 = st.multiselect("Select Influencer(s)", influencer_names, key="influencers1")

                            campaign_objectives = st.session_state.last_month_data['Campaign Objective'].unique()
                            selected_objectives1 = st.multiselect("Select Campaign Objective(s)", campaign_objectives, key="objectives1")

                        with col2:
                            st.write("Data Selection 2")
                            selected_accounts2 = st.multiselect("Select Account Name(s)", account_names, key="accounts2")
                            
                            campaign_filter = st.session_state.last_month_data['Account name'].isin(selected_accounts2) if selected_accounts2 else True
                            campaign_names = st.session_state.last_month_data[campaign_filter]['Campaign name'].unique()
                            selected_campaigns2 = st.multiselect("Select Campaign Name(s)", campaign_names, key="campaigns2")

                            adset_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts2) if selected_accounts2 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns2) if selected_campaigns2 else True)
                            )
                            adset_names = st.session_state.last_month_data[adset_filter]['Ad Set Name'].unique()
                            selected_adsets2 = st.multiselect("Select Ad Set Name(s)", adset_names, key="adsets2")

                            ad_filter = (
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts2) if selected_accounts2 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns2) if selected_campaigns2 else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets2) if selected_adsets2 else True)
                            )
                            ad_names = st.session_state.last_month_data[ad_filter]['Ad name'].unique()
                            selected_ads2 = st.multiselect("Select Ad Name(s)", ad_names, key="ads2")

                            selected_creative_types2 = st.multiselect("Select Creative Type(s)", creative_types, key="creative_types2")
                            selected_creative_themes2 = st.multiselect("Select Creative Theme(s)", creative_themes, key="creative_themes2")
                            selected_product_cats2 = st.multiselect("Select Product Category(s)", product_cats, key="product_cats2")
                            selected_influencers2 = st.multiselect("Select Influencer(s)", influencer_names, key="influencers2")
                            selected_objectives2 = st.multiselect("Select Campaign Objective(s)", campaign_objectives, key="objectives2")

                        filtered_data1 = st.session_state.last_month_data[
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts1) if selected_accounts1 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns1) if selected_campaigns1 else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets1) if selected_adsets1 else True) &
                            (st.session_state.last_month_data['Ad name'].isin(selected_ads1) if selected_ads1 else True) &
                            (st.session_state.last_month_data['Creative Type'].isin(selected_creative_types1) if selected_creative_types1 else True) &
                            (st.session_state.last_month_data['Creative Theme'].isin(selected_creative_themes1) if selected_creative_themes1 else True) &
                            (st.session_state.last_month_data['Product Cat'].isin(selected_product_cats1) if selected_product_cats1 else True) &
                            (st.session_state.last_month_data['Influencer Name'].isin(selected_influencers1) if selected_influencers1 else True) &
                            (st.session_state.last_month_data['Campaign Objective'].isin(selected_objectives1) if selected_objectives1 else True)
                        ]

                        filtered_data2 = st.session_state.last_month_data[
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts2) if selected_accounts2 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns2) if selected_campaigns2 else True) &
                            (st.session_state.last_month_data['Ad Set Name'].isin(selected_adsets2) if selected_adsets2 else True) &
                            (st.session_state.last_month_data['Ad name'].isin(selected_ads2) if selected_ads2 else True) &
                            (st.session_state.last_month_data['Creative Type'].isin(selected_creative_types2) if selected_creative_types2 else True) &
                            (st.session_state.last_month_data['Creative Theme'].isin(selected_creative_themes2) if selected_creative_themes2 else True) &
                            (st.session_state.last_month_data['Product Cat'].isin(selected_product_cats2) if selected_product_cats2 else True) &
                            (st.session_state.last_month_data['Influencer Name'].isin(selected_influencers2) if selected_influencers2 else True) &
                            (st.session_state.last_month_data['Campaign Objective'].isin(selected_objectives2) if selected_objectives2 else True)
                        ]

                        daily_metrics1 = filtered_data1.groupby('Date').agg({
                            'clicks': 'sum',
                            'Impressions': 'sum',
                            'spend': 'sum',
                            'Revenue': 'sum'
                        }).reset_index()
                        daily_metrics1['CTR'] = (daily_metrics1['clicks'] / daily_metrics1['Impressions']) * 100
                        daily_metrics1['CPM'] = (daily_metrics1['spend'] / daily_metrics1['Impressions']) * 1000
                        daily_metrics1['ROAS'] = (daily_metrics1['Revenue'] / daily_metrics1['spend'])

                        daily_metrics2 = filtered_data2.groupby('Date').agg({
                            'clicks': 'sum',
                            'Impressions': 'sum',
                            'spend': 'sum',
                            'Revenue': 'sum'
                        }).reset_index()
                        daily_metrics2['CTR'] = (daily_metrics2['clicks'] / daily_metrics2['Impressions']) * 100
                        daily_metrics2['CPM'] = (daily_metrics2['spend'] / daily_metrics2['Impressions']) * 1000
                        daily_metrics2['ROAS'] = (daily_metrics2['Revenue'] / daily_metrics2['spend'])

                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=daily_metrics1['Date'], y=daily_metrics1['CPM'],
                                    name='CPM (Selection 1)', line=dict(color='blue')))
                        fig.add_trace(go.Scatter(x=daily_metrics1['Date'], y=daily_metrics1['CTR'],
                                    name='CTR (Selection 1)', line=dict(color='red'), yaxis='y2'))
                        fig.add_trace(go.Scatter(x=daily_metrics2['Date'], y=daily_metrics2['CPM'],
                                    name='CPM (Selection 2)', line=dict(color='lightblue')))
                        fig.add_trace(go.Scatter(x=daily_metrics2['Date'], y=daily_metrics2['CTR'],
                                    name='CTR (Selection 2)', line=dict(color='pink'), yaxis='y2'))
                        fig.update_layout(
                            yaxis=dict(title="CPM (₹)"),
                            yaxis2=dict(title="CTR (%)", overlaying="y", side="right"),
                            title="Comparison of CPM and CTR Trends"
                        )            
                        st.plotly_chart(fig)

                        fig = go.Figure()
                        fig.add_trace(go.Bar(x=daily_metrics1['Date'], y=daily_metrics1['spend'], name='Spend (Selection 1)', marker_color='blue'))
                        fig.add_trace(go.Scatter(x=daily_metrics1['Date'], y=daily_metrics1['ROAS'], name='ROAS (Selection 1)', yaxis='y2', mode='lines+text', marker_color='yellow',
                                text=daily_metrics1['ROAS'].round(2),
                                textposition='top center'))
                        fig.add_trace(go.Bar(x=daily_metrics2['Date'], y=daily_metrics2['spend'], name='Spend (Selection 2)', marker_color='lightblue'))
                        fig.add_trace(go.Scatter(x=daily_metrics2['Date'], y=daily_metrics2['ROAS'], name='ROAS (Selection 2)', yaxis='y2', mode='lines+text', marker_color='pink',
                                text=daily_metrics2['ROAS'].round(2), 
                                textposition='top center'))
                        fig.update_layout(
                            barmode='group',
                            yaxis=dict(title="Spend (₹)"),
                            yaxis2=dict(title="ROAS (%)", overlaying="y", side="right"),
                            title="Comparison of Spend and ROAS Trends",
                            showlegend=True
                        )
                        fig.data[1].update(yaxis="y2")
                        fig.data[3].update(yaxis="y2") 
                        st.plotly_chart(fig)

                        if st.checkbox("Show Data for Selections"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.subheader("Selection 1 Daily Metrics")
                                st.dataframe(daily_metrics1)
                            
                            with col2:
                                st.subheader("Selection 2 Daily Metrics") 
                                st.dataframe(daily_metrics2)
                    except Exception as e:
                        st.error(f"Error in displaying comparative metrics: {str(e)}")

                with tab3:
                    try:
                        st.subheader("Product-Category View")
                        
                        # Add date range filter
                        col_31, col_32 = st.columns(2)
                        with col_31:
                            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=7))
                        with col_32:
                            end_date = st.date_input("End Date", datetime.now())

                        # Here i want to create a multiselect dropdown for Product Category and a single select option for metric, and the data for the same will be displayed in a chart. Multiple lines for each product category as selected
                        # Account selection
                        account_names_1 = st.session_state.last_month_data['Account name'].unique()
                        selected_accounts_1 = st.multiselect("Select Account(s)", account_names_1, key="product_accounts")
                        
                        # Campaign selection
                        campaign_filter_1 = st.session_state.last_month_data['Account name'].isin(selected_accounts_1) if selected_accounts_1 else True
                        campaign_names_1 = st.session_state.last_month_data[campaign_filter_1]['Campaign name'].unique()
                        selected_campaigns_1 = st.multiselect("Select Campaign(s)", campaign_names_1, key="product_campaigns")

                        # Product category selection 
                        product_categories = st.session_state.last_month_data['Product Cat'].unique()
                        selected_product_categories = st.multiselect("Select Product Category(s)", product_categories, key="product_categories")

                        # Metric selection
                        metrics = ['spend','Impressions', 'clicks', 'Revenue', 'ROAS', 'CTR', 'CPM']
                        selected_metric = st.selectbox("Select Metric", metrics)

                        # Filter data based on all selections and date range
                        filtered_data_1 = st.session_state.last_month_data[
                            (st.session_state.last_month_data['Account name'].isin(selected_accounts_1) if selected_accounts_1 else True) &
                            (st.session_state.last_month_data['Campaign name'].isin(selected_campaigns_1) if selected_campaigns_1 else True) &
                            (st.session_state.last_month_data['Product Cat'].isin(selected_product_categories)) &
                            (pd.to_datetime(st.session_state.last_month_data['Date']).dt.date >= start_date) &
                            (pd.to_datetime(st.session_state.last_month_data['Date']).dt.date <= end_date)
                        ]

                        # Group data by Date and Product Category and calculate aggregated metrics
                        product_metrics = filtered_data_1.groupby(['Date', 'Product Cat']).agg({
                            'spend': 'sum',
                            'Impressions': 'sum',
                            'clicks': 'sum',
                            'Revenue': 'sum'
                        }).reset_index()

                        # Calculate CTR & CPM on the aggregated data
                        product_metrics['CTR'] = (product_metrics['clicks'] / product_metrics['Impressions']) * 100
                        product_metrics['CPM'] = (product_metrics['spend'] / product_metrics['Impressions']) * 1000
                        product_metrics['ROAS'] = (product_metrics['Revenue'] / product_metrics['spend'])

                        # Create a line chart for the selected metric
                        fig_1 = px.line(product_metrics, x='Date', y=selected_metric, color='Product Cat', title=f"{selected_metric} Trends by Product Category")
                        st.plotly_chart(fig_1)

                        if st.checkbox("Show Product Category Metrics"):
                            st.subheader("Product Category Metrics")
                            st.dataframe(product_metrics)

                    except Exception as e:
                        st.error(f"Error in displaying Product-Category View: {str(e)}")

            except Exception as e:
                st.error(f"Error calculating metrics: {str(e)}")
        else:
            st.warning("Unable to fetch all required data. Please check your internet connection or API token.")

        # Optional manual refresh button
        if st.button("Refresh Data"):
            # Clear the cached data and session state
            st.cache_data.clear()
            del st.session_state.yesterday_data
            del st.session_state.last_7_days_data
            del st.session_state.last_month_data
            st.experimental_rerun()

if __name__ == "__main__":
    main()
