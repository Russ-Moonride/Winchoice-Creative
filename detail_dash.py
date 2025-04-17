import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import service_account
import gspread

st.set_page_config(page_title="Winchoice Creative Report", layout="wide", page_icon="🔬")

scope = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Set up Google Cloud credentials with correct scope
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes = scope
)

# Initialize separate clients
bq_client = bigquery.Client(credentials=credentials)  # BigQuery
gs_client = gspread.authorize(credentials)  # Google Sheets

# Cache the data to avoid reloading on every interaction
@st.cache_data
def load_meta_data():
    query = """
    SELECT *
    FROM `winchoice.winchoice_segments.meta_adlevel`
    """  # Replace with actual table name
    df = bq_client.query(query).to_dataframe()  # Use `bq_client` instead of `client`

    df.rename(columns={"Ad_Name__Facebook_Ads" : "Ad Name", "Ad_Set_Name__Facebook_Ads" : "Ad Set", "Campaign_Name__Facebook_Ads" : "Campaign Name", "Link_Clicks__Facebook_Ads" : "Clicks", "Impressions__Facebook_Ads" : "Impressions", "Amount_Spent__Facebook_Ads" : "Cost", 
                         "n_3_Second_Video_Views__Facebook_Ads" : "3 Sec Views", "Video_Watches_at_100__Facebook_Ads" : "Thruplays", "Leads__Facebook_Ads" : "Leads"}, inplace=True)

    df['Tier'] = df['Campaign Name'].apply(lambda x: 'T1' if 'T1' in x else 'No Tier')

    return df

# Function to filter data based on start and end date
def filter_data(df, start_date, end_date):
    return df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# Function to load data from Google Sheets
@st.cache_data
def load_meta_gsheet_data():
    try:
        # Open the Google Sheet
        spreadsheet_id = "1GwN8pFB9Gkjuq9MJnHX6LxaFUakE9WXF6MKRy23xPQc"  # Replace with your actual ID
        spreadsheet = gs_client.open_by_key(spreadsheet_id)

        # Select the first worksheet (or specify by name)
        var_sheet = spreadsheet.worksheet("Meta_AdName_REF")  
        camp_sheet = spreadsheet.worksheet("Meta_Campaign_Name_REF")
        
        # Get all records
        var_data = pd.DataFrame(var_sheet.get_all_records())
        camp_data = pd.DataFrame(camp_sheet.get_all_records())

        # Convert to DataFrame
        return var_data, camp_data

    except Exception as e:
        st.error(f"Error loading Google Sheets data: {e}")
        return pd.DataFrame()  # Return an empty dataframe on failure

def format_percentage(value):

    if pd.isna(value):  # Handle NaN values
        return "N/A"
    return f"{value:.1%}"  # Converts 0.25 to '25.0%'


def format_dollar(value):

    if pd.isna(value):  # Handle NaN values
        return "N/A"
    return f"${value:,.2f}"  # Converts 1234.56 to '$1,234.56'


# Streamlit app
def main():
    st.title("Winchoice Creative Report")

    st.divider()

    # Load Meta data
    meta_data = load_meta_data()
    meta_ref_data, meta_camp_data = load_meta_gsheet_data()

    merged_data = pd.merge(meta_data, meta_ref_data, on="Ad Name", how="left")
    merged_data = pd.merge(merged_data, meta_camp_data, on="Campaign Name", how="left")
    
    ### **Add Campaign Type filter**
    type_options = ["All"] + sorted(merged_data["Tier"].dropna().astype(str).unique().tolist())
    selected_type = st.selectbox("Select Campaign Type:", type_options, index=0)

    # if selected_type == "Unmapped":
    #     merged_data = merged_data[merged_data["Type"].isna()]
    if selected_type != "All":
        merged_data = merged_data[merged_data["Type"] == selected_type]

    # **Date filters**
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.today() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.today())

    if start_date > end_date:
        st.error("End date must be after start date.")
        return

    # **Apply date filtering**
    filtered_df = filter_data(merged_data, start_date, end_date)

    # **Define all categorical variables**
    all_categorical_vars = [
        "Campaign Name", "Asset", "Asset Name", "Ad Name", "Batch", "Ad Format", "Hook Text", "Supporting Text", "Hook Visuals", "Supporting Visuals", "Concept", "Font Style", "Aesthetic",
        "Background Brightness", "Creative Theme Variable", "Video Duration",
        "Video Duration", "Video Audio: Voice Over", "Video Audio: BG Music", "Video Close Message"
    ]

    # **User selects breakdown order first**
    st.write("### Select Breakdown Variables")
    selected_vars = st.multiselect("Breakdown order:", all_categorical_vars, default=["Ad Name"])

    if selected_vars:
        # **📌 Only show filters for selected variables**
        st.write("### Filter Data")

        num_columns = 5
        num_rows = -(-len(selected_vars) // num_columns)  # Ceiling division

        rows = [st.columns(num_columns) for _ in range(num_rows)]
        filter_values = {}

        for i, var in enumerate(selected_vars):
            row_idx = i // num_columns
            col_idx = i % num_columns
            col = rows[row_idx][col_idx]

            # Get unique values including "All" and "Unmapped"
            unique_values = ["All"] + sorted(filtered_df[var].dropna().astype(str).unique().tolist()) + ["Unmapped"]
            filter_values[var] = col.multiselect(f"Filter by {var}", unique_values, default=["All"])

        # **Apply filters dynamically**
        for var, selected_values in filter_values.items():
            if "All" not in selected_values:
                if "Unmapped" in selected_values:
                    filtered_df = filtered_df[filtered_df[var].isna() | filtered_df[var].isin(selected_values)]
                else:
                    filtered_df = filtered_df[filtered_df[var].isin(selected_values)]

        # **Group data dynamically based on selection**
        grouped_data = filtered_df.groupby(selected_vars).agg({
            "Clicks": "sum", "Impressions": "sum", "Cost": "sum",
            "3 Sec Views": "sum", "Thruplays": "sum", "Leads": "sum"
        }).reset_index()

        # **Generate calculated metrics**
        grouped_data["CTR"] = (grouped_data["Clicks"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["CPC"] = (grouped_data["Cost"] / grouped_data["Clicks"]).apply(format_dollar)
        grouped_data["CPM"] = ((grouped_data["Cost"] / grouped_data["Impressions"]) * 1000).apply(format_dollar)
        grouped_data["3 Sec View Rate"] = (grouped_data["3 Sec Views"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["Vid Complete Rate"] = (grouped_data["Thruplays"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["CPL"] = (grouped_data["Cost"] / grouped_data["Leads"]).apply(format_dollar)
        grouped_data["CVR (Click)"] = (grouped_data["Leads"] / grouped_data["Clicks"]).apply(format_percentage)

        # **Define column order**
        metric_order = [
            "Impressions", "Clicks", "CTR", "Cost", "CPC", "CPM",
            "3 Sec Views", "3 Sec View Rate", "Thruplays", "Vid Complete Rate",
            "Leads", "CPL", "CVR (Click)"
        ]
        grouped_data = grouped_data[selected_vars + metric_order]

        # **Display main results**
        st.write("### Breakdown by Selected Variables")
        st.dataframe(grouped_data, use_container_width=True)

    else:
        st.write("Please select at least one variable to break down by.")

    st.divider()
    # **📌 Additional breakdowns for all categorical variables**
    st.write("### All Variable Breakdowns")

    for var in all_categorical_vars:
        st.write(f"#### Breakdown by {var}")

        single_var_grouped = filtered_df.groupby(var).agg({
            "Clicks": "sum", "Impressions": "sum", "Cost": "sum",
            "3 Sec Views": "sum", "Thruplays": "sum", "Leads": "sum"
        }).reset_index()

        # Generate calculated columns
        single_var_grouped["CTR"] = (single_var_grouped["Clicks"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["CPC"] = (single_var_grouped["Cost"] / single_var_grouped["Clicks"]).apply(format_dollar)
        single_var_grouped["CPM"] = ((single_var_grouped["Cost"] / single_var_grouped["Impressions"]) * 1000).apply(format_dollar)
        single_var_grouped["3 Sec View Rate"] = (single_var_grouped["3 Sec Views"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["Vid Complete Rate"] = (single_var_grouped["Thruplays"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["CPL"] = (single_var_grouped["Cost"] / single_var_grouped["Leads"]).apply(format_dollar)
        single_var_grouped["CVR (Click)"] = (single_var_grouped["Leads"] / single_var_grouped["Clicks"]).apply(format_percentage)

        # Organize column order
        single_var_grouped = single_var_grouped[[var] + metric_order]

        st.dataframe(single_var_grouped, use_container_width=True)
        st.divider()

    st.divider()

if __name__ == "__main__":
    main()
