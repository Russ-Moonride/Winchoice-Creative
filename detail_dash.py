import streamlit as st
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta
from google.oauth2 import service_account
import plotly.express as px
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

    df['Tier'] = df['Campaign Name'].apply(lambda x: 'T1' if 'T1' in x else ('T2' if 'T2' in x else 'No Tier'))

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


def show_ad_insights_section(filtered_df):
    st.subheader("📊 Ad-Level Insights Explorer")

    st.markdown("Use the filters below to select a group of ads and an optional comparison group.")

    col1, col2 = st.columns(2)

    with col1:
        primary_search = st.text_input("Search Ad Names (Group A)", "")
        all_ads = sorted(filtered_df["Ad Name"].dropna().astype(str).unique())
        filtered_ads = [ad for ad in all_ads if primary_search.lower() in ad.lower()]
        selected_ads = st.multiselect("Select Ads (Group A)", options=filtered_ads, default=filtered_ads[:5])

    with col2:
        compare_enabled = st.checkbox("Enable Comparison Group (Group B)")
        if compare_enabled:
            compare_search = st.text_input("Search Ad Names (Group B)", "")
            compare_ads = [ad for ad in all_ads if compare_search.lower() in ad.lower()]
            selected_compare_ads = st.multiselect("Select Ads (Group B)", options=compare_ads, default=compare_ads[:5])
        else:
            selected_compare_ads = []

    if not selected_ads and not selected_compare_ads:
        st.info("Select at least one ad to display insights.")
        return

    # Metric and dimension selection
    numeric_cols = [
        "Impressions", "Clicks", "Cost", "3 Sec Views", "Thruplays", "Leads",
        "CTR", "CPC", "CPM", "3 Sec View Rate", "Vid Complete Rate", "CPL", "CVR (Click)"
    ]
    dimension_cols = [
        "Ad Name", "Campaign Name", "Asset Name", "Batch", "Ad Format", "Concept"
    ]

    col3, col4 = st.columns(2)
    with col3:
        selected_metric = st.selectbox("Metric to Display", numeric_cols)
    with col4:
        selected_dimension = st.selectbox("Group By Dimension", dimension_cols)

    # Build group A dataframe
    df_a = filtered_df[filtered_df["Ad Name"].isin(selected_ads)].copy()
    df_a["Group"] = "Group A"

    # Build group B dataframe (if enabled)
    if selected_compare_ads:
        df_b = filtered_df[filtered_df["Ad Name"].isin(selected_compare_ads)].copy()
        df_b["Group"] = "Group B"
        plot_df = pd.concat([df_a, df_b])
        # Recalculate derived metrics in plot_df
        plot_df["CTR"] = plot_df["Clicks"] / plot_df["Impressions"]
        plot_df["CPC"] = plot_df["Cost"] / plot_df["Clicks"]
        plot_df["CPM"] = (plot_df["Cost"] / plot_df["Impressions"]) * 1000
        plot_df["3 Sec View Rate"] = plot_df["3 Sec Views"] / plot_df["Impressions"]
        plot_df["Vid Complete Rate"] = plot_df["Thruplays"] / plot_df["Impressions"]
        plot_df["CPL"] = plot_df["Cost"] / plot_df["Leads"]
        plot_df["CVR (Click)"] = plot_df["Leads"] / plot_df["Clicks"]

    else:
        plot_df = df_a
        plot_df["CTR"] = plot_df["Clicks"] / plot_df["Impressions"]
        plot_df["CPC"] = plot_df["Cost"] / plot_df["Clicks"]
        plot_df["CPM"] = (plot_df["Cost"] / plot_df["Impressions"]) * 1000
        plot_df["3 Sec View Rate"] = plot_df["3 Sec Views"] / plot_df["Impressions"]
        plot_df["Vid Complete Rate"] = plot_df["Thruplays"] / plot_df["Impressions"]
        plot_df["CPL"] = plot_df["Cost"] / plot_df["Leads"]
        plot_df["CVR (Click)"] = plot_df["Leads"] / plot_df["Clicks"]

    # Handle missing columns
    if selected_metric not in plot_df.columns or selected_dimension not in plot_df.columns:
        st.warning("Selected metric or dimension not found in data.")
        return

    # Group by dimension and group
    agg_df = (
        plot_df.groupby([selected_dimension, "Group"])[
            ["Clicks", "Impressions", "Cost", "3 Sec Views", "Thruplays", "Leads"]
        ]
        .sum()
        .reset_index()
    )
    
    # Recalculate derived metrics post-aggregation
    agg_df["CTR"] = agg_df["Clicks"] / agg_df["Impressions"]
    agg_df["CPC"] = agg_df["Cost"] / agg_df["Clicks"]
    agg_df["CPM"] = (agg_df["Cost"] / agg_df["Impressions"]) * 1000
    agg_df["3 Sec View Rate"] = agg_df["3 Sec Views"] / agg_df["Impressions"]
    agg_df["Vid Complete Rate"] = agg_df["Thruplays"] / agg_df["Impressions"]
    agg_df["CPL"] = agg_df["Cost"] / agg_df["Leads"]
    agg_df["CVR (Click)"] = agg_df["Leads"] / agg_df["Clicks"]
    
    # Final chart data
    chart_df = agg_df[[selected_dimension, "Group", selected_metric]]


    # Plot
    fig = px.bar(
        chart_df,
        x=selected_dimension,
        y=selected_metric,
        color="Group",
        barmode="group",
        title=f"{selected_metric} by {selected_dimension} (Grouped by A/B)"
    )
    fig.update_layout(xaxis_title=selected_dimension, yaxis_title=selected_metric)
    st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("Winchoice Creative Report")

    st.divider()

    # Load Meta data
    meta_data = load_meta_data()
    meta_ref_data, meta_camp_data = load_meta_gsheet_data()

    merged_data = pd.merge(meta_data, meta_ref_data, on="Ad Name", how="left")
    merged_data = pd.merge(merged_data, meta_camp_data, on="Campaign Name", how="left")
    
    # Campaign Type filter
    type_options = ["All"] + sorted(merged_data["Tier"].dropna().astype(str).unique().tolist())
    selected_type = st.selectbox("Select Campaign Type:", type_options, index=0)

    if selected_type != "All":
        merged_data = merged_data[merged_data["Tier"] == selected_type]

    # Date filters
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", datetime.today() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", datetime.today())

    if start_date > end_date:
        st.error("End date must be after start date.")
        return

    # Apply date filtering
    filtered_df = filter_data(merged_data, start_date, end_date)

    # Categorical variables
    all_categorical_vars = [
        "Campaign Name", "Asset", "Asset Name", "Ad Name", "Batch", "Ad Format", "Hook Text", "Supporting Text", "Hook Visuals", "Supporting Visuals", "Concept", "Font Style", "Aesthetic",
        "Background Brightness", "Creative Theme Variable", "Video Duration",
        "Video Audio: Voice Over", "Video Audio: BG Music", "Video Close Message", "Tier"
    ]

    # Select breakdown and metrics side by side
    breakdown_col, metric_col = st.columns(2)

    with breakdown_col:
        st.write("### Select Breakdown Variables")
        selected_vars = st.multiselect("Breakdown order:", all_categorical_vars, default=["Ad Name"])

    with metric_col:
        all_metrics = [
            "Impressions", "Clicks", "CTR", "Cost", "CPC", "CPM",
            "3 Sec Views", "3 Sec View Rate", "Thruplays", "Vid Complete Rate",
            "Leads", "CPL", "CVR (Click)"
        ]
        metric_options = ["All"] + all_metrics
        st.write("### Select Metrics to Display")
        selected_metrics = st.multiselect(
            "Choose metrics to display:",
            options=metric_options,
            default=["All"],
            key="metric_selector"
        )
        final_metrics = all_metrics if "All" in selected_metrics else selected_metrics

    if selected_vars:
        st.write("### Filter Data")

        num_columns = 5
        num_rows = -(-len(selected_vars) // num_columns)
        rows = [st.columns(num_columns) for _ in range(num_rows)]
        filter_values = {}

        for i, var in enumerate(selected_vars):
            row_idx = i // num_columns
            col_idx = i % num_columns
            col = rows[row_idx][col_idx]

            contains_filter = col.text_input(f"Search {var}", value="", key=f"contains_{var}").strip().lower()

            # Apply contains filter directly to the data
            if contains_filter:
                filtered_df = filtered_df[
                    filtered_df[var].astype(str).str.lower().str.contains(contains_filter)
                ]

            # After contains filter, build dropdown values from what's left
            dropdown_options = filtered_df[var].dropna().astype(str).unique().tolist()
            dropdown_vals = ["All"] + sorted(dropdown_options) + ["Unmapped"]
            filter_values[var] = col.multiselect(f"Filter by {var}", dropdown_vals, default=["All"], key=f"dropdown_{var}")


        for var, selected_values in filter_values.items():
            if "All" not in selected_values:
                if "Unmapped" in selected_values:
                    filtered_df = filtered_df[filtered_df[var].isna() | filtered_df[var].isin(selected_values)]
                else:
                    filtered_df = filtered_df[filtered_df[var].isin(selected_values)]

        # Group data
        grouped_data = filtered_df.groupby(selected_vars).agg({
            "Clicks": "sum", "Impressions": "sum", "Cost": "sum",
            "3 Sec Views": "sum", "Thruplays": "sum", "Leads": "sum"
        }).reset_index()

        grouped_data["CTR"] = (grouped_data["Clicks"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["CPC"] = (grouped_data["Cost"] / grouped_data["Clicks"]).apply(format_dollar)
        grouped_data["CPM"] = ((grouped_data["Cost"] / grouped_data["Impressions"]) * 1000).apply(format_dollar)
        grouped_data["3 Sec View Rate"] = (grouped_data["3 Sec Views"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["Vid Complete Rate"] = (grouped_data["Thruplays"] / grouped_data["Impressions"]).apply(format_percentage)
        grouped_data["CPL"] = (grouped_data["Cost"] / grouped_data["Leads"]).apply(format_dollar)
        grouped_data["CVR (Click)"] = (grouped_data["Leads"] / grouped_data["Clicks"]).apply(format_percentage)

        grouped_data = grouped_data[selected_vars + final_metrics]

        st.write("### Breakdown by Selected Variables")
        st.dataframe(grouped_data, use_container_width=True)

    else:
        st.write("Please select at least one variable to break down by.")

    st.divider()

    show_ad_insights_section(filtered_df)
    st.divider()
    
    # Additional breakdowns
    st.write("### All Variable Breakdowns")

    for var in all_categorical_vars:
        st.write(f"#### Breakdown by {var}")

        single_var_grouped = filtered_df.groupby(var).agg({
            "Clicks": "sum", "Impressions": "sum", "Cost": "sum",
            "3 Sec Views": "sum", "Thruplays": "sum", "Leads": "sum"
        }).reset_index()

        single_var_grouped["CTR"] = (single_var_grouped["Clicks"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["CPC"] = (single_var_grouped["Cost"] / single_var_grouped["Clicks"]).apply(format_dollar)
        single_var_grouped["CPM"] = ((single_var_grouped["Cost"] / single_var_grouped["Impressions"]) * 1000).apply(format_dollar)
        single_var_grouped["3 Sec View Rate"] = (single_var_grouped["3 Sec Views"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["Vid Complete Rate"] = (single_var_grouped["Thruplays"] / single_var_grouped["Impressions"]).apply(format_percentage)
        single_var_grouped["CPL"] = (single_var_grouped["Cost"] / single_var_grouped["Leads"]).apply(format_dollar)
        single_var_grouped["CVR (Click)"] = (single_var_grouped["Leads"] / single_var_grouped["Clicks"]).apply(format_percentage)

        single_var_grouped = single_var_grouped[[var] + final_metrics]

        st.dataframe(single_var_grouped, use_container_width=True)
        st.divider()

    st.divider()

if __name__ == "__main__":
    main()
