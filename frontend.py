import requests
import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title='Twitter data analysis Dashboard',
    page_icon='✅',
    layout='wide'
)

st.title("Twitter analysis Dashboard")

# Define the API base endpoint
API_BASE_ENDPOINT = "http://127.0.0.1:5000/aggregations"

# Get user input for the query parameter
query_param = st.text_input("Enter query parameter", "to")

# Define the API endpoint with the user-provided query parameter
API_ENDPOINT = f"{API_BASE_ENDPOINT}?q={query_param}"

# Make the API call to fetch data
response = requests.get(API_ENDPOINT)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()

    # Extract stats data
    stats = data.get("stats", {})
    stats_df = pd.DataFrame(stats.items(), columns=["Sentiment", "Count"])

    # Extract demographics data
    demographics = data.get("demographics", [{}])
    demographics_df = pd.DataFrame(demographics)

    # Extract hashtag count data
    hashtag_counts = data.get("hashtags_count", [{}])
    hashtag_counts_df = pd.DataFrame(hashtag_counts)

    # Extract location count data
    location_counts = data.get("location_count", [{}])
    location_counts_df = pd.DataFrame(location_counts)

    # Layout the charts in a 2x2 grid
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Sentiment Distribution")
        # Pie chart for sentiment stats
        fig_sentiment = px.pie(stats_df, values='Count', names='Sentiment', title='Sentiment Distribution')
        st.plotly_chart(fig_sentiment)

        st.subheader("Hashtag Counts")
        # Bar chart for hashtag counts
        if not hashtag_counts_df.empty:
            hashtag_counts_df = pd.melt(hashtag_counts_df, var_name='Hashtag', value_name='Count')
            fig_hashtags = px.bar(hashtag_counts_df, x='Hashtag', y='Count', title='Hashtag Counts')
            st.plotly_chart(fig_hashtags)
        else:
            st.write("No hashtag counts data available.")

    with col2:
        st.subheader("Demographics Distribution")
        # Bar chart for demographics
        if not demographics_df.empty:
            demographics_df = demographics_df.reset_index().rename(columns={'index': 'Category'})
            demographics_df = pd.melt(demographics_df, id_vars=['Category'], var_name='State', value_name='Count')
            fig_demographics = px.bar(demographics_df, x='Category', y='Count', color='State',
                                      title='Demographics Distribution')
            st.plotly_chart(fig_demographics)
        else:
            st.write("No demographics data available.")

        st.subheader("Location Counts")
        # Bar chart for location counts
        if not location_counts_df.empty:
            location_counts_df = pd.melt(location_counts_df, var_name='Location', value_name='Count')
            fig_locations = px.bar(location_counts_df, x='Location', y='Count', title='Location Counts')
            st.plotly_chart(fig_locations)
        else:
            st.write("No location counts data available.")

else:
    st.error(f"Failed to fetch data from the API. Status code: {response.status_code}")
