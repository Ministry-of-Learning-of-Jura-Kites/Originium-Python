import streamlit as st
import pandas as pd
import altair as alt
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import json
import pydeck as pdk
import os
import plotly.express as px
import sys

ROOT_PATH = ""

sys.path.append(ROOT_PATH)
from utils.load_pipeline import *

DATA_PATH = os.path.join(ROOT_PATH, "data/processed/")
PIPELINE_PATH = os.path.join(ROOT_PATH, "models/pipeline.pkl")
SUBJECTS_PATH = os.path.join(ROOT_PATH, "data/processed/subjects.csv")
GEOJSON_PATH = os.path.join(ROOT_PATH, "notebooks/data_visualization/countries.geo.json")

@st.cache_data
def load_data(filename: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(DATA_PATH, filename))

@st.cache_data
def preprocess_papers(df: pd.DataFrame) -> pd.DataFrame:
    if 'publish_date' in df.columns:
        df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
    return df

def setup_sidebar(papers_df: pd.DataFrame):
    st.sidebar.header("Filters")
    min_year = int(papers_df['publish_date'].dt.year.min()) if papers_df['publish_date'].notnull().any() else 2000
    max_year = int(papers_df['publish_date'].dt.year.max()) if papers_df['publish_date'].notnull().any() else 2023
    year_range = st.sidebar.slider("Publication Year Range", min_value=min_year, max_value=max_year, value=(min_year, max_year))
    return year_range

@st.cache_data
def load_and_preprocess_data():
    papers_df = preprocess_papers(load_data("papers.csv"))
    affiliations_df = load_data("affiliations.csv")
    classification_codes_df = load_data("classification_codes.csv")
    paper_to_classification_code_df = load_data("paper_to_classification_code.csv")
    paper_to_affiliation_df = load_data("paper_to_affiliation.csv")
    paper_to_keyword_df = load_data("paper_to_keyword.csv")
    author_pub_counts_df = load_data("author_pub_counts.csv")
    return (papers_df, affiliations_df, classification_codes_df,
            paper_to_classification_code_df, paper_to_affiliation_df,
            paper_to_keyword_df, author_pub_counts_df)

@st.cache_data
def load_pipeline():
    return Pipeline(PIPELINE_PATH, SUBJECTS_PATH)

@st.cache_data
def get_filtered_papers(papers_df, year_range):
    return papers_df[
        (papers_df['publish_date'].dt.year >= year_range[0]) & 
        (papers_df['publish_date'].dt.year <= year_range[1])
    ]

@st.cache_data
def plot_publications_over_time(filtered_papers):
    st.subheader("1. Publications Over Time")
    st.write("Yearly research publication trends.")
    pub_per_year = filtered_papers.groupby(filtered_papers['publish_date'].dt.year).size().reset_index(name='count')
    line_chart = alt.Chart(pub_per_year).mark_line(point=True).encode(
        x=alt.X('publish_date:O', title='Year'),
        y=alt.Y('count:Q', title='Number of Papers')
    )
    st.altair_chart(line_chart, use_container_width=True)
    
@st.cache_data
def plot_top_journals(filtered_papers):
    st.subheader("2. Top Journals")
    st.write("Most popular journals.")
    if 'publication_name' in filtered_papers.columns:
        top_journals = filtered_papers['publication_name'].value_counts().head(10).reset_index()
        top_journals.columns = ['publication_name', 'count']
        bar_chart = alt.Chart(top_journals).mark_bar().encode(
            x=alt.X('count:Q', title='Count'),
            y=alt.Y('publication_name:N', sort='-x', title='Journal')
        )
        st.altair_chart(bar_chart, use_container_width=True)
    else:
        st.write("No journal information available.")
    
@st.cache_data
def plot_top_classification_codes(filtered_papers, paper_to_classification_code_df, classification_codes_df):
    st.subheader("3. Top Research Classification Codes")
    st.write("Most common research areas.")
    merged_class = pd.merge(paper_to_classification_code_df, classification_codes_df, on='code', how='inner')
    merged_class = merged_class[merged_class['paper_id'].isin(filtered_papers['id'])]
    if 'abbreviation' in merged_class.columns and 'name' in merged_class.columns:
        merged_class['display_name'] = merged_class['abbreviation']
        merged_class['full_name'] = merged_class['name']
    grouped_data = merged_class.groupby('display_name').agg(
        count=('full_name', 'size'),
        full_name_list=('full_name', list)
    ).reset_index()
    top_codes = grouped_data.sort_values(by='count', ascending=False).head(10)
    bar_chart_class = alt.Chart(top_codes).mark_bar().encode(
        x=alt.X('count:Q', title='Count'),
        y=alt.Y('display_name:N', sort='-x', title='Classification Code'),
        tooltip=[
            alt.Tooltip('display_name:N', title='Abbreviation'),
            alt.Tooltip('full_name_list:N', title='Full Names'),
            alt.Tooltip('count:Q', title='Count')
        ]
    )
    st.altair_chart(bar_chart_class, use_container_width=True)

@st.cache_data
def get_research_trends_over_time_data(filtered_papers, paper_to_classification_code_df, classification_codes_df):
    merged_class = pd.merge(paper_to_classification_code_df, classification_codes_df, on='code', how='inner')
    merged_class = merged_class[merged_class['paper_id'].isin(filtered_papers['id'])]
    merged_class['year'] = merged_class['paper_id'].map(filtered_papers.set_index('id')['publish_date'].dt.year)
    if 'abbreviation' in merged_class.columns and 'name' in merged_class.columns:
        merged_class['display_name'] = merged_class['abbreviation']
        merged_class['full_name'] = merged_class['name']
    max_categories = merged_class['display_name'].nunique()
    return merged_class, max_categories

@st.cache_data
def get_research_trends_chart(merged_class, top_n):
    category_counts = merged_class.groupby('display_name').size().reset_index(name='total_count')
    top_categories = category_counts.nlargest(top_n, 'total_count')['display_name']
    merged_class = merged_class[merged_class['display_name'].isin(top_categories)]
    trends = merged_class.groupby(['year', 'display_name']).size().reset_index(name='count')
    trend_chart = alt.Chart(trends).mark_line(point=True).encode(
        x=alt.X('year:O', title='Year'),
        y=alt.Y('count:Q', title='Number of Papers'),
        color=alt.Color('display_name:N', title='Research Category'),
        tooltip=[
            alt.Tooltip('year:O', title='Year'),
            alt.Tooltip('display_name:N', title='Category'),
            alt.Tooltip('count:Q', title='Number of Papers')
        ]
    )
    return trend_chart

def plot_research_trends_over_time(filtered_papers, paper_to_classification_code_df, classification_codes_df):
    st.subheader("4. Research Category Trends Over Time")
    st.write("Trends in research areas over time.")
    merged_class, max_categories = get_research_trends_over_time_data(filtered_papers, paper_to_classification_code_df, classification_codes_df)
    top_n = st.slider("Select number of top categories to display:", min_value=3, max_value=max_categories, value=10)
    trend_chart = get_research_trends_chart(merged_class, top_n)
    st.altair_chart(trend_chart, use_container_width=True)

@st.cache_data
def get_merged_keywords(filtered_papers, paper_to_keyword_df):
    merged_keywords = paper_to_keyword_df[paper_to_keyword_df['id'].isin(filtered_papers['id'])]
    if merged_keywords.empty:
        return None
    else:
        return merged_keywords

@st.cache_data
def plot_keyword_analysis(filtered_papers, paper_to_keyword_df):
    st.subheader("7. Keyword Analysis")
    st.write("Common keywords in research.")
    merged_keywords = get_merged_keywords(filtered_papers, paper_to_keyword_df)
    if not merged_keywords.empty:
        keyword_text = " ".join(merged_keywords['keyword'].dropna().astype(str))
        wordcloud = WordCloud(
            background_color="white",
            width=800,
            height=400,
            colormap="viridis",
            max_words=150,
            max_font_size=100
        ).generate(keyword_text)

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)
    else:
        st.write("No keywords available for the selected years.")

@st.cache_data
def plot_affiliations_by_country(filtered_papers, paper_to_affiliation_df, affiliations_df):
    st.subheader("6. Affiliations by Country")
    st.write("Geographic distribution of research.")
    merged_aff = pd.merge(paper_to_affiliation_df, affiliations_df, left_on='id', right_on='id', how='inner')
    merged_aff = merged_aff[merged_aff['paper_id'].isin(filtered_papers['id'])]
    if 'country' in merged_aff.columns:
        country_counts = merged_aff['country'].value_counts().reset_index()
        country_counts.columns = ['country', 'count']
        # Map country names to match geojson data
        name_mapping = {
            "Russian Federation": "Russia",
            "United States": "United States of America",
            "Viet Nam": "Vietnam",
            "North Macedonia": "Macedonia",
            "Democratic Republic Congo": "Democratic Republic of the Congo",
            "Cote d'Ivoire": "Ivory Coast",
            "Brunei Darussalam": "Brunei",
            "Czech Republic": "Czechia",
            "Syria": "Syrian Arab Republic",
            "Swaziland": "Eswatini",
        }
        country_counts['country'] = country_counts['country'].replace(name_mapping)
        country_dict = dict(zip(country_counts['country'], country_counts['count']))

        with open(GEOJSON_PATH, 'r') as f:
            geojson_data = json.load(f)

        bubble_data = []
        counts = list(country_dict.values())
        if len(counts) == 0:
            st.write("No country data available.")
            return
        min_count = min(counts)
        max_count = max(counts)

        def normalize_size(c):
            if max_count == min_count:
                return 20000
            return 200000 + (1000000 - 200000) * ((c - min_count) / (max_count - min_count))

        for feature in geojson_data['features']:
            country_name = feature['properties'].get('name', None)
            count = country_dict.get(country_name, 0)
            if count > 0:
                geometry = feature['geometry']
                if geometry['type'] == 'Polygon':
                    coords = geometry['coordinates'][0]
                    lon = sum(c[0] for c in coords) / len(coords)
                    lat = sum(c[1] for c in coords) / len(coords)
                elif geometry['type'] == 'MultiPolygon':
                    all_coords = []
                    for poly in geometry['coordinates']:
                        all_coords.extend(poly[0])
                    lon = sum(c[0] for c in all_coords) / len(all_coords)
                    lat = sum(c[1] for c in all_coords) / len(all_coords)
                else:
                    continue

                bubble_data.append({
                    'coordinates': [lon, lat],
                    'count': count,
                    'country': country_name,
                    'radius': normalize_size(count),
                    'color': [255, 140, 0, 150]
                })

        layer = pdk.Layer(
            "ScatterplotLayer",
            bubble_data,
            pickable=True,
            get_position="coordinates",
            get_radius="radius",
            get_fill_color="color",
            get_line_color=[0, 0, 0, 50],
        )

        tooltip = {
            "html": "<b>{country}</b><br>Publications: {count}",
            "style": {
                "backgroundColor": "white",
                "border": "1px solid black",
                "padding": "5px"
            }
        }

        view_state = pdk.ViewState(latitude=20, longitude=0, zoom=1)
        r = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style='mapbox://styles/mapbox/light-v9'
        )

        st.pydeck_chart(r)
    else:
        st.write("No country information available.")

@st.cache_data
def get_top_authors_publication_distribution_data(author_pub_counts):
    author_column = 'name'
    max_authors = author_pub_counts['name'].nunique()
    return author_column, max_authors

@st.cache_data
def get_top_authors(author_pub_counts, top_n):
    return author_pub_counts.nlargest(top_n, 'publication_count')

def plot_top_authors_publication_distribution(author_pub_counts):
    st.subheader("5. Top Authors Publication Distribution")
    st.write("Pie chart showing publication contributions by most prolific authors.")
    author_column, max_authors = get_top_authors_publication_distribution_data(author_pub_counts)
    top_n = st.slider(
        "Select number of top authors to display:", 
        min_value=3, 
        max_value=min(20, max_authors), 
        value=min(10, max_authors)
    )
    top_authors = get_top_authors(author_pub_counts, top_n)
    fig = px.pie(top_authors, values='publication_count', names=author_column, title='Publication Contributions by Top Authors')
    st.plotly_chart(fig)

def main():
    st.set_page_config(page_title="Research Data Visualization Dashboard", layout="wide", page_icon="📊")

    dashboard_tab, app_demo_tab = st.tabs(["Dashboard", "App Demo"])

    with dashboard_tab:
        st.title("Research Data Visualization Dashboard")
        st.markdown("This dashboard provides an overview of research publications data.")

        (papers_df, affiliations_df, classification_codes_df,
            paper_to_classification_code_df, paper_to_affiliation_df,
            paper_to_keyword_df, author_pub_counts_df) = load_and_preprocess_data()

        year_range = setup_sidebar(papers_df)
        filtered_papers = get_filtered_papers(papers_df, year_range)
        num_filtered_papers = len(filtered_papers)
        st.markdown(f"**Showing data from {year_range[0]} to {year_range[1]} ({num_filtered_papers} papers)**")

        print("Publication Over Time")
        with st.expander("Publication Over Time", expanded=True):
            plot_publications_over_time(filtered_papers)

        print("Top Journals")
        with st.expander("Top Journals", expanded=True):
            plot_top_journals(filtered_papers)

        print("Top Research Classification Codes")
        with st.expander("Top Research Classification Codes", expanded=True):
            plot_top_classification_codes(filtered_papers, paper_to_classification_code_df, classification_codes_df)

        print("Research Trends Over Time")
        with st.expander("Research Trends Over Time", expanded=True):
            plot_research_trends_over_time(filtered_papers, paper_to_classification_code_df, classification_codes_df)

        print("Top Authors Publication Distribution")
        with st.expander("Top Authors Publication Distribution", expanded=True):
            plot_top_authors_publication_distribution(author_pub_counts_df)

        print("Affiliations by Country")
        with st.expander("Affiliations by Country", expanded=True):
            plot_affiliations_by_country(filtered_papers, paper_to_affiliation_df, affiliations_df)

        print("Keyword Analysis")
        with st.expander("Keyword Analysis", expanded=True):
            plot_keyword_analysis(filtered_papers, paper_to_keyword_df)

        st.markdown("**End of Dashboard**")
    
    with app_demo_tab:
        pipeline = load_pipeline()
        st.title("Demo: Predicting Research Subjects and Supergroups from Paper Title and Abstract")

        st.write("This app predicts the subjects and supergroups of a research paper based on its title and/or abstract.")
        title = st.text_area("Enter paper title and/or abstract here.")

        if title:
            prediction = pipeline.predict([title])[0]

            st.header("Predicted subjects")
            subject_full_names = prediction.get_full_names()
            if len(subject_full_names) == 0:
                st.write("No subjects predicted.")
            else:
                for subject_full_name in prediction.get_full_names():
                    st.markdown(f"- {subject_full_name}")

            st.header("Predicted supergroups")
            supergroups = prediction.get_full_names()
            if len(supergroups) == 0:
                st.write("No supergroups predicted.")
            else:
                for supergroup in prediction.get_supergroups():
                    st.markdown(f"- {supergroup}")

if __name__ == "__main__":
    main()

