import streamlit as st
import pandas as pd
import networkx as nx
import plotly.express as px

# --- Load data ---


@st.cache_data
def load_papers():
    papers = pd.read_csv("../../data/processed/papers.csv")
    papers = papers.astype({"publish_date": "date64[pyarrow]"})
    return papers


@st.cache_data
def load_paper_to_keyword():
    paper_to_keyword = pd.read_csv("../../data/processed/paper_to_keyword.csv")
    return paper_to_keyword


@st.cache_data
def load_paper_to_classification_code():
    paper_to_classification_code = pd.read_csv(
        "../../data/processed/paper_to_classification_code.csv"
    )
    return paper_to_classification_code


@st.cache_data
def load_classification_codes():
    classification_codes = pd.read_csv("../../data/processed/classification_codes.csv")
    return classification_codes


papers = load_papers()
paper_to_keyword = load_paper_to_keyword()
paper_to_classification_code = load_paper_to_classification_code()
classification_codes = load_classification_codes()


# Sidebar
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Visualizations", "About"])

papers_tab, tab2, tab3 = st.tabs(["Papers", "Tab 2", "Tab 3"])

# Home
if page == "Visualizations":
    with papers_tab:
        st.title("Papers")

        # Papers
        st.write(papers)

        # Papers by year
        papers["year"] = papers["publish_date"].dt.year
        papers_by_year = papers.groupby("year").size().reset_index()
        papers_by_year.columns = ["year", "paper_count"]

        fig = px.line(
            papers_by_year,
            x="year",
            y="paper_count",
            title="Papers by year",
            markers=True,
        )
        st.plotly_chart(fig)

        # Papers by year by abbreviation
        papers.info()
        paper_to_classification_code.info()
        papers_by_year_by_abbreviation = (
            papers.rename(columns={"id": "paper_id"})
            .join(paper_to_classification_code, on="paper_id")
            .join(classification_codes, on="code")
        )
        st.write(papers_by_year_by_abbreviation)

# About
elif page == "About":
    st.title("About")
    st.write("This is the about page.")
