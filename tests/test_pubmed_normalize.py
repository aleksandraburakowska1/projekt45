import pandas as pd
from src.literature.pubmed_fetch import normalize_papers

def test_normalize_papers_sorts_and_strips():
    df = pd.DataFrame([
        {"query": " q ", "pmid": "2", "title": " B ", "year": 2024, "journal": " J ", "authors": " A ", "pubdate": "2024"},
        {"query": " q ", "pmid": "1", "title": " A ", "year": 2024, "journal": " J ", "authors": " A ", "pubdate": "2024"},
    ])

    out = normalize_papers(df)

    # sortowanie po pmid
    assert out["pmid"].tolist() == ["1", "2"]

    # strip
    assert out.loc[0, "query"] == "q"
    assert out.loc[0, "title"] == "A"

    # kolumny (kolejność)
    assert list(out.columns) == ["query", "pmid", "title", "year", "journal", "authors", "pubdate"]
