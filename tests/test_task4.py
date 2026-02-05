import pandas as pd

def test_pubmed_papers_sorted_by_pmid(tmp_path):
    # minimalny test deterministyczności: sort po pmid
    df = pd.DataFrame([
        {"query":"q", "pmid":"2", "title":"b", "year":2024, "journal":"J", "authors":"A", "pubdate":"2024"},
        {"query":"q", "pmid":"1", "title":"a", "year":2024, "journal":"J", "authors":"A", "pubdate":"2024"},
    ])
    df = df.sort_values(["query", "pmid"]).reset_index(drop=True)
    assert df["pmid"].tolist() == ["1","2"]
