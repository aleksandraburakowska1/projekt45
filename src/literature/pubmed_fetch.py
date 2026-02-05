#!/usr/bin/env python3
import argparse
from pathlib import Path
import time
import yaml
import pandas as pd
from Bio import Entrez

def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    pub = cfg.get("pubmed", {})

    email = pub.get("entrez_email")
    queries = pub.get("queries", [])
    retmax = int(pub.get("retmax", 200))

    if not email:
        raise SystemExit("Brak pubmed.entrez_email w config.")
    if not isinstance(queries, list) or len(queries) == 0:
        raise SystemExit("Brak pubmed.queries (lista zapytań) w config.")

    Entrez.email = str(email)

    out_dir = Path(f"results/literature/{args.year}")
    _ensure_dir(out_dir)

    rows = []
    summary_rows = []

    for q in queries:
        term = f"({q}) AND ({args.year}[pdat])"

        # esearch: ile + lista PMID (limitowana retmax)
        h = Entrez.esearch(db="pubmed", term=term, retmax=retmax)
        res = Entrez.read(h)
        h.close()

        pmids = res.get("IdList", [])
        total_count = int(res.get("Count", 0))
        summary_rows.append({"query": q, "year": args.year, "count": total_count, "retmax": retmax})

        if not pmids:
            continue

        # esummary: metadane (szybkie)
        hs = Entrez.esummary(db="pubmed", id=",".join(pmids), retmode="xml")
        docs = Entrez.read(hs)
        hs.close()

        for d in docs:
            pmid = str(d.get("Id", "")).strip()
            title = str(d.get("Title", "")).strip()
            journal = (str(d.get("FullJournalName", "")).strip() or str(d.get("Source", "")).strip())
            pubdate = str(d.get("PubDate", "")).strip()

            authors = d.get("AuthorList", [])
            authors_str = "; ".join([str(a) for a in authors]) if authors else ""

            rows.append({
                "query": q,
                "pmid": pmid,
                "title": title,
                "journal": journal,
                "pubdate": pubdate,
                "year": int(args.year),
                "authors": authors_str,
            })

        # delikatny throttle (NCBI)
        time.sleep(0.34)

    papers = pd.DataFrame(rows, columns=["query","pmid","title","year","journal","authors","pubdate"])
    if not papers.empty:
        papers = papers.sort_values(["query", "pmid"]).reset_index(drop=True)
    papers.to_csv(out_dir / "pubmed_papers.csv", index=False)

    summary = pd.DataFrame(summary_rows, columns=["query","year","count","retmax"]).sort_values(["query","year"]).reset_index(drop=True)
    summary.to_csv(out_dir / "summary_by_year.csv", index=False)

    if papers.empty:
        topj = pd.DataFrame(columns=["journal","n_papers"])
    else:
        topj = (
            papers.groupby("journal", as_index=False)
                  .agg(n_papers=("pmid", "count"))
                  .sort_values("n_papers", ascending=False)
                  .head(10)
                  .reset_index(drop=True)
        )
    topj.to_csv(out_dir / "top_journals.csv", index=False)

    print(f"OK: zapisano {out_dir}/pubmed_papers.csv, summary_by_year.csv, top_journals.csv")

if __name__ == "__main__":
    main()
