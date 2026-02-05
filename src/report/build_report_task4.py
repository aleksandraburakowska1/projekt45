#!/usr/bin/env python3
import argparse
from pathlib import Path
import yaml
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    years = cfg.get("years", [])
    cities = cfg.get("cities", [])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append("# Raport Task 4\n")
    lines.append(f"**Lata:** {years}\n")
    if cities:
        lines.append(f"**Miasta:** {cities}\n")

    # --- PM2.5 ---
    lines.append("\n## PM2.5\n")
    pm_rows = []
    for y in years:
        p = Path(f"results/pm25/{y}/exceedance_days.csv")
        if p.exists():
            df = pd.read_csv(p)
            df["year"] = y
            pm_rows.append(df)

    if pm_rows:
        pm = pd.concat(pm_rows, ignore_index=True)
        if cities and "city" in pm.columns:
            pm = pm[pm["city"].isin(cities)]
        # porządek kolumn
        cols = [c for c in ["year","city","exceedance_days","total_days","exceedance_fraction","who_daily_limit_ugm3","who_limit"] if c in pm.columns]
        pm = pm[cols] if cols else pm
        lines.append(pm.to_markdown(index=False))
        lines.append("")
    else:
        lines.append("_Brak danych PM2.5._\n")

    # --- Literatura ---
    lines.append("\n## Literatura (PubMed)\n")

    sum_rows = []
    for y in years:
        s = Path(f"results/literature/{y}/summary_by_year.csv")
        if s.exists():
            df = pd.read_csv(s)
            sum_rows.append(df)

    if sum_rows:
        summ = pd.concat(sum_rows, ignore_index=True)
        lines.append("### Liczba publikacji (per zapytanie i rok)\n")
        lines.append(summ.to_markdown(index=False))
        lines.append("")
    else:
        lines.append("_Brak danych literaturowych._\n")

    for y in years:
        lines.append(f"### Rok {y}\n")

        tj = Path(f"results/literature/{y}/top_journals.csv")
        pp = Path(f"results/literature/{y}/pubmed_papers.csv")

        if tj.exists():
            t = pd.read_csv(tj)
            lines.append("**Top journals (top 10):**\n")
            lines.append(t.to_markdown(index=False))
            lines.append("")
        else:
            lines.append("_Brak top_journals.csv._\n")

        if pp.exists():
            p = pd.read_csv(pp)
            lines.append("**Przykładowe tytuły (max 5):**\n")
            for title in p.get("title", pd.Series(dtype=str)).dropna().head(5).tolist():
                lines.append(f"- {title}")
            lines.append("")
        else:
            lines.append("_Brak pubmed_papers.csv._\n")

    out.write_text("\n".join(lines), encoding="utf-8")
    print(f"OK: zapisano {out}")

if __name__ == "__main__":
    main()
