#!/usr/bin/env python3
import argparse
from pathlib import Path
import yaml
import pandas as pd

def shift_midnight_to_prev_day(dt: pd.Series) -> pd.Series:
    midnight = dt.dt.strftime("%H:%M:%S") == "00:00:00"
    dt = dt.copy()
    dt.loc[midnight] = dt.loc[midnight] - pd.Timedelta(days=1)
    return dt

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8")) or {}
    cities = cfg.get("cities", [])
    pm25_cfg = cfg.get("pm25", {})

    limit = float(pm25_cfg.get("who_daily_limit", 15))
    input_csv = pm25_cfg.get("input_csv", "PM25_all_years.csv")

    if not Path(input_csv).exists():
        raise SystemExit(f"Brak pliku wejściowego: {input_csv}")

    df = pd.read_csv(input_csv, low_memory=False)

    # wykryj kolumnę czasu
    time_col = None
    for c in df.columns:
        if str(c).lower().strip() in ["data", "czas", "datetime", "date", "timestamp"]:
            time_col = c
            break
    if time_col is None:
        time_col = df.columns[0]

    df = df.rename(columns={time_col: "Data"})
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce")
    df = df.dropna(subset=["Data"]).copy()

    # 00:00 -> poprzedni dzień (jeśli u Ciebie tak było w projekcie)
    df["Data"] = shift_midnight_to_prev_day(df["Data"])

    # filtr na rok
    df = df[df["Data"].dt.year == args.year].copy()
    if df.empty:
        raise SystemExit(f"Brak danych w {input_csv} dla roku {args.year}")

    df["date"] = df["Data"].dt.floor("D")

    # wybór kolumn z wartościami (Twoja wersja: miasta jako kolumny)
    if cities:
        missing = [c for c in cities if c not in df.columns]
        if missing:
            raise SystemExit(
                f"Brakuje kolumn miast {missing} w danych. "
                f"Dostępne kolumny: {list(df.columns)}"
            )
        value_cols = cities
    else:
        value_cols = [c for c in df.columns if c not in ["Data", "date"]]

    for c in value_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    daily = df.groupby("date", as_index=False)[value_cols].mean(numeric_only=True)

    exceed = pd.DataFrame({
        "city": value_cols,
        "year": args.year,
        "who_limit": limit,
        "exceedance_days": [(daily[c] > limit).sum() for c in value_cols],
    })

    out_dir = Path(f"results/pm25/{args.year}")
    out_dir.mkdir(parents=True, exist_ok=True)
    daily.to_csv(out_dir / "daily_means.csv", index=False)
    exceed.to_csv(out_dir / "exceedance_days.csv", index=False)

    print(f"OK: zapisano {out_dir/'daily_means.csv'} i {out_dir/'exceedance_days.csv'}")

if __name__ == "__main__":
    main()
