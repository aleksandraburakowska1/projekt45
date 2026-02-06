**Analiza PM2.5 w Polsce – pipeline danych środowiskowych i literaturowych**

Projekt realizuje analizę stężeń pyłu zawieszonego PM2.5 w Polsce oraz łączy ją z analizą publikacji naukowych dotyczących wpływu zanieczyszczeń powietrza na zdrowie.Całość została zautomatyzowana przy pomocy Snakemake, co pozwala łatwo odtwarzać wyniki dla kolejnych lat bez ręcznego przetwarzania danych. Pipeline wykonuje:
- obliczanie dobowych średnich stężeń PM2.5,
- zliczanie dni z przekroczeniem norm WHO,
- pobieranie publikacji z PubMed,
- tworzenie zestawień literaturowych,
- generowanie raportu końcowego.

**Struktura projektu**

```projekt4/
├── config/
│   └── task4.yaml                # konfiguracja pipeline
│
├── data/
│   └── raw/                      # ewentualne dane źródłowe
│
├── results/
│   ├── pm25/{rok}/
│   │   ├── daily_means.csv
│   │   └── exceedance_days.csv
│   │
│   ├── literature/{rok}/
│   │   ├── pubmed_papers.csv
│   │   ├── summary_by_year.csv
│   │   └── top_journals.csv
│   │
│   └── report_task4.md           # raport końcowy
│
├── src/
│   ├── pm25/
│   │   └── run_pm25_year_from_all.py
│   │
│   ├── literature/
│   │   └── pubmed_fetch.py
│   │
│   └── report/
│       └── build_report_task4.py
│
├── tests/
│   ├── test_io_clean.py
│   ├── test_metrics.py
│   └── test_task4.py
│
├── io_clean.py
├── metrics.py
├── viz.py
├── Snakefile_task4
├── PM25_all_years.csv
└── README.md

```

**Dane PM2.5:**

Analiza oparta jest na zbiorze: PM25_all_years.csv, zawierającym dane pomiarowe PM2.5 z wielu lat i stacji pomiarowych.
Pipeline:
1)filtruje dane dla wybranego roku,
2)oblicza dobowe średnie,
3)liczy dni przekroczenia norm WHO,
4)zapisuje wyniki do katalogu results/pm25/{rok}/

**Analiza literatury (PubMed)**

Pipeline automatycznie:
- pobiera publikacje z PubMed,
- filtruje je według zapytań konfiguracyjnych,
- zapisuje listę artykułów,
- generuje statystyki roczne,
- wyznacza najczęściej występujące czasopisma.

Wyniki zapisywane są do: results/literature/{rok}/

**Konfiguracja**
Pipeline sterowany jest plikiem: config/task4.yaml. Przykład:
```
years: [2021, 2024]

cities:
  - Warszawa
  - Katowice

pm25:
  who_daily_limit: 15
  input_csv: PM25_all_years.csv

pubmed:
  retmax: 100
  queries:
    - "(PM2.5 OR particulate matter) AND (health OR mortality)"

```
Zmiana lat lub miast nie wymaga zmiany kodu — wystarczy edycja pliku YAML.

**Uruchomienie pipeline**

Instalacja zależności:
```
pip install -r requirements.txt
```
Uruchomienie:
```
snakemake -s Snakefile_task4 --cores 1 --rerun-triggers checksum
```

**Dlaczego używamy --rerun-triggers checksum?**
Domyślnie Snakemake sprawdza jedynie czas modyfikacji pliku (mtime).
Opcja:
```
--rerun-triggers checksum
```
powoduje, że Snakemake wykrywa zmiany na podstawie zawartości plików, a nie tylko ich daty. Dzięki temu:
- pipeline nie uruchamia się niepotrzebnie,
- zmiana danych zawsze wymusza przeliczenie wyników,
- wyniki są bardziej powtarzalne.

**Autor:**

Aleksandra Burakowska
