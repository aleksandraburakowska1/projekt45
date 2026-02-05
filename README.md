**Analiza PM2.5 w Polsce – pipeline danych środowiskowych i literaturowych**

Projekt dotyczy analizy stężenia pyłu PM2.5 na podstawie danych pomiarowych z różnych stacji w Polsce. W projekcie obliczane są średnie wartości, identyfikowane są dni z przekroczeniem normy oraz tworzone są wykresy porównawcze.

**Struktura projektu**

```text
Projekt 4/
|-- tests/
| |-- test_io_clean.py
| |-- test_metrics.py
|-- Metadane oraz kody stacji i stanowisk pomiarowych.xlsx
|-- PM25_all_years.csv        # główny zbiór danych
|-- init.py
|-- io_clean.py               # wczytywanie i czyszczenie danych
|-- metrics.py                # obliczanie statystyk i norm
|-- viz.py                    # generowanie wykresów
|-- projekt_3.ipynb   # notatnik główny
|-- requirements.txt
```

**Zadania**


**Zadanie 1 – Wczytanie i czyszczenie danych**

- import danych z plików CSV

- usunięcie braków i błędnych rekordów

- konwersja kolumny daty do typu datetime

**Zadanie 2 – Obliczanie średnich**

Obliczanie średnich ze wzorem:

   $\overline{x} = \frac{1}{n}\sum_{i=1}^n x_i$

- W kodzie liczone są średnie dobowe oraz miesięczne

- rysowanie liniowych wykresów

- porównania lat i lokalizacji zgodnie z treścią zadania

**Zadanie 3 – Wizualizacja**

Wykonanie heatmap miesięcznych średnich dla miast

**Zadanie 4 – Przekroczenia normy:**
- zliczanie dni z przekroczeniem dopuszczalnej wartości i liczby przekroczeń dla wybranych stacji
- wizualizacja na wykresie i interpretacja

**Wymagania pakietów**

Projekt wykorzystuje następujące biblioteki:

- numpy
- pandas
- matplotlib
- seaborn
- requests
- pytest
- openpyxl

Wymagana jest również nowa wersja Pythona >3.10

Instalacja pakietów:
```bash
pip install -r requirements.txt
```
**Pliki:**

Wymienione poniżej pliki są wczytywane przez plik projekt_1_student.ipynb, zawierają one funkcje potrzebne do realizacji zadań, dzięki takiemu rozwiązaniu kod jest uporządkowany i czytelny.
- io_clean.py
- metrics.py
 - viz.py

**Instalacja:**
- sklonuj repozytorium:

```bash
git clone https://github.com/aleksandraburakowska1/Maly_projekt3_Ola_i_Michal.git
```

- można zainstalować wirtualne środowisko
- zainstaluj odpowiednie pakiety

**Uruchomienie testów:**
```bash
python -m pytest
```
**Autorzy:**

Aleksandra Burakowska, Michał Pszenicyn
