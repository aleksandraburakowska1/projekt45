import requests
import zipfile
import io, os
import pandas as pd

gios_archive_url = "https://powietrze.gios.gov.pl/pjp/archives/downloadFile/"
# funkcja do ściągania podanego archiwum
def download_gios_archive(year, gios_id, filename):
    # Pobranie archiwum ZIP do pamięci
    url = f"{gios_archive_url}{gios_id}"
    response = requests.get(url)
    response.raise_for_status()  # jeśli błąd HTTP, zatrzymaj

    # Otwórz zip w pamięci
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        # znajdź właściwy plik z PM2.5
        if not filename:
            print(f"Błąd: nie znaleziono {filename}.")
        else:
            # wczytaj plik do pandas
            with z.open(filename) as f:
                try:
                    df = pd.read_excel(f, header=None)
                except Exception as e:
                    print(f"Błąd przy wczytywaniu {year}: {e}")
    return df
def build_kod2miasto(dfmeta_raw: pd.DataFrame) -> dict:
    meta = dfmeta_raw.copy()
    meta.columns = meta.iloc[0]
    meta = meta[1:].reset_index(drop=True)

    def pick(startswith_txt: str) -> str:
        for c in meta.columns:
            if str(c).strip().lower().startswith(startswith_txt):
                return c
        raise KeyError(f"Brak kolumny zaczynającej się od: {startswith_txt}")

    col_code = pick("kod stacji")  # aktualny kod
    # bywa 'Miejscowość' albo 'Miasto'
    try:
        col_city = pick("miejscowość")
    except KeyError:
        col_city = pick("miasto")

    tmp = meta[[col_code, col_city]].copy()
    tmp[col_code] = tmp[col_code].astype(str).str.strip()
    tmp[col_city] = tmp[col_city].astype(str).str.strip()
    tmp = tmp.dropna(subset=[col_code, col_city])
    tmp = tmp[(tmp[col_code] != "") & (tmp[col_city] != "")]

    return dict(zip(tmp[col_code], tmp[col_city]))


#wprowadzona zmiana względem poprzedniego kodu: wczytuję metadane z pliku excel 
import pandas as pd

def load_gios_metadata(path: str) -> pd.DataFrame | None:
    """
    Wczytuje metadane GIOŚ z lokalnego pliku Excel.

    Parametry:
    - path : str
        Ścieżka do pliku .xlsx z metadanymi

    Zwraca:
    - DataFrame (header=None), jeśli się uda
    - None, jeśli wystąpi błąd (bez wywalania notebooka)
    """
    try:
        df = pd.read_excel(path, header=None)
        return df
    except Exception as e:
        print(f"Błąd przy wczytywaniu metadanych z pliku '{path}': {e}")
        return None

#Zamiana wiersza "Kod stacji" na nagłówek i dodanie kolumny 'Data', żeby ujednolicić format
#potem zmapuję stare kody stacji na nowe zgodnie z metadanymi
def use_station_header(df: pd.DataFrame) -> pd.DataFrame:
    # znajdź wiersz (jego numer), który zawiera frazę "Kod stacji" w pierwszej kolumnie
    first_col = df.columns[0]
    hdr_idx = df.index[df[first_col].astype(str).str.contains("Kod stacji", case=False, na=False)]
    if len(hdr_idx) == 0:
        raise ValueError("Nie znalazłem wiersza z nagłówkiem 'Kod stacji'.")
    hdr_idx = hdr_idx[0]


    new_cols = df.iloc[hdr_idx].tolist()  #cały wiersz zawierający kody stacji zamieniam w listę (ta lista staje się nowymi nazwami kolumn)
    df2 = df.drop(index=hdr_idx).copy()  #usuwamy ten wiersz z danych (bo jest użyty już jako nagłówek)
    df2.columns = new_cols  #przypisujemy nowe nazwy kolumn

    # nazwij pierwszą kolumnę 'Data'

    df2 = df2.rename(columns={df2.columns[0]: "Data"})


    return df2.reset_index(drop=True)
import pandas as pd

def usun_wiersze_opisowe(datasets: dict, verbose: bool = True) -> dict:
    """
    Usuwa wiersze opisowe z danych (np. 'wskaźnik'),
    zostawiając tylko wiersze z datami oraz wiersz 'Kod stacji'.

    Parametry:
    - datasets: dict {rok: DataFrame}
    - verbose: czy wypisywać informacje o liczbie usuniętych wierszy

    Zwraca:
    - dict {rok: oczyszczony DataFrame}
    """
    cleaned = {}

    for year, df in datasets.items():
        first_col = df.columns[0]

        is_date = pd.to_datetime(
            df[first_col],
            errors="coerce",
            format="%Y-%m-%d %H:%M:%S"
        ).notna()

        is_kod = df[first_col].astype(str).str.contains(
            "Kod stacji", case=False, na=False
        )

        df_clean = df.loc[is_date | is_kod].copy()

        if verbose:
            removed = len(df) - len(df_clean)
            print(f"{year}: usunięto {removed} wierszy opisowych, pozostało {len(df_clean)}")

        cleaned[year] = df_clean.reset_index(drop=True)

    return cleaned

#w razie gdyby jedna komórka zawierała 2 stare kody wprowadziłam poprawkę
def build_old2new(dfmeta_raw: pd.DataFrame) -> dict:
    m = dfmeta_raw.copy()
    m.columns = m.iloc[0]
    m = m[1:].reset_index(drop=True)

    def pick(col_starts):
        for c in m.columns:
            if str(c).strip().lower().startswith(col_starts):
                return c
        raise KeyError(f"Brak kolumny zaczynającej się od: {col_starts}")

    col_new = pick("kod stacji")
    col_old = pick("stary kod stacji")

    m2 = (
        m[[col_old, col_new]]
        .dropna(subset=[col_old, col_new])
        .astype(str)
    )

    m2[col_old] = m2[col_old].str.strip()
    m2[col_new] = m2[col_new].str.strip()

    old2new = {}

    for _, row in m2.iterrows():
        new_code = row[col_new]

        # 👉 rozbij TYLKO po przecinku i spacji
        old_codes = row[col_old].replace(",", " ").split(" ")

        for old in old_codes:
            old = old.strip()
            if old:  # pomiń puste
                old2new[old] = new_code

    return old2new

def mapuj_kolumny_z_podgladem(df: pd.DataFrame, mapa: dict) -> pd.DataFrame:
    """
    Zmienia nazwy kolumn na podstawie słownika 'mapa' (stary_kod → nowy_kod).
    Przy okazji wypisuje te kolumny, które zostały przemianowane - dla podglądu, informacji czy coś się zmieniło
    """
    rename_pairs = {}
    for col in df.columns:
        new_name = mapa.get(str(col).strip())
        if new_name and new_name != col:
            rename_pairs[col] = new_name

    # wypisz zmiany
    if rename_pairs:
        print(" Zmienione kolumny:")
        for old, new in rename_pairs.items():
            print(f"   {old}  →  {new}")

    else:
        print(" Żadna kolumna nie wymagała zmiany.")

    # faktyczna zmiana nazw kolumn
    return df.rename(columns=lambda c: mapa.get(str(c).strip(), c))
# --- 1️. Utwórz mapę: kod stacji → miejscowość --
def build_kod2miasto(dfmeta_raw: pd.DataFrame) -> dict:
    """
    Buduje słownik: kod stacji → miejscowość
    na podstawie metadanych GIOŚ.

    Parametry:
    - dfmeta_raw: DataFrame z metadanymi (pierwszy wiersz = nagłówki)

    Zwraca:
    - dict {kod_stacji: miejscowość}
    """
    meta = dfmeta_raw.copy()
    meta.columns = meta.iloc[0]
    meta = meta[1:].reset_index(drop=True)

    col_code = [c for c in meta.columns if "Kod stacji" in str(c)][0]
    col_city = [c for c in meta.columns if "Miejscowość" in str(c)][0]

    kod2miasto = (
        meta[[col_code, col_city]]
        .dropna(subset=[col_code, col_city])
        .astype(str)
        .set_index(col_code)[col_city]
        .to_dict()
    )

    return kod2miasto

#MultiIndex to po prostu wielopoziomowy indeks — czyli taki, gdzie:
#każda kolumna (lub wiersz) ma kilka warstw etykiet zamiast jednej.
# dodaję multiindeks korzystając ze słownika który utworzyłam wcześniej
import pandas as pd

def dodaj_multiindex(df: pd.DataFrame, mapa_kod_miasto: dict) -> pd.DataFrame:
    """
    Dodaje MultiIndex do kolumn: (Miejscowość, Kod stacji).
    Jeśli DataFrame ma już MultiIndex w kolumnach – zwraca go bez zmian.
    """
    # jeśli kolumny są już MultiIndex, nie ruszamy
    if isinstance(df.columns, pd.MultiIndex):
        print("Kolumny już mają MultiIndex – nic nie zmieniam.")
        return df

    nowe_kolumny = []
    for col in df.columns:
        if str(col).lower() == "data":
            # kolumna z datą – osobny poziom 'Data'
            nowe_kolumny.append(("", "Data"))
        else:
            miasto = mapa_kod_miasto.get(col, "Nieznane")
            nowe_kolumny.append((miasto, col))

    df2 = df.copy()
    df2.columns = pd.MultiIndex.from_tuples(nowe_kolumny,
                                            names=["Miejscowość", "Kod stacji"])
    return df2


