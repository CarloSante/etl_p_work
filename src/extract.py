import os

import pandas as pd
from src import ROOT_DIR, raw_path


def extract_csv(path, file_name, sep):
    try:
        df = pd.read_csv(ROOT_DIR / path / file_name, sep=sep)
        return df

    except FileNotFoundError as ex:
        print(ex)
        print("file non trovato")

def check_path():
    orig = ROOT_DIR / "data"
    obj = os.scandir(orig)
    val = 1
    diz = {}
    for entry in obj:
        if entry.is_dir():
            if any(os.scandir(entry.path)):
                print(val, "-", entry.name)
                diz[val] = entry.name
                val += 1

    n = int(input("\nDa quale cartella vuoi prendere il file? Premi 0 per uscire"))
    if n == 0: return None
    try:
        folder_name = diz[n]
        path = orig / folder_name
        return path
    except KeyError as ex:
        print("Nessuna cartella valida selezionata")
        return None


def check_files(path):
    obj = os.scandir(path)
    val = 1
    diz = {}
    for entry in obj:
        if entry.is_file():
            print(val, "-", entry.name)
            diz[entry.name] = val
            val += 1

    n = int(input("quale file vuoi aprire? Premi 0 per uscire"))

    try:
        txt =[key for key, val in diz.items() if val == n]
        f_name = txt[0]
        print(f_name)
        return f_name
    except (IndexError,ValueError) as ex:
        print("Nessun file valido selezionato")
        return None

if __name__ == "__main__":
    ...