import pandas as pd
import src.extract as e, src.load as l, src.transform as t

pd.set_option("display.max.rows", None)
pd.set_option("display.width", None)

if __name__ == "__main__":

    path = ""
    f_name = ""
    while True:
        scelta = input("\nScegli un'opzione:"
                       "\n1 - Visualizza il contenuto di un file"
                       "\n2 - inserisci i dati dei file processati nel db"
                       "\n3 - Inserire i dati in una singola tabella a partire dal file"
                       "\n4 - Visualizza il contenuto di una tabella del database"
                       "\n0 - Chiudi il programma\n").strip()

        try:
            if scelta == "1":
                path = e.check_path()
                f_name = e.check_files(path)
                if f_name != "" and f_name is not None:
                    print(e.extract_csv(path, f_name, ","))

            elif scelta == "2":
                l.etl_completa()

            elif scelta == "3":
                l.etl_singola()

            elif scelta == "4":
                l.select_tables()

            elif scelta == "0":
                print("Arrivederci")
                break
            else:
                raise ValueError
        except ValueError as ex:
            print("Opzione non valida")
