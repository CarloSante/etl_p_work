import os
from datetime import date, datetime
from decimal import Decimal

import pandas as pd

from src.logging_config import logger
import numpy as np
from dotenv import load_dotenv
import psycopg
from src import raw_path
import src.extract as e, src.transform as t

load_dotenv()

class DatabaseConnection:
    _instance = None
    _connection = None

    def __new__(cls):
        if cls._instance is None:
            #print("Nessuna istanza ancora: ne creo una nuova")
            cls._instance = super(DatabaseConnection, cls).__new__(cls)

            #print("Apro la connessione al DB")
            cls._connection = psycopg.connect(
                host=os.getenv("dbhost"),
                dbname=os.getenv("dbname"),
                user=os.getenv("dbuser"),
                password=os.getenv("dbpw"),
                port=os.getenv("dbport")
            )
        else:
            ...
        return cls._instance

    @property
    def connection(self):
        return self._connection

def load_clienti(df):
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
            sql = """
            CREATE TABLE IF NOT EXISTS clienti
            (id_cliente character(32) PRIMARY KEY,
            regione character varying(200),
            provincia character varying(200),
            CAP character(5) NOT NULL CHECK (CAP ~ '^[0-9]{5}$'));
            """

            cur.execute(sql)

            sql = """
                  INSERT INTO clienti (id_cliente, regione, provincia, CAP) VALUES (%s,%s,%s,%s)
                  ON CONFLICT DO NOTHING;
                  """
            for index, row in df.iterrows():
                cur.execute(sql, row.to_list())
            conn.connection.commit()

def load_venditori(df):
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS venditori
        (id_venditore character(32) PRIMARY KEY,
        regione character varying(200));
        """

        cur.execute(sql)

        sql = """
              INSERT INTO venditori (id_venditore, regione) VALUES (%s,%s)
              ON CONFLICT DO NOTHING;
              """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())
        conn.connection.commit()

def load_categorie(df):
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS categorie
        (id_categoria serial PRIMARY KEY,
        nome_eng_categoria character varying(200) UNIQUE,
        nome_ita_categoria character varying(200) UNIQUE);
        """

        cur.execute(sql)

        sql = """
              INSERT INTO categorie (nome_eng_categoria, nome_ita_categoria) VALUES (%s,%s)
              ON CONFLICT DO NOTHING;
              """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())
        conn.connection.commit()

def load_prodotti(df):
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS prodotti
        (id_prodotto character(32) PRIMARY KEY,
        nome_ita_categoria_fk character varying(200) NOT NULL,
        lunghezza_nome integer,
        lunghezza_descrizione integer,
        numero_foto integer,
        FOREIGN KEY (nome_ita_categoria_fk) REFERENCES categorie(nome_ita_categoria));
        """

        cur.execute(sql)

        sql = """
              INSERT INTO prodotti (id_prodotto, nome_ita_categoria_fk, lunghezza_nome,
              lunghezza_descrizione, numero_foto)
              VALUES (%s,%s,%s,%s,%s)
              ON CONFLICT DO NOTHING;
              """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())
        conn.connection.commit()

def load_ordini(df):
    df = df.replace({np.nan: None})
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS ordini
        (id_ordine character(32) PRIMARY KEY,
        id_cliente_fk character(32) NOT NULL,
        stato_ordine VARCHAR(100) NOT NULL,
        data_e_ora_acquisto TIMESTAMP NOT NULL,
        data_e_ora_consegna TIMESTAMP CHECK(data_e_ora_consegna > data_e_ora_acquisto),
        data_di_consegna_stimata date CHECK(data_di_consegna_stimata >= DATE(data_e_ora_acquisto)),
        FOREIGN KEY (id_cliente_fk) REFERENCES clienti(id_cliente));
        """

        cur.execute(sql)

        sql = """
              INSERT INTO ordini (id_ordine, id_cliente_fk, stato_ordine,
              data_e_ora_acquisto, data_e_ora_consegna, data_di_consegna_stimata)
              VALUES (%s,%s,%s,%s,%s,%s)
              ON CONFLICT DO NOTHING;
              """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())
        conn.connection.commit()

def load_articoli(df):
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
        sql = """
        CREATE TABLE IF NOT EXISTS articoli
        (id_articolo serial PRIMARY KEY,
        id_ordine_fk character(32) NOT NULL,
        numero_oggetti  integer NOT NULL CHECK(numero_oggetti > 0),
        id_prodotto_fk character(32) NOT NULL,
        id_venditore_fk character(32) NOT NULL,
        prezzo NUMERIC(10,2) CHECK(prezzo >= 0),
        prezzo_spedizione NUMERIC(10,2) CHECK(prezzo_spedizione >= 0),
        FOREIGN KEY (id_ordine_fk) REFERENCES ordini(id_ordine),
        FOREIGN KEY (id_prodotto_fk) REFERENCES prodotti(id_prodotto),
        FOREIGN KEY (id_venditore_fk) REFERENCES venditori(id_venditore),
        CONSTRAINT uc_articoli UNIQUE (id_ordine_fk, numero_oggetti, id_prodotto_fk, id_venditore_fk));
        """

        cur.execute(sql)

        sql = """
              INSERT INTO articoli (id_ordine_fk, numero_oggetti, id_prodotto_fk,
              id_venditore_fk, prezzo, prezzo_spedizione)
              VALUES (%s,%s,%s,%s,%s,%s)
              ON CONFLICT DO NOTHING;
              """
        for index, row in df.iterrows():
            cur.execute(sql, row.to_list())
        conn.connection.commit()

def etl_completa():
    lista_file = [f for f in os.listdir(raw_path)]

    diz_file = {k: [] for k in TRANSFORMERS.keys()}
    for f in lista_file:
        for k in TRANSFORMERS.keys():
            if k in f:
                diz_file[k].append(f)

    for k, file_list in diz_file.items():
        dfs = []
        for f in file_list:
            try:
                df = e.extract_csv(raw_path, f, ",")
                dfs.append(df)
                logger.info(f"File {f} caricato per {k}")
                print(f"File {f} caricato per {k}")
            except Exception as ex:
                logger.error(f"Errore caricando file {f} ({k}): {ex}")
                print(f"Errore caricando file {f} ({k}): {ex}")
                continue

        df_concat = pd.concat(dfs, ignore_index=True)

        transformer = TRANSFORMERS.get(k)
        if transformer:
            try:
                df_concat = transformer(df_concat)
                logger.info(f"Trasformazione completata per {k}")
                print(f"\nTrasformazione completata per {k}")
            except Exception as ex:
                logger.error(f"Errore nella trasformazione di {k}: {ex}")
                print(f"\nErrore nella trasformazione di {k}: {ex}")
                continue
        else:
            logger.warning(f"Nessuna trasformazione definita per {k}")
            continue

        loader = LOADERS.get(k)
        if loader:
            try:
                loader(df_concat)
                logger.info(f"Caricati {len(df_concat)} record di {k}")
                print(f"\nCaricati {len(df_concat)} record di {k}\n")
            except Exception as ex:
                logger.error(f"Errore nel caricamento di {k}: {ex}")
                print(f"\nErrore nel caricamento di {k}: {ex}")
        else:
            logger.warning(f"Nessun loader definito per {k}")

def select_tables():
    n_tabella = ""
    conn = DatabaseConnection()
    with conn.connection.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = [row[0] for row in cur.fetchall()]

    conn.connection.commit()

    mapping = {str(i+1): t for i, t in enumerate(tables)}
    while True:
        print("\n---Scegli una tabella---")

        for k, v in mapping.items():
            print(f"{k} - {v}")

        print("0 - esci dal programma")

        tab = input("> ").strip()

        if tab in mapping:
            n_tabella = mapping[tab]
            break

        elif tab == "0":
            break
        else:
            print("Scelta non valida, riprova.")

    with conn.connection.cursor() as cur:
        sql = f"""
                    SELECT * FROM {n_tabella}
               """
        cur.execute(sql)
        for record in cur:
            clean_record = tuple(
                val.strftime("%Y-%m-%d") if isinstance(val, date) and not isinstance(val, datetime)
                else val.strftime("%Y-%m-%d %H:%M:%S") if isinstance(val, datetime)
                else f"{val:.2f}" if isinstance(val, Decimal)
                else val
                for val in record
            )
            print(clean_record)
        conn.connection.commit()

TRANSFORMERS = {
    "customers": t.transform_customers,
    "sellers": t.transform_sellers,
    "categories": t.transform_categories,
    "products": t.transform_products,
    "orders": t.transform_orders,
    "items": t.transform_items
}

LOADERS = {
    "customers": load_clienti,
    "sellers": load_venditori,
    "categories": load_categorie,
    "products": load_prodotti,
    "orders": load_ordini,
    "items": load_articoli
}

def etl_singola():
    file_name = e.check_files(raw_path)
    df = e.extract_csv(raw_path, file_name, ",")

    transformer = None
    loader = None

    for key, funz in TRANSFORMERS.items():
        if key in file_name:
            transformer = funz
            break

    for key, funz in LOADERS.items():
        if key in file_name:
            loader = funz
            break

    if transformer:
        df = transformer(df)
    else:
        print(f"Nessuna trasformazione definita per {file_name}")
        return

    if loader:
        loader(df)
    else:
        print(f"Nessun loader definito per {file_name}")

if __name__ == "__main__":
    ...
