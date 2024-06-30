import logging
import time

import pandas as pd
import requests
from psycopg2._json import Json
from psycopg2.extensions import register_adapter
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from columns_data import (
    raw_molecule_cols,
    raw_molecule_dtype_dict,
    raw_chembl_id_lookup_cols,
    raw_chembl_id_lookup_dtype_dict,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Load_ChemBL_Logger")

register_adapter(dict, Json)   # Register adapter for postgres to convert dicts to Json

aws_postgres = "postgresql://username:password@de-database.endpoint:5432/postgres"
local_postgres = "postgresql://Roman:qwerty@localhost:5432/new_chembl"



def chembl_molecules_api_call(partition=1000, offset=0, mode="molecules"):
    if mode == "molecules":
        path = f"molecule.json?limit={partition}&offset={offset}"
    elif mode == "chembl_id_lookups":
        path = f"chembl_id_lookup.json?limit={partition}&offset={offset}"

    logger.info("Trying to get response from API...")
    try:
        response = requests.get(f"https://www.ebi.ac.uk/chembl/api/data/{path}")
        response.raise_for_status()

    except requests.exceptions.RequestException as e:
        logger.error("No response from Quote url")
        raise e

    except requests.exceptions.HTTPError as e:
        logger.error("Bad request status code")
        raise e
    logger.info("response from API - OK")

    return response


def _total_rows_count(mode="molecules"):
    response = chembl_molecules_api_call(partition=1, mode=mode)
    data = response.json()
    total_rows = data["page_meta"]["total_count"]

    return total_rows


def molecules_to_df(response, mode="molecules"):
    data = response.json()

    logger.info("Trying to create molecules df from response...")

    df = pd.json_normalize(data[mode], max_level=0)
    if mode == "molecules":
        cols_list = raw_molecule_cols
    elif mode == "chembl_id_lookups":
        cols_list = raw_chembl_id_lookup_cols
    df = df[cols_list]
    logger.info(f"raw_{mode} df created")

    return df


def save_df_to_db(df, if_exists="append", mode="molecules"):

    if mode == "molecules":
        table_name = "staging_raw_molecules"
        dtype = raw_molecule_dtype_dict
    elif mode == "chembl_id_lookups":
        table_name = "staging_raw_chembl_id_lookups"
        dtype = raw_chembl_id_lookup_dtype_dict
    try:
        engine = create_engine(aws_postgres)
        df.to_sql(table_name, engine, if_exists=if_exists, dtype=dtype)
    except Exception as e:
        raise(e)


def ingestion_process(partition=1000, offset=0, mode="molecules"):
    total_rows = _total_rows_count()
    start = time.time()
    last_rows_time = start
    max_tries = 10
    tries = 0

    while offset < total_rows:
        logger.info(f"Now requesting {offset} - {offset + partition} rows...")
        response = chembl_molecules_api_call(partition=partition, offset=offset, mode=mode)
        df = molecules_to_df(response, mode=mode)
        df.index += offset + 1

        try:
            if offset == 0:
                save_df_to_db(df, if_exists="replace", mode=mode)
            else:
                save_df_to_db(df, mode=mode)

        except OperationalError as e:
            tries += 1
            logger.info(f"In except loop now. Try: {tries}")
            if tries >= max_tries:
                logger.error(f"Limit of reconnections is reached! ")
                raise e
            logger.error(f"Lost connection to db. Trying one more time...")
            logger.info(f"Fetching rows {offset} - {offset + partition} one more time")
            continue

        logger.info(f"Rows {offset} - {offset + partition} inserted!")

        new_rows_time = time.time()
        logger.info(f"time from start: {new_rows_time - start}")
        logger.info(f"time this insertation required: {new_rows_time - last_rows_time}")
        last_rows_time = new_rows_time

        offset += partition
        tries = 0


if __name__ == "__main__":
    ingestion_process(mode="chembl_id_lookups")
    ingestion_process()
