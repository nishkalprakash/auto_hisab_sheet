# %% Init
from pathlib import Path

import pandas as pd
import time

from lib.hisab_func import *

# %%
# from lib.hisab_func import lprint


def auto_update(asset_name, conn, sh, ws_name="JBP"):
    df = get_df(asset_name, conn=conn)
    if df is None:
        lprint("DATA ALREADY UP TO DATE, SKIPPING")
        raise "UPDATED"

    lprint("DATA FETCHED FROM DB")

    #%% Upload the DataFrame to google_sheets
    insert_df(get_ws(sh, ws_name), df)
    lprint("GOOGLE SHEETS UPDATED")

    update_last_db_fetched()


if __name__ == "__main__":
    while 1:
        try:
            JBP_L = [
                "Kaushalya flat",
                "Mummy (Police)",
                "Mummy (Lawyer)",
                "Mummy (Doctor)",
                "Mummy (Loan)",
            ]

            ws_title_asset_l = {
                # "JBP": JBP_L, 
                # "Kaushalya Only":"Kaushalya flat",
                # "Mummy (Doctor) Only":"Mummy (Doctor)",
                # "Nikki": "Nikki", 
                # "Alka": "Alka", 
                # "Mummy": "Mummy",
                # "Imran":"Imu",
                "Debasis Sir":"DSm",
                # "Soumya":"Somo",
                # "Anshi":"Anshi",
                # "DJ":"DJ",
                }

            conn = get_db_conn(skip=True)
            # conn = get_db_conn(skip=False)
            sh = auth_pyghseets()
            for ws_name, asset_name in ws_title_asset_l.items():
                lprint(f"Running for {ws_name}".center(80,"*"))
                auto_update(asset_name, conn, sh, ws_name)

        except Exception as e:
            lprint(e)
        finally:
            lprint("SLEEPING FOR 6hrs")
            lprint("*" * 60)
            if conn:
                conn.close()
            time.sleep(60 * 60 * 6)
    