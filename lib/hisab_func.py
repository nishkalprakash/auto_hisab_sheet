#%% useful functiions
from pathlib import Path
import pygsheets
import pandas as pd


def lprint(*args, **kwargs):
    import datetime

    log_path = "auto_log.txt"
    with open(log_path, "a") as f:
        __builtins__["print"](datetime.datetime.now(), " - ", file=f, *args, **kwargs)
    print(*args,**kwargs)

#%% DB functions
def get_latest_db_name():
    return max(
        Path("G:/My Drive/MoneyManager").glob("*.mmbak"),
        key=lambda x: x.stat().st_mtime,
    )


def get_db_conn(db_name=None, skip=True):
    """Gets the latest DB from gdrive
        # NEEDS GDRIVE FOR DESKTOP RUNNING

    Args:
        db_name (str, optional): Name of the MMBAK file including the extension (eg. "MMAuto[FO220117](17-01-22-104255).mmbak"). Defaults to "".
        skip (bool, optional): If True: Checks db_name with contents of "last_db_fetched.txt", if matched: returns False. Defaults to True.

    Returns:
        sqlite3.connect: sqlite database connection
    """
    import sqlite3

    if db_name is None:
        db_name = get_latest_db_name()
    if skip:
        if (
            Path("last_db_fetched.txt").exists()
            and Path(db_name).name == Path("last_db_fetched.txt").read_text()
        ):
            lprint("NO CHANGE SINCE LAST SYNC")
            return False
    return sqlite3.connect(db_name)


def get_sqlcols():
    sqlcols = {
        "AID": "AID",
        "Date": "WDATE",
        "Account": "(select NIC_NAME from ASSETS where uid=assetUid)",
        "toAccount": "(select NIC_NAME from ASSETS where uid=toAssetUid)",
        "Category": "ZCONTENT",
        "Details": "ZDATA",
        "Amount": "AMOUNT_ACCOUNT",
    }
    return sqlcols


def get_q_placeholder(asset_name=None, extra_filter=""):
    """
    asset_name: , seperated string of account names
    """
    q = (
        f'SELECT {", ".join([v+" as "+k for k,v in get_sqlcols().items()])}'
        + """
        FROM INOUTCOME
        WHERE
        (
            toAssetUid in (select uid from ASSETS where NIC_NAME in ("{asset_name}"))
            or
            AssetUid in (select uid from ASSETS where NIC_NAME in ("{asset_name}"))
        )
        and
        DO_TYPE="3"
        {extra_filter}
        order by Date desc
    """
    )
    if asset_name:
        return q.format(asset_name=asset_name, extra_filter=extra_filter)
    return q


def get_df(asset_name, extra_filter="",conn=None,db_name=None,update=True):
    multi=False
    if type(asset_name) == list:
        asset_l=asset_name
        asset_name='","'.join(asset_name)
        multi=True
    q=get_q_placeholder(asset_name,extra_filter)
    if conn is None and (conn:=get_db_conn(db_name,update)) is None:
        return None
    df=pd.read_sql(
        q, conn, parse_dates=["Date"], index_col="AID", coerce_float=True
    )
    if multi:
        df.loc[df.Account.isin(asset_l),'Amount']=df.loc[df.Account.isin(asset_l),'Amount'].apply(lambda x: -x)
    else:
        df.loc[df.Account==asset_name,'Amount']=df.loc[df.Account==asset_name,'Amount'].apply(lambda x: -x)
    return df


def update_last_db_fetched(dbname=None):
    if dbname is None:
        dbname = get_latest_db_name()
    return Path("last_db_fetched.txt").write_text(dbname.name)


#%% Pygsheets functions


def auth_pyghseets(sh_id="1OtMfETXwXQBZR15P0BHbRuaqGVrvI9U9BHLCENcpgLM"):
    try:
        gc = pygsheets.authorize(service_account_file='service_account.json')
    except pygsheets.AuthenticationError:
        # gc = pygsheets.authorize()
        pass

    #%%  Open SpreadSheet
    return gc.open_by_key(sh_id)


def get_ws(sh, ws_name):
    try:
        return sh.worksheet_by_title(ws_name)
    except pygsheets.WorksheetNotFound:
        return sh.add_worksheet(ws_name, rows=1, cols=1)


def insert_into_ws(ws, df):
    ws.set_dataframe(df, copy_index=True, start=(1, 1), extend=True, copy_head=True)


def reset_ws(ws):
    ws.frozen_rows = 0
    ws.clear(fields="*")
    ws.resize(1, 1)


def format_ws(ws):
    #%% bold and center header
    model_cell = ws.cell((1, 1))
    model_cell.set_value("AID")
    model_cell.set_text_format("bold", True)
    model_cell.set_text_format("fontSize", 12)
    model_cell.set_vertical_alignment(pygsheets.VerticalAlignment.MIDDLE)
    model_cell.set_horizontal_alignment(pygsheets.HorizontalAlignment.CENTER)
    pygsheets.DataRange((1, 1), (1, ws.cols), worksheet=ws).apply_format(model_cell)

    #%% Auto fit conlumns
    ws.adjust_column_width(1, ws.cols)
    #%% Format Amount column
    amnt_cell = ws.cell((2, ws.cols))
    amnt_cell.set_number_format(
        pygsheets.FormatType.CURRENCY,
        "[$₹][>9999999]##\,##\,##\,##0.00;[$₹][>99999]##\,##\,##0.00;[$₹]##,##0.00",
    )
    amnt_cell.set_horizontal_alignment(pygsheets.HorizontalAlignment.RIGHT)
    pygsheets.DataRange(
        amnt_cell.address, (ws.rows, ws.cols), worksheet=ws
    ).apply_format(amnt_cell)
    #%% Freeze header
    ws.frozen_rows = 1


def insert_df(ws, df=pd.DataFrame()):
    # ws=auth_pyghseets(sh_id).get_ws(ws_name)
    reset_ws(ws)
    insert_into_ws(ws, df)
    format_ws(ws)


# %% data agg functions


def sm(det_l, m=75):
    # m = 50
    if len(det_l) == 0 or m < 1:
        return ""
    if len(det_l.iloc[0]) <= m:
        return (
            " ".join([det_l.iloc[0], sm(det_l.iloc[1:], m - len(det_l.iloc[0]) - 1)])
            .strip()
            .replace("\n", ", ")
        )
    if m >= 3:
        return det_l.iloc[0][: m - 3] + "..."
    return ""


def agg_by_day(dt) -> pd.DataFrame:
    aggr = {
        "Account": "first",
        "TransToAccount": "first",
        "Details": sm,
        "Amount": sum,
    }
    out = dt.groupby([dt["Date"].dt.date, "Category"]).agg(aggr)
    return out


def agg_by_month(dt):
    # out=dt.groupby([pd.Grouper(dty='Date',freq='M'),'Category']).agg(aggr)
    return dt.groupby([dt["Date"].dt.strftime("%Y.%m"), "Category"]).agg(
        {"Amount": sum}
    )


def agg_overall(dt):
    return dt.groupby("Category").agg({"Amount": sum})


def agg_m(dt) -> pd.DataFrame:
    return dt.groupby([dt["Date"].dt.strftime("%Y.%m")]).agg({"Amount": sum})
