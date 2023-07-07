# %% [markdown]
# # Code to Format Data into Human Readable


# %% Init
from pathlib import Path
from lib.hisab_func import *
import pandas as pd
# Read exported data

# %% [markdown]
# ### Reading data into pandas from sql

# %%
# def get_df_d(asset_d,db_name,q=""):
#     def get_db_conn():
#         import sqlite3
#         dbf=db_name
#         return sqlite3.connect(base/dbf)

#     def get_df(asset_name,conn,q=""):
#         sqlcols={
#             'Date':'WDATE',
#             'Category':'ZCONTENT',
#             'Details':'ZDATA',
#             'Amount':'AMOUNT_ACCOUNT',
#             # 'Account':'assetUid',
#             # 'toAccount':'toAssetUid'
#         }
#         if q=="":
#             q=f"""
#             SELECT 
#                 {", ".join([v+" as "+k for k,v in sqlcols.items()])},
#                 (select NIC_NAME from ASSETS where uid=assetUid) as Account,
#                 (select NIC_NAME from ASSETS where uid=toAssetUid) as TransToAccount
#                 FROM INOUTCOME
#                 WHERE
#                 toAssetUid in (select uid from ASSETS where NIC_NAME="{asset_name}")
#                 and
#                 AssetUid in (select uid from ASSETS where NIC_NAME="Churail")
#                 and
#                 Category not like "Kaushalya Ghar Kharcha"
#                 order by Date
#             """ 
#         df = pd.read_sql(q,conn,parse_dates=['Date'],coerce_float=True)
#         return df

#     # %% separate data into 3 datasets
#     df_d={}
#     conn=get_db_conn()

#     # assetUid_d=get_assetUid_d(asset_d,conn)
#     for fname,asset_name in asset_d.items():
#         print(fname,asset_name)
#         df_d[fname]=get_df(asset_name,conn,q)
#     conn.close()
    
#     return df_d




def sm(det_l,m=75):
    # m = 50
    if len(det_l)==0 or m<1:
        return ""
    if len(det_l.iloc[0])<=m:
        return " ".join([
            det_l.iloc[0],
            sm(det_l.iloc[1:],m-len(det_l.iloc[0])-1)
            ]).strip().replace('\n',', ')
    if m>=3:
        return det_l.iloc[0][:m-3]+"..."
    return ""

    
def agg_by_day(dt) -> pd.DataFrame:
    aggr = {
        'Account':'first',
        'toAccount':'first',
        'Details': sm,
        'Amount': sum,
    }
    out = dt.groupby([dt['Date'].dt.date, 'Category']).agg(aggr)
    return out

def agg_by_month(dt):
    # out=dt.groupby([pd.Grouper(dty='Date',freq='M'),'Category']).agg(aggr)
    return dt.groupby([dt['Date'].dt.strftime("%Y.%m"), 'Category']).agg({
        'Amount': sum
    })

def agg_overall(dt):
    return dt.groupby('Category').agg({'Amount': sum})

def agg_m(dt) -> pd.DataFrame:
    return dt.groupby([dt['Date'].dt.strftime("%Y.%m")]).agg({'Amount': sum})

# %%

#%% functions list
def sql_injection(*args):
    n="_".join(args)
    asset_d[n]=f'''{'" OR NIC_NAME="'.join(asset_d.pop(k) for k in args)}'''



# %% [markdown]
# ### Getting df from sql

# %%
## Get excel file for nikki di's hisab
db_name=get_latest_db_name()
asset_d={
    'ke':'Kaushalya flat',
    # 'mp':'Mummy (Police)',
    'ml':'Mummy (Lawyer)',
    'md':"Mummy (Doctor)"
    # Mummy (Police)" OR NIC_NAME="Mummy (Lawyer)
}

fun_l=[
    agg_overall,
    agg_by_day,
    # agg_by_month,
    # agg_m
]

# sql_injection('ke','md','ml')
# df_d=get_df_d(asset_d,db_name)


# %%

for shrt_name,asset_name in asset_d.items():
    # running the functions and saving the files by its var names -_-
    df=get_df(asset_name=asset_name,update=False)
    for fun in fun_l:
        fname=Path('res')/f'{shrt_name}.xlsx'
        fn=fun.__name__
        if fname.exists():
            with pd.ExcelWriter(fname,mode='a',if_sheet_exists='replace') as writer: 
                fun(df).to_excel(excel_writer=writer,sheet_name=fn,float_format="%.2f",freeze_panes=(1,0))
        else:
            fun(df).to_excel(excel_writer=fname,sheet_name=fn,float_format="%.2f",freeze_panes=(1,0))

        print(f"Done for {fname}")
# %%
