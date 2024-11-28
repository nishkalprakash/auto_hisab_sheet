# %% Init
from pathlib import Path
from lib.hisab_func import *

# %% For NIKKI Di

# q = get_q_placeholder().format(asset_name='Nikki',extra_filter="")
df=get_df(asset_name='Nikki',allow_skip=False)

df[df.toAccount=='Mummy (Loan)']
#%% Filter new entries and remove trans with Mummy (Loan) account
### 
df[(df.Category=='Repayment of old loan')]
#%%
df=df[
    (df.toAccount!='Mummy (Loan)') & 
    (df.Category!='Repayment of old loan') &
    (df.Date>=pd.to_datetime('2021-07-01'))
    ]
Account_Nikki=df[df.Account=='Nikki']
toAccount_Nikki=df[df.toAccount=='Nikki']
#%%
sum_after_july=Account_Nikki.Amount.sum()+toAccount_Nikki.Amount.sum()
sum_after_july
#%% Insert data into sheets

# sh=auth_pyghseets()
# ws=get_ws(sh,'Account_Nikki')
# insert_df(ws,Account_Nikki)
# ws=get_ws(sh,'toAccount_Nikki')
# insert_df(ws,toAccount_Nikki)


#%% Sleep
import time
time.sleep(10)



# %% For MUMMY

# q = get_q_placeholder().format(asset_name='Mummy',extra_filter="")
df=get_df(asset_name='Mummy',allow_skip=False)

df[df.toAccount=='Mummy (Doctor)']
#%% Filter new entries and remove trans with Mummy (Loan) account
df=df[
    (df.toAccount!='Mummy (Loan)') & 
    (df.Date>=pd.to_datetime('2021-07-01'))
    ]
Account_Mummy=df[df.Account=='Mummy']
toAccount_Mummy=df[df.toAccount=='Mummy']
#%%
sum_after_july=Account_Mummy.Amount.sum()-toAccount_Mummy.Amount.sum()
sum_after_july
#%% Insert data into sheets

# sh=auth_pyghseets()
# ws=get_ws(sh,'Account_Mummy')
# insert_df(ws,Account_Mummy)
# ws=get_ws(sh,'toAccount_Mummy')
# insert_df(ws,toAccount_Mummy)
# %%
