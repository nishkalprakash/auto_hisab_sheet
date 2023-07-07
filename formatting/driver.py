# This file runs the whole setup

# %% Init
from pathlib import Path

import pandas as pd
# Read exported data

base = Path("res/")

# %% for exporting data

"""
"Date",     # DD.MM.YY HH:MM (eg 13.01.22 19:10)
"Account",  # Sender account
"Main Cat.",# Income Cat/Expence Cat/Receivers account
"Sub Cat.", # Income Sub Cat/Expence Sub Cat/Blank
"Contents", # Notes in MM app
"Amount",   # Amount
"Inc./Exp.",# Possible Values : Income/Expenses/Transfer-Out
"Details"   # Description in MM app
"""

"""
# header = [
#     "Date",
#     "Account",
#     "Main Cat.",
#     "Sub Cat.",
#     "Contents",
#     "Amount",
#     "Inc./Exp.",
#     "Details"
# ]
# df = pd.read_csv(base/"export.txt")
"""


# %% Importing data

df = pd.read_excel(base/"export.xlsx")
# pd.read_sql()

# %% keep only req cols

req_head = [
    'Date',
    'Account',
    'Category',
    'Note',
    'Note.1',
    'Income/Expense',
    'Amount'
]

df = df[req_head]
# %% keep only transfers
df = df[df['Income/Expense'] == 'Transfer-Out']

# %% separate data into 3 datasets
# ke=df[(df['Category']=="Kaushalya flat") | (df['Account']=="Kaushalya flat")]
ke = df[df['Category'] == "Kaushalya flat"]
md = df[df['Category'] == "Mummy (Doctor)"]
ml = df[df['Category'] == "Mummy (Lawyer)"]


# %% date agg

def agg_by_day(ke):
    ke['Note'].replace('Outside Food', 'Consumables', inplace=True)
    # ke['Date']=ke['Date'].dt.strftime('%y.%m.%d')
    note_len = 50
    aggr = {
        'Amount': sum,
        'Note.1': lambda x: " ".join(
            (str(i).replace(" ### ", ' ')[:(
                note_len-len(x)*4)//len(x)]+"... ") for i in x)
    }
    out = ke.groupby([df['Date'].dt.date, 'Note']).agg(aggr)
    out.rename(columns={'Note.1': 'Details'}, inplace=True)
    # out['Date']=out['Date'].apply(func=lambda x:".".join(x.split('.')[::-1]))
    return out


def agg_by_month(ke):
    # out=ke.groupby([pd.Grouper(key='Date',freq='M'),'Note']).agg(aggr)
    return ke.groupby([ke['Date'].dt.strftime("%Y.%m"), 'Note']).agg({
        'Amount': sum
    })


def agg_overall(ke):
    return ke.groupby('Note').agg({'Amount': sum})


# %%
for i in ['ke', 'ml', 'md']:
    # running the functions and saving the files by its var names XD
    for j in ['agg_overall','agg_by_day','agg_by_month']:
        eval(j)(eval(i)).to_excel(base/f'{i}_{j}.xlsx')
#%% Others

def agg_m(ke):
    return ke.groupby([ke['Date'].dt.strftime("%Y.%m")]).agg({'Amount': sum})
agg_m(ke).to_excel(base/"ke_agg_m.xlsx")