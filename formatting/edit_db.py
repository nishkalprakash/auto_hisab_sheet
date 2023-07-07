## This file is experimenting with sqlite to create a more reproducabe tank

#%% init
from sqlite3 import Error,connect
from pathlib import Path


#%% init useful variables

base =Path('res/')
dbf="money_android.sqlite"


ke_filter="""(
    assetUid="87b7b5aa-4225-4c04-9052-9a9014775738" or toAssetUid="87b7b5aa-4225-4c04-9052-9a9014775738"
    )"""
ke="INOUTCOME"
zc="ZCONTENT"
zd="ZDATA"
qa=f'select * from {ke} where {ke_filter}'

# df=pd.read_sql(q,conn)

c=connect(base/dbf)
cr=c.cursor()

#%% Change Vegetables to conusmables and add Vegetables to desc

q=f"""UPDATE {ke}
    set 
        ZCONTENT="Consumables",
        ZDATA="Vegetables\n" || ZDATA
    where
        ZCONTENT="Vegetables"
        and
        ZDATA not like "%Vegetables%"
        and
        {ke_filter}
"""
res=cr.execute(q)
res.rowcount
#%% Change Vegetables to conusmables
q=f"""UPDATE {ke}
    set 
        ZCONTENT="Consumables"
    where
        ZCONTENT="Vegetables"
        and
        {ke_filter}
"""
# cr=c.cursor()
res=cr.execute(q)
res.rowcount

#%% fix ''
q=f"""UPDATE {ke}
    set 
        ZCONTENT="Accessories"
    where
        (
            ZCONTENT=""
            or
            ZCONTENT="Mustard oil"
        )
        and
        {ke_filter}
"""
# cr=c.cursor()
res=cr.execute(q)
res.rowcount

#%% Fix taaza
q=f"""UPDATE {ke}
    set 
        {zd}="Milk"
    where
        (
            {zd}="500ml cow's milk"
        )
        and
        {ke_filter}
"""
# cr=c.cursor()
res=cr.execute(q)
res.rowcount
#%% Fix dahi
q=f"""UPDATE {ke}
    set 
        {zd}="Dahi"
    where
        (
            {zd}="dahi"
        )
        and
        {ke_filter}
"""
# cr=c.cursor()
res=cr.execute(q)
res.rowcount
#%% Fix Outside_food 
## remove outside_food category altogether and make it consumable
## add outside_food to zdata
q=f"""UPDATE {ke}
    set 
        {zd}={zd} || "\n#outside_food",
        {zc}="Consumables"
    where
        (
            {zc}="Outside Food"
        )
        and
        {ke_filter}
"""
# cr=c.cursor()
res=cr.execute(q)
res.rowcount
#%% remove carriage return
q="""
update INOUTCOME set
ZDATA=REPLACE(ZDATA,"\r","")
"""

res=cr.execute(q)
res.rowcount

#%% Check unique ZCONTENT
q=f"""select {zd},COUNT({zd})/2 as count
    from {ke}
    where {ke_filter}
    group by {zd}
    order by count
    ;
"""
# cr=c.cursor()
res=cr.execute(q)
res.fetchall()
# res=cr.execute()
#%% Commit the changes to db
c.commit()
#%% Close the db
c.close()
#%%
"""
#%% edit vegetable to consumable and append vegetable to those entries if not exists
## FAIL 

## DONT USE PANDAS FOR SQLITE
## STICK TO SQL

# for i in df[df['ZCONTENT']=='Vegetables'].index:
#     print(i)
#     if 'vegetables' not in df[i].ZDATA.lower():
#         df.loc[i,'ZDATA']='Vegetables\n'+i.ZDATA
#         print(f"Modified entry {i.index}")
#     df.loc[i,'ZCONTENT']='Consumables'



#%%
# cur.close()

#%% close the db
# conn.commit()

# conn.close()

"""