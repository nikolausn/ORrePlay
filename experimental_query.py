import os
import sqlite3
import csv

conn = sqlite3.connect('example.db')

c = conn.cursor()

# How many rows are affected by an operation?

print("rows are affected by an operation")
seq_id = 35
for x in c.execute("select count(1) from (select distinct * from (SELECT old_row FROM ROW_CHANGES WHERE seq_id=? union SELECT row_id from cell_changes where seq_id=?))",(str(seq_id),str(seq_id))):
    print(x)

#- [ ]  How many columns are affected/produced by an operation?
print("cols are affected by an operation")
seq_id = 35
for x in c.execute("select count(1) from (select distinct * from (SELECT old_col FROM COL_CHANGES WHERE seq_id=? union SELECT cell_id from cell_changes where seq_id=?))",(str(seq_id),str(seq_id))):
    print(x)

#- [ ]  How many rows are affected on a certain range of operations (multiple operations)?
print("rows are affected by range of operations")
seq_id_first = 7
seq_id_last = 25
for x in c.execute("select count(1) from (select distinct * from (SELECT old_row FROM ROW_CHANGES WHERE seq_id>=? and seq_id<=? union SELECT row_id from cell_changes where seq_id>=? and seq_id<=?))",(str(seq_id_first),str(seq_id_last),str(seq_id_first),str(seq_id_last))):
    print(x)


#- [ ]  How many columns are affected/produced by a certain range of operations?

print("cells are affected by range of operations")
seq_id_first = 7
seq_id_last = 25
for x in c.execute("select count(1) from (select distinct * from (SELECT old_col FROM COL_CHANGES WHERE seq_id>=? and seq_id<=? union SELECT cell_id from cell_changes where seq_id>=? and seq_id<=?))",(str(seq_id_first),str(seq_id_last),str(seq_id_first),str(seq_id_last))):
    print(x)


#- [ ]  What are the dependencies (input) of an operation?
print("columns dependencies are affected by an operation")
seq_id = 18
for x in c.execute("""
select count(1) from (select distinct col_dep from col_dependency where seq_id=?)
""",(str(seq_id),)):
    print(x)

#- [ ]  What are the dependencies for a range of operations?
print("columns dependencies are affected by range of operation")
seq_id_first = 7
seq_id_last = 25
for x in c.execute("""
select count(1) from (select distinct col_dep from col_dependency where seq_id>=? and seq_id<=?)
""",(str(seq_id_first),str(seq_id_last))):
    print(x)

#- [ ]  What operations are dependency of an operation?
print("What operations are dependency of an operation?")

seq_id = 35
q1 = c.execute("""
select cell_id from cell_changes where seq_id = ?
UNION
select new_col from col_changes where seq_id = ?
UNION
select col_dep from col_dependency where seq_id = ?
""",(str(seq_id),str(seq_id),str(seq_id)))

def get_parents(seq_id,cell_id,new_seq_id=999999,old_col=-1):
    col_id = cell_id
    #print(col_id)
    q2 = c.execute("""
    select seq_id,old_col from col_changes where new_col=? and seq_id>=? order by seq_id desc limit 1
    """,(str(col_id),str(seq_id)))
    #print(seq_id)
    #new_seq_id = 9999999
    for y in q2:
        new_seq_id = y[0]
        old_col = y[1]

    q3 = c.execute("""select distinct * from(
    select seq_id,op_name from cell_changes where seq_id < ? and seq_id > ? and cell_id=?
    union
    select seq_id,op_name from col_dependency where seq_id < ?  and seq_id > ? and col=?
    )
    """,(new_seq_id,seq_id,col_id,new_seq_id,seq_id,col_id))
    all_ops = [y for y in q3]
    #print(all_ops)
    q4 = c.execute("""    
    select seq_id,col_dep from col_dependency where seq_id < ?  and seq_id > ? and col=?
    """,(new_seq_id,seq_id,col_id))

    for y in q4:
        all_ops =  all_ops + get_parents(y[0],y[1],new_seq_id,old_col)

    if new_seq_id!=999999:
        all_ops = all_ops + get_parents(new_seq_id,old_col)

    return all_ops 
    #print([y for y in q3])

for x in q1:
    # get the first col_changes
    col_id = x[0]
    print(get_parents(seq_id,x[0]))

    '''
    q2 = c.execute("""
    select seq_id,new_col from col_changes where new_col=? and seq_id>=? order by seq_id desc limit 1
    """,(str(col_id),str(seq_id)))
    new_seq_id = 9999999
    for y in q2:
        new_seq_id = y[0]
        new_col = y[1]
    q3 = c.execute("""select distinct * from(
    select seq_id,op_name from cell_changes where seq_id < ? and seq_id > ? and cell_id=?
    union
    select seq_id,op_name from col_dependency where seq_id < ?  and seq_id > ? and col=?
    )
    """,(new_seq_id,seq_id,col_id,new_seq_id,seq_id,col_id))
    print([y for y in q3])
    '''

#- [ ]  What operations are affected by an operation?

print("What operations are affected by an operation?")

seq_id = 35
q1 = c.execute("""
select cell_id from cell_changes where seq_id = ?
UNION
select new_col from col_changes where seq_id = ?
UNION
select col_dep from col_dependency where seq_id = ?
""",(str(seq_id),str(seq_id),str(seq_id)))


def get_childrens(seq_id,cell_id,new_seq_id=-1,new_col=-1):
    col_id = cell_id
    #print(col_id)
    q2 = c.execute("""
    select seq_id,new_col from col_changes where old_col=? and seq_id<=? order by seq_id desc limit 1
    """,(str(col_id),str(seq_id)))
    #print(seq_id)
    #new_seq_id = 9999999
    for y in q2:
        new_seq_id = y[0]
        new_col = y[1]

    q3 = c.execute("""select distinct * from(
    select seq_id,op_name from cell_changes where seq_id > ? and seq_id < ? and cell_id=?
    union
    select seq_id,op_name from col_dependency where seq_id > ?  and seq_id < ? and col_dep=?
    )
    """,(new_seq_id,seq_id,col_id,new_seq_id,seq_id,col_id))
    all_ops = [y for y in q3]
    #print(all_ops)
    q4 = c.execute("""    
    select seq_id,col_dep from col_dependency where seq_id > ?  and seq_id < ? and col=?
    """,(new_seq_id,seq_id,col_id))

    for y in q4:
        all_ops =  all_ops + get_parents(y[0],y[1],new_seq_id,new_col)

    if new_seq_id!=-1:
        all_ops = all_ops + get_parents(new_seq_id,new_col)

    return all_ops 
    #print([y for y in q3])

for x in q1:
    # get the first col_changes
    print(get_childrens(seq_id,x[0]))

    '''
    col_id = x[0]
    q2 = c.execute("""
    select seq_id,new_col from col_changes where old_col=? and seq_id<=? order by seq_id desc limit 1
    """,(str(col_id),str(seq_id)))
    new_seq_id = -1
    for y in q2:
        new_seq_id = y[0]
        new_col = y[1]
    q3 = c.execute("""select distinct * from(
    select seq_id,op_name from cell_changes where seq_id > ? and seq_id < ? and cell_id=?
    union
    select seq_id,op_name from col_dependency where seq_id > ?  and seq_id < ? and col_dep=?
    )
    """,(new_seq_id,seq_id,col_id,new_seq_id,seq_id,col_id))
    print([y for y in q3])
    '''


from or_extract import extract_project,read_dataset,search_cell_column
file_name = "airbnb_dirty-csv.openrefine.tar.gz"
locex,_ = extract_project(file_name)
# extract data
dataset = read_dataset(locex)

#- [ ]  How the value on a cell was constructed (history of a cell)?
col_id = 23
#row_id = 456
row_id = 65
start_seq = 0
# select the first column / row changes for anchor
print("history of a cell")

def history_cell(col_id,row_id,start_seq=0,latest_value="",start_row_id=None):
    #print("cell trace",col_id,row_id,start_seq)
    if start_row_id==None:
        start_row_id = row_id

    if latest_value=="":
    #if start_seq==0:
        latest_value = dataset[2]["rows"][int(row_id)]["cells"][int(col_id)]
        print("cell trace",col_id,row_id,start_seq,"latest value:",latest_value)

    x, y = search_cell_column(dataset[0]["cols"],col_id)
    #print(min(dataset[2]["rows"].keys()))
    #print("cell trace",col_id,row_id,start_seq,"latest value:",dataset[2]["rows"][int(row_id)]["cells"][int(col_id)])
    q1 = c.execute("""
    select * from (
    select seq_id,old_col,'col_changes' from col_changes where new_col = ? and seq_id>=?
    UNION
    select seq_id,old_row,'row_changes' from row_changes where new_row = ? and seq_id>=?
    ) 
    order by seq_id asc limit 1
    """,(str(col_id),str(start_seq),str(row_id),str(start_seq)))

    xx = [x for x in q1]
    if len(xx)>0:
        xx = xx[0]        
        #print(xx)
        change_seq = xx[0]
        change_cc = xx[1]
        change_type = xx[2]
    else:
        change_seq = 9999999
        change_type = None

    q2 = c.execute("""
    select * from cell_changes where cell_id=? and row_id=? and seq_id>=? and seq_id<? order by seq_id asc
    """,(str(col_id),str(row_id),str(start_seq),str(change_seq)))
    
    
    all_result = [x for x in q2]
    #print(all_result)

    if len(all_result)>0:
        latest_value = all_result[-1][6]
        print("cell trace",col_id,row_id,start_seq,"latest value:",latest_value)

    #elif latest_value=="":
    # first time, no change on cell_changes, set latest_value as

    q3 = c.execute("""
    select col_dep from col_dependency where col=? and seq_id>=? and seq_id<? order by seq_id asc
    """,(str(col_id),str(start_seq),str(change_seq)))

    #print("seq_dep",[x for x in q3])
    for x in q3:
        print("column dependency:",x[0])
        #all_result = all_result + history_cell(x[0],row_id,change_seq)
        all_result = all_result + history_cell(x[0],start_row_id,0,start_row_id=start_row_id)

    if change_type=="col_changes":
        col_id = change_cc
        print("col_changes:",col_id,row_id,latest_value)        
    elif change_type=="row_changes":
        row_id = change_cc
        print("row_changes:",col_id,row_id,latest_value)        
    else:
        pass

    # dependency for the column

    if change_seq!=9999999:
        all_result = all_result + history_cell(col_id,row_id,change_seq+1,latest_value,start_row_id=start_row_id)

    return all_result

print("\n".join([str(x) for x in history_cell(col_id,row_id)]))

'''
q1 = c.execute("""
select * from (
select seq_id,old_col,'col_changes' from col_changes where new_col = ?
UNION
select seq_id,old_row,'row_changes' from row_changes where new_row = ?
) order by seq_id desc limit 1
""",(str(col_id),str(row_id)))

xx = [x for x in q1]
if len(xx)>0:
    xx = xx[0]        
    print(xx)
    change_seq = xx[0]
    change_cc = xx[1]
    change_type = xx[2]
else:
    change_seq = 9999999
    change_type = None
q2 = c.execute("""
select * from cell_changes where cell_id=? and row_id=? and seq_id>=? and seq_id<?
""",(str(col_id),str(row_id),str(start_seq),str(change_seq)))

print([x for x in q2])

if change_type=="col_changes":
    col_id = change_cc
elif change_type=="row_changes":
    row_id = change_cc

#row_id = 463
start_seq = change_seq
# select the first column / row changes for anchor
#print("history of a cell")
q1 = c.execute("""
select * from (
select seq_id,old_col,'col_changes' from col_changes where new_col = ? and seq_id>?
UNION
select seq_id,old_row,'row_changes' from row_changes where new_row = ? and seq_id>?
) order by seq_id desc limit 1
""",(str(col_id),str(start_seq),str(row_id),str(start_seq)))

xx = [x for x in q1]
if len(xx)>0:
    xx = xx[0]        
    print(xx)
    change_seq = xx[0]
    change_type = xx[2]
else:
    change_seq = 9999999
    change_type = None

print(col_id,row_id,start_seq,change_seq)
q2 = c.execute("""
select * from cell_changes where cell_id=? and row_id=? and seq_id>=? and seq_id<?
""",(str(col_id),str(row_id),str(start_seq),str(change_seq)))

print([x for x in q2])
'''


