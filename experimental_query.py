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



#- [ ]  How the value on a cell was constructed (history of a cell)?

