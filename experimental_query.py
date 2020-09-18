import os
import sqlite3
import csv

conn = sqlite3.connect('example.db')

c = conn.cursor()

# How many rows are affected by an operation?

print("rows are affected by an operation")
seq_id = 25
for x in c.execute("select count(1) from (select distinct * from (SELECT old_row FROM ROW_CHANGES WHERE seq_id=? union SELECT row_id from cell_changes where seq_id=?))",(str(seq_id),str(seq_id))):
    print(x)

#- [ ]  How many columns are affected/produced by an operation?
print("cols are affected by an operation")
seq_id = 25
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

#- [ ]  What operations are dependent on an operation?
print("columns dependencies are affected by range of operation")
seq_id_first = 7
seq_id_last = 25
for x in c.execute("""
select count(1) from (select distinct col_dep from col_dependency where seq_id>=? and seq_id<=?)
""",(str(seq_id_first),str(seq_id_last))):
    print(x)

#- [ ]  What operations are affected by an operation?

#- [ ]  How the value on a cell was constructed (history of a cell)?

