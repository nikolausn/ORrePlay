import os
import sqlite3
import csv

conn = sqlite3.connect('example.db')

c = conn.cursor()

# Create table
c.execute('''CREATE TABLE IF NOT EXISTS cell_changes
            (seq_id number, history_id text, op_name text, row_id text, cell_id text, old_value text, new_value text, row_dep text, col_dep text)''')

#"42","1592858128492","com.google.refine.model.changes.MassCellChange","3353","14","{'v': '1'}","{'v': 1}","3353","14"

c.execute('''CREATE TABLE IF NOT EXISTS col_changes
            (seq_id number, history_id text, op_name text, old_col text, new_col text)''')

#"6","1594402354527","com.google.refine.model.changes.ColumnMoveChange","5","6"

c.execute('''CREATE TABLE IF NOT EXISTS row_changes
            (seq_id number, history_id text, op_name text, old_row text, new_row text)''')
#"0","1594506908967","com.google.refine.model.changes.RowRemovalChange","9","10"

c.execute('''CREATE TABLE IF NOT EXISTS col_dependency
            (seq_id number, history_id text, op_name text, col text, col_dep text)''')
#"16","1592858547560","com.google.refine.model.changes.MassCellChange","19","6"

c.execute('''CREATE TABLE IF NOT EXISTS recipe_changes
            (seq_id number, history_id text, op_name text, columns_state text)''')

c.execute('DELETE FROM cell_changes')
c.execute('DELETE FROM col_changes')
c.execute('DELETE FROM row_changes')
c.execute('DELETE FROM col_dependency')
c.execute('DELETE FROM recipe_changes')

conn.commit()


# Insert a row of data
with open("cell_changes.log","r") as file:
    c_reader = csv.reader(file,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    for i,l in enumerate(c_reader):
        #print(len(l))
        l = list(l)
        l[0] = int(l[0])
        #c.execute("INSERT INTO cell_changes VALUES (?,?,?,?,?,?,?,?,?)",(int(l[0]),l[1],l[2],l[3],l[4],l[5],l[6],l[7],l[8]))
        try:
            c.execute("INSERT INTO cell_changes VALUES (?,?,?,?,?,?,?,?,?)",l)
        except BaseException as ex:
            print(l)
            raise ex
        if ((i+1)%5000) == 0:            
            #print(l)
            print(i)
            conn.commit()
            #break
conn.commit()

with open("col_changes.log","r") as file:
    c_reader = csv.reader(file,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    for i,l in enumerate(c_reader):
        #print(len(l))
        l = list(l)
        l[0] = int(l[0])
        try:
            c.execute("INSERT INTO col_changes VALUES (?,?,?,?,?)",l)
        except BaseException as ex:
            print(l)
            raise ex
        if ((i+1)%5000) == 0:            
            #print(l)
            print(i)
            conn.commit()
            #break
conn.commit()

with open("row_changes.log","r") as file:
    c_reader = csv.reader(file,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    for i,l in enumerate(c_reader):
        #print(len(l))
        l = list(l)
        l[0] = int(l[0])
        try:
            c.execute("INSERT INTO row_changes VALUES (?,?,?,?,?)",l)
        except BaseException as ex:
            print(l)
            raise ex
        if ((i+1)%5000) == 0:            
            #print(l)
            print(i)
            conn.commit()
            #break
conn.commit()        

with open("col_dependency.log","r") as file:
    c_reader = csv.reader(file,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    for i,l in enumerate(c_reader):
        #print(len(l))
        l = list(l)
        l[0] = int(l[0])
        try:
            c.execute("INSERT INTO col_dependency VALUES (?,?,?,?,?)",l)
        except BaseException as ex:
            print(l)
            raise ex
        if ((i+1)%5000) == 0:            
            #print(l)
            print(i)
            conn.commit()
            #break
conn.commit()        

with open("recipe_changes.log","r") as file:
    c_reader = csv.reader(file,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    for i,l in enumerate(c_reader):
        #print(len(l))
        l = list(l)
        l[0] = int(l[0])
        try:
            c.execute("INSERT INTO recipe_changes VALUES (?,?,?,?)",l)
        except BaseException as ex:
            print(l)
            raise ex
        if ((i+1)%5000) == 0:            
            #print(l)
            print(i)
            conn.commit()
            #break
conn.commit()        

"""
c.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")

# Save (commit) the changes
conn.commit()

# We can also close the connection if we are done with it.
# Just be sure any changes have been committed or they will be lost.
conn.close()


create_fact = open("facts.data","w",encoding="ascii", errors="ignore")

with open("cell_changes.log","r",encoding="ascii", errors="ignore") as file:
    for i,l in enumerate(file):
        create_fact.write("changes("+l.replace("\n","").replace("\\'","'").replace("\\t"," ")+").\n")
        #if i>1000:
        #    break

create_fact.close()
"""