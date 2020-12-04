import os

create_fact = open("facts.data","w",encoding="ascii", errors="ignore")

with open("cell_changes.log","r",encoding="ascii", errors="ignore") as file:
    for i,l in enumerate(file):
        create_fact.write("changes("+l.replace("\n","").replace("\\'","'").replace("\\t"," ")+").\n")
        #if i>1000:
        #    break

create_fact.close()
