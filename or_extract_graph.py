import tarfile
import os
import zipfile
import os
from tqdm import tqdm
import json
import numpy as np
import csv
import sqlite3

db_name = 'prov_graph.db'
if os.path.exists(db_name):
    os.remove(db_name)
conn = sqlite3.connect(db_name)

cursor = conn.cursor()

# Create table source
cursor.execute('''CREATE TABLE IF NOT EXISTS source
            (source_id number, source_url text, source_format text)''')
cursor.execute('''CREATE UNIQUE INDEX source_id
ON source(source_id)''');
source_id = 0

# Create table dataset
cursor.execute('''CREATE TABLE IF NOT EXISTS dataset
            (dataset_id number, source_id number)''')
cursor.execute('''CREATE UNIQUE INDEX dataset_id
ON dataset(dataset_id)''');
dataset_id = 0

# Create table array
cursor.execute('''CREATE TABLE IF NOT EXISTS array
            (array_id number, dataset_id number)''')
cursor.execute('''CREATE UNIQUE INDEX array_id
ON array(array_id)''');
array_id = 0

# Create table column
cursor.execute('''CREATE TABLE IF NOT EXISTS column
            (col_id number, array_id number)''')
cursor.execute('''CREATE UNIQUE INDEX col_id 
ON column(col_id)''');
col_id = 0

# row
cursor.execute('''CREATE TABLE IF NOT EXISTS row
            (row_id number, array_id number)''')
cursor.execute('''CREATE UNIQUE INDEX row_id
ON row(row_id)''')
row_id = 0

# cell
cursor.execute('''CREATE TABLE IF NOT EXISTS cell
            (cell_id number, col_id number, row_id number)''')
cursor.execute('''CREATE UNIQUE INDEX cell_id
ON cell(cell_id)''')            
cursor.execute('''CREATE UNIQUE INDEX cell_col_row
ON cell(col_id,row_id)''')            
cell_id = 0

# state
cursor.execute('''CREATE TABLE IF NOT EXISTS state
            (state_id number, array_id number, prev_state_id number, state_label text, command text)''')
state_id = 0

# content
cursor.execute('''CREATE TABLE IF NOT EXISTS content
            (content_id number, cell_id number, state_id number, value_id number, prev_content_id)''')
content_id = 0
cursor.execute('''CREATE UNIQUE INDEX content_id
ON content(content_id)''')            
cursor.execute('''CREATE INDEX content_cell
ON content(cell_id)''')            

# value
cursor.execute('''CREATE TABLE IF NOT EXISTS value
            (value_id number, value_text text)''')
value_id = 0

# column_schema
cursor.execute('''CREATE TABLE IF NOT EXISTS column_schema
            (col_schema_id number, col_id number, state_id number, col_type string, col_name string, prev_col_id number, prev_col_schema_id number)''')
col_schema_id = 0

# row_position
cursor.execute('''CREATE TABLE IF NOT EXISTS row_position
            (row_pos_id number, row_id number, state_id number, prev_row_id number)''')
row_pos_id = 0


class RowId():
    def __init__(self,row_id):
        self.row_id = row_id
    
    def __str__(self):
        return self.row_id

class ColId():
    def __init__(self,col_id):
        self.col_id = col_id
    
    def __str__(self):
        return self.col_id

def init_column(n_col):
    list_col = []
    for x in range(n_col):
        list_col.append(ColId(x))

def init_row(n_row):
    list_row = []
    for x in range(n_row):
        list_row.append(RowId(x))


def extract_project(file_name,temp_folder="temp"):
    fname = ".".join(file_name.split(".")[:-2])
    tar = tarfile.open(file_name,"r:gz")
    locex = "{}/{}".format(temp_folder,fname)
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)
    tar.extractall(path=locex)
    tar.close()
    return locex,fname

def read_dataset(project_file):
    locex = "{}/{}/".format(project_file,"data")
    zipdoc = zipfile.ZipFile(project_file+"/data.zip")
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)
    zipdoc.extractall(path=locex)
    zipdoc.close()

    datafile = open(locex+"/data.txt","r",encoding="ascii", errors="ignore")
    
    # read column model
    column_model_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            column_model_dict[head] = val
            cols = []
            #print(head)
            if head == "columnCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    col = json.loads(line)
                    cols.append(col)
                column_model_dict["cols"] = cols
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(column_model_dict)    

    # read history
    history_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            history_dict[head] = val
            hists = []
            #print(head)
            if head == "pastEntryCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    hist = json.loads(line)
                    hists.append(hist)
                history_dict["hists"] = hists
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(history_dict)

    # read data row
    data_row_dict = {}
    line = next(datafile).replace("\n","")
    while line:
        if line=="/e/":
            break
        else:
            head = line.split("=")[0]
            val = line.split("=")[-1]
            data_row_dict[head] = val
            rows = []
            #print(head)
            if head == "rowCount":
                for x in range(int(val)):
                    line = next(datafile).replace("\n","")
                    row = json.loads(line)
                    rows.append(row)
                data_row_dict["rows"] = rows
        try:
            line=next(datafile).replace("\n","")
        except:
            break
    #print(data_row_dict)

    return column_model_dict,history_dict,data_row_dict


def open_change(hist_dir,file_name,target_folder):
    fname = ".".join(file_name.split(".")[:-1])
    zipdoc = zipfile.ZipFile(hist_dir+file_name)
    locex = target_folder+fname
    try:
        os.mkdir(locex)
    except BaseException as ex:
        print(ex)    
    zipdoc.extractall(locex)
    zipdoc.close()
    return locex,fname
        
"""
3.3
com.google.refine.model.changes.ColumnAdditionChange
columnName=price_crazy
columnIndex=16
newCellIndex=24
newCellCount=5
564;{"v":1}
3277;{"v":1}
5442;{"v":1}
5454;{"v":1}
6934;{"v":1}
oldColumnGroupCount=0
/ec/
"""


def read_change(changefile):
    changefile = open(changefile,"r",encoding="ascii", errors="ignore")
    # read version
    version = next(changefile).replace("\n","")
    # read command_name
    command_name = next(changefile).replace("\n","")
    print(version,command_name)
    header_dict = {}
    data_row = []    
    if command_name == "com.google.refine.model.changes.MassCellChange":
        header = ["commonColumnName","updateRowContextDependencies","cellChangeCount"]
        for head in header:
            header_dict[head] = next(changefile).replace("\n","").split("=")[-1]
        print(header_dict)
        data_header = ["row","cell","old","new"]
        data_row = []
        data_dict = {}
        line = next(changefile).replace("\n","")
        while line:
            if line=="/ec/":
                data_row.append(data_dict)
                data_dict = {}
                #break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass

                data_dict[head] = val
            try:
                line=next(changefile).replace("\n","")
            except:
                break
        #print(data_row)
    elif command_name == "com.google.refine.model.changes.ColumnAdditionChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                rows = {}
                #print(head)
                if head == "newCellCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","").split(";")
                        val = None
                        try:
                            val = json.loads(line[1])
                        except:
                            pass
                        row = int(line[0])
                        rows[row] = val
                    header_dict["val"] = rows
            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.ColumnRemovalChange" :
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                rows = {}
                #print(head)
                if head == "oldCellCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","").split(";")
                        val = None
                        try:
                            val = json.loads(line[1])
                        except:
                            pass
                        row = int(line[0])
                        rows[row] = val
                    header_dict["val"] = rows
            try:
                line=next(changefile).replace("\n","")
            except:
                break   
    elif command_name == "com.google.refine.model.changes.ColumnSplitChange" :
        line = next(changefile).replace("\n","")
        print(line)

        new_columns = []
        new_cells = {}
        new_rows = {}       
        
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                #print(head)
                # read new column name
                if head == "columnNameCount":                    
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        new_columns.append(line)
                    header_dict["new_columns"] = new_columns

                # read new cells
                if head == "rowIndexCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        index = int(line)
                        new_cells[x] = [None for i in range(int(header_dict["columnNameCount"]))]
                        new_rows[x] = None

                if head == "tupleCount":
                    r_idx = list(new_cells.keys())
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        for y in range(int(line)):
                            line = next(changefile).replace("\n","")
                            new_cells[r_idx[i]][y] = line
                    header_dict["new_cells"] = new_cells

                # read new rows values
                if head == "newRowCount":
                    for x in new_rows.keys():
                        line = next(changefile).replace("\n","")
                        new_rows[x] = json.loads(line)
                    header_dict["new_rows"] = new_rows

            try:
                line=next(changefile).replace("\n","")
            except:
                break 
    elif command_name == "com.google.refine.model.changes.ColumnRenameChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.CellChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.ColumnMoveChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val        

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowReorderChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val
                if head == "rowIndexCount":
                    row_order = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        row_order.append(int(line))
                    header_dict["row_order"] = row_order

            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowRemovalChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val                
                if head == "rowIndexCount":
                    row_idx_remove = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        row_idx_remove.append(int(line))
                    header_dict["row_idx_remove"] = row_idx_remove
                if head == "rowCount":
                    old_values = []                    
                    for i,x in enumerate(range(int(val))):
                        line = next(changefile).replace("\n","")
                        old_values.append(json.loads(line))
                    header_dict["old_values"] = old_values                                    
            try:
                line=next(changefile).replace("\n","")
            except:
                break
    elif command_name == "com.google.refine.model.changes.RowStarChange":
        line = next(changefile).replace("\n","")
        #print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = None
                try:
                    val = "=".join(line.split("=")[1:])
                except:
                    pass
                header_dict[head] = val                             
            try:
                line=next(changefile).replace("\n","")
            except:
                break


    return version,command_name,header_dict,data_row

def search_cell_column(col_mds,cell_index):
    for i,col in enumerate(col_mds):
        if col["cellIndex"] == cell_index:
            return i,col

    return -1, None

def search_cell_column_byname(col_mds,name):
    for i,col in enumerate(col_mds):
        if col["originalName"] == name:
            return i,col

    return -1, None

at = 0 
if __name__ == "__main__":
    # extract project
    file_name = "airbnb_dirty-csv.openrefine.tar.gz"
    cursor.execute("INSERT INTO source VALUES (?,?,?)",(source_id,file_name,"OpenRefine Project File"))

    locex,_ = extract_project(file_name)
    # extract data
    dataset = read_dataset(locex)
    cursor.execute("INSERT INTO dataset VALUES (?,?)",(dataset_id,source_id))
    cursor.execute("INSERT INTO array VALUES (?,?)",(array_id,dataset_id))

    #columns = dataset[0]["cols"].copy()
    #print(columns)
    #exit()
    recipes = {}
    for x in dataset[1]["hists"]:
        recipes[x["id"]] = x
    #exit()
    #print(recipes)
    #exit()
    
    #print([str(x["id"])+".change.zip" for x in dataset[1]["hists"][::-1]])
    #exit()

    # prepare cell changes log
    cell_changes = open("cell_changes.log","w",newline="",encoding="ascii", errors="ignore")
    cell_writer = csv.writer(cell_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    meta_changes = open("meta_changes.log","w",encoding="ascii", errors="ignore")
    recipe_changes = open("recipe_changes.log","w",encoding="ascii", errors="ignore")
    recipe_writer = csv.writer(recipe_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    col_changes = open("col_changes.log","w",encoding="ascii", errors="ignore")
    col_writer = csv.writer(col_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    row_changes = open("row_changes.log","w",encoding="ascii", errors="ignore")
    row_writer = csv.writer(row_changes,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)
    col_dependency = open("col_dependency.log","w",encoding="ascii", errors="ignore")
    col_dep_writer = csv.writer(col_dependency,delimiter=",",quotechar='"',quoting=csv.QUOTE_ALL,escapechar="\\",doublequote=False)

    # read history file
    hist_dir = locex+"/history/"
    list_dir = os.listdir(hist_dir)
    #for change in sorted(list_dir)[::-1]:    
    #order = 0    

    """    
    cursor.execute('''CREATE TABLE IF NOT EXISTS cell
                (cell_id number, col_id number, row_id number)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS value
                (value_id number, value_text text)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS content
                (content_id number, cell_id number, state_id number, value_id number, prev_content_id)''')
    """

    print(dataset[0]["cols"],len(dataset[0]["cols"]))
    print(sorted([x["cellIndex"] for x in dataset[0]["cols"]]))
    #exit()
    for ix, xx in enumerate(dataset[0]["cols"]):
        cursor.execute("INSERT INTO column VALUES (?,?)",(col_id,array_id))
        tcid = xx["cellIndex"]
        if ix==0:
            prev_col_id = None

        cursor.execute('''INSERT INTO column_schema VALUES
            (?,?,?,?,?,?,?)''',(col_schema_id,tcid,state_id,"",xx["name"],prev_col_id,None))

        prev_col_id=tcid
        col_id+=1
        col_schema_id+=1

    cc_ids = list(cursor.execute("SELECT distinct state_id from column_schema order by state_id desc limit 1"))[0][0]            
    ccexs = list(cursor.execute("SELECT col_id,col_schema_id from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))
    ccexs = [(x[0],x[1]) for x in ccexs]

    #print(ccexs,len(ccexs))
    for temp_row_id,x in enumerate(dataset[2]["rows"]):
        #print(x["cells"])
        cursor.execute("INSERT INTO row VALUES (?,?)",(row_id,array_id))
        if temp_row_id==0:
            prev_row_id = None

        cursor.execute('''INSERT INTO row_position VALUES
            (?,?,?,?)''',(row_pos_id,row_id,state_id,prev_row_id))

        for temp_col_id,y in enumerate(x["cells"]):
            #print(temp_col_id)
            try:
                cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,temp_col_id,temp_row_id))
            except BaseException as ex:
                print(x["cells"])
                raise ex
            try:
                val = y["v"]
            except:
                val = None
            cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
            cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,state_id,value_id,-1))
            cell_id+=1
            value_id+=1
            content_id+=1            

            """
            if temp_row_id==0:
                cursor.execute("INSERT INTO column VALUES (?,?)",(col_id,array_id))
                if temp_col_id==0:
                    prev_col_id = None

                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,col_id,state_id,"","",prev_col_id,None))

                prev_col_id=col_id
                col_id+=1
                col_schema_id+=1
            """

        prev_row_id=row_id
        row_id+=1
        row_pos_id+=1        
            
    #print(temp_row_id,temp_col_id,dataset[0],dataset[1])

    prev_source_id = source_id
    source_id+=1
    prev_dataset_id=dataset_id        
    dataset_id+=1
    prev_array_id = array_id
    array_id+=1    
    conn.commit()
    #exit()

    # extract table to csv
    print("Extracting CSV:")
    import pandas as pd
    tables = ["array","cell","column","column_schema","content","dataset","row","row_position","source","state","value"]
    for table in tables:
        print("Extract {}".format(table))
        df = pd.read_sql_query("SELECT * from {}".format(table), conn)
        df.to_csv("{}.csv".format(table),sep=",",index=False,header=None)
    
    conn.close()

