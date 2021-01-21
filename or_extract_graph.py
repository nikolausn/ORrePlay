import tarfile
import os
import zipfile
import os
from tqdm import tqdm
import json
import numpy as np
import csv
import sqlite3

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
    import sys
    args = sys.argv
    if len(args)!=2:
        print("usage: {} <openrefine_projectfile>".format(args[0]))
        exit()
    
    file_name = args[1]
    print("Process file: {}".format(file_name))

    # extract project
    #file_name = "airbnb_dirty-csv.openrefine.tar.gz"
    #file_name = "03_poster_demo.openrefine.tar.gz"

    #prepare database
    db_name = '{}.db'.format(".".join(file_name.split(".")[:-2]))
    if os.path.exists(db_name):
        os.remove(db_name)
    conn = sqlite3.connect(db_name)

    cursor = conn.cursor()

    # Create table source
    cursor.execute('''CREATE TABLE IF NOT EXISTS source
                (source_id integer, source_url text, source_format text)''')
    cursor.execute('''CREATE UNIQUE INDEX source_id
    ON source(source_id)''');
    source_id = 0

    # Create table dataset
    cursor.execute('''CREATE TABLE IF NOT EXISTS dataset
                (dataset_id integer, source_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX dataset_id
    ON dataset(dataset_id)''');
    dataset_id = 0

    # Create table array
    cursor.execute('''CREATE TABLE IF NOT EXISTS array
                (array_id integer, dataset_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX array_id
    ON array(array_id)''');
    array_id = 0

    # Create table column
    cursor.execute('''CREATE TABLE IF NOT EXISTS column
                (col_id integer, array_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX col_id 
    ON column(col_id)''');
    col_id = 0

    # row
    cursor.execute('''CREATE TABLE IF NOT EXISTS row
                (row_id integer, array_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX row_id
    ON row(row_id)''')
    row_id = 0

    # cell
    cursor.execute('''CREATE TABLE IF NOT EXISTS cell
                (cell_id integer, col_id integer, row_id integer)''')
    cursor.execute('''CREATE UNIQUE INDEX cell_id
    ON cell(cell_id)''')            
    cursor.execute('''CREATE UNIQUE INDEX cell_col_row
    ON cell(col_id,row_id)''')            
    cell_id = 0

    # state
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS state
                (state_id integer, array_id integer, prev_state_id integer, state_label text, command text)''')
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS state
                (state_id integer, array_id integer, prev_state_id integer)''')
    state_id = 0

    cursor.execute('''CREATE TABLE IF NOT EXISTS state_command
                (state_id integer, state_label text, command text)''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS col_dependency
                (state_id integer, output_column integer, input_column integer)''')

    # content
    cursor.execute('''CREATE TABLE IF NOT EXISTS content
                (content_id integer, cell_id integer, state_id integer, value_id integer, prev_content_id integer)''')
    content_id = 0
    cursor.execute('''CREATE UNIQUE INDEX content_id
    ON content(content_id)''')            
    cursor.execute('''CREATE INDEX content_cell
    ON content(cell_id)''')            

    # value
    cursor.execute('''CREATE TABLE IF NOT EXISTS value
                (value_id integer, value_text text)''')
    value_id = 0

    # column_schema
    cursor.execute('''CREATE TABLE IF NOT EXISTS column_schema
                (col_schema_id integer, col_id integer, state_id integer, col_type string, col_name string, prev_col_id integer, prev_col_schema_id integer)''')
    col_schema_id = 0

    # row_position
    cursor.execute('''CREATE TABLE IF NOT EXISTS row_position
                (row_pos_id integer, row_id integer, state_id integer, prev_row_id integer)''')
    row_pos_id = 0

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
    """
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
    """

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

    #print(dataset[0]["cols"],len(dataset[0]["cols"]))
    #print(sorted([x["cellIndex"] for x in dataset[0]["cols"]]))
    #exit()
    for ix, xx in enumerate(dataset[0]["cols"]):
        cursor.execute("INSERT INTO column VALUES (?,?)",(col_id,array_id))
        tcid = xx["cellIndex"]
        if ix==0:
            prev_col_id = -1

        cursor.execute('''INSERT INTO column_schema VALUES
            (?,?,?,?,?,?,?)''',(col_schema_id,tcid,state_id-1,"",xx["name"],prev_col_id,-1))

        prev_col_id=tcid
        col_id+=1
        col_schema_id+=1

    cc_ids = list(cursor.execute("SELECT distinct state_id from column_schema order by state_id desc limit 1"))[0][0]            
    ccexs = list(cursor.execute("SELECT col_id,col_schema_id from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))
    ccexs = [(x[0],x[1]) for x in ccexs]

    #print(ccexs,len(ccexs))
    for temp_row_id,x in enumerate(dataset[2]["rows"]):
        #print(x["cells"])
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
            if type(val)==str:
                val = val.replace("\\","\\\\")
            cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
            cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,-1,value_id,-1))
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

        cursor.execute("INSERT INTO row VALUES (?,?)",(row_id,array_id))
        if temp_row_id==0:
            prev_row_id = -1

        #cursor.execute('''INSERT INTO row_position VALUES
        #    (?,?,?,?)''',(row_pos_id,row_id,state_id,prev_row_id))
        cursor.execute('''INSERT INTO row_position VALUES
            (?,?,?,?)''',(row_pos_id,row_id,-1,prev_row_id))
        prev_row_id=row_id
        row_id+=1
        row_pos_id+=1        
            
    #print(temp_row_id,temp_col_id,dataset[0],dataset[1])
    conn.commit()
    #exit()

    #backward
    ccexs_all = list(cursor.execute("SELECT * from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))

    for order,(change_id, change) in enumerate([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"][::-1]]):
        #print(change)
        if change.endswith(".zip"):

            locexzip,_ = open_change(hist_dir,change,target_folder=hist_dir)
            # read change
            changes = read_change(locexzip+"/change.txt")

            #recipe_writer.writerow([order,change_id,changes[1],dataset[0]["cols"],recipes[change_id]["description"]])
            
            # insert state
            #prev_state_id = state_id
            #state_id+=1            
            #(state_id number, array_id number, prev_state_id number, state_label text, command text)
            #print(state_id,array_id,prev_state_id,change_id,changes[1])
            #cursor.execute("INSERT INTO state VALUES (?,?,?,?,?)",(state_id,array_id,prev_state_id,change_id,changes[1]))
            if order == 0:
                prev_state_id = -1

            cursor.execute("INSERT INTO state VALUES (?,?,?)",(state_id,array_id,prev_state_id))
            #cursor.execute("INSERT INTO state VALUES (?,?,?)",(state_id,array_id,state_id))
            cursor.execute("INSERT INTO state_command VALUES (?,?,?)",(state_id,change_id,changes[1]))
            conn.commit()

            # get rows and cols indexes for the state
            # latest state_id of change
            rc_ids = list(cursor.execute("SELECT distinct state_id from row_position order by state_id desc limit 1"))[0][0]            
            rcexs = list(cursor.execute("SELECT row_id from row_position  where state_id=? order by row_pos_id asc",(str(rc_ids),)))
            rcexs = [x[0] for x in rcexs]
            #print(rcexs)
            cc_ids = list(cursor.execute("SELECT distinct state_id from column_schema order by state_id desc limit 1"))[0][0]            
            ccexs = list(cursor.execute("SELECT col_id,col_schema_id from column_schema  where state_id=? order by col_schema_id asc",(str(cc_ids),)))
            ccexs = [(x[0],x[1]) for x in ccexs]

            #ccexs_all = [(x[0],x[1]) for x in ccexs]
            #print(ccexs_all)
            #exit()


            #exit()

            if changes[1] == "com.google.refine.model.changes.MassCellChange":
                #print(changes[3])
                is_change = False
                for ch in changes[3]:
                    #print(ch)
                    try:
                        r = int(ch["row"])
                        is_change = True
                    except BaseException as ex:
                        print(ex)
                        continue
                    #print(ch)
                    c = int(ch["cell"])
                    nv = json.loads(ch["new"])
                    ov = json.loads(ch["old"])
                    #print(dataset[2]["rows"][r])            
                    #print(dataset[2]["rows"][r]["cells"][c],ch)
                    #print(dataset[2]["rows"][r]["cells"][c],nv)
                    if dataset[2]["rows"][r]["cells"][c] == nv:
                        # log file recorded here
                        # 0, start, cell_no, row_no, null, 1
                        # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>,<row_depend>,<cell_depend>
                        dataset[2]["rows"][r]["cells"][c] = ov

                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],r,c,ov,nv,r,c))
                        #cell_writer.writerow([order,change_id,changes[1],r,c,ov,nv,r,c])

                        # write cell_changes
                        # get previous value_id
                        try:
                            cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,rcexs[r]))
                        except BaseException as ex:
                            print(dataset[2]["rows"][r]["cells"])
                            print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                            raise ex
                        #print(dataset[2]["rows"][r]["cells"])
                        #print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                        #exit()
                        #print(len(dataset[2]["rows"][r]["cells"]))
                        try:
                            cex = list(cex)[0]
                        except BaseException as ex:
                            print((r,c),list(cex))
                            raise ex
                        
                        if type(val)==str:
                            val = val.replace("\\","\\\\")
                            
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cex[1],state_id,value_id,cex[0]))
                        value_id+=1
                        content_id+=1
                        #conn.commit()
                        #exit()

                        #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][0]["cells"])
                conn.commit()

                columns = dataset[0]["cols"].copy()
                col_names = [x["name"] for x in columns]
                
                # add dependency column
                ##col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                #print(len(changes[3]))
                if is_change:
                    description = recipes[change_id]["description"]
                    # find columns from description
                    #print(description)
                    col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                    #print(col_names)
                    all_col = set()
                    for x in col_names:
                        while description.find(x)>=0:
                            all_col.add(x)
                            description = description.replace(x,"")
                                    
                    respective_index = set()
                    #print(all_col)
                    for x in all_col:
                        cc = search_cell_column_byname(columns,x)
                        icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                        #respective_index.add(icol)
                        respective_index.add(cc[1]["cellIndex"])
                        #print(cc)
                    #print(c,respective_index,cc)
                    #exit()
                    
                    #print(respective_index,c_idx)
                    dependency_index = respective_index - set([c])
                    #for x in dependency_index:
                    #    col_dep_writer.writerow([order,change_id,changes[1],c,x])
                    for x in dependency_index:
                        cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,[x[1] for x in ccexs_all].index(c),[x[1] for x in ccexs_all].index(x)))


            elif changes[1] == "com.google.refine.model.changes.ColumnAdditionChange":
                #print(changes[2])
                new_cell_index = int(changes[2]["newCellIndex"])
                #print(dataset[0])
                # remove cell_index from coll definition
                c_idx, col = search_cell_column(dataset[0]["cols"],new_cell_index)
                #dataset[0]["cols"].pop(new_cell_index)
                
                columns = dataset[0]["cols"].copy()
                col_names = [x["name"] for x in columns]

                # add dependency column
                ##col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                description = recipes[change_id]["description"]
                # find columns from description
                col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                #print(col_names)
                all_col = set()
                for x in col_names:
                    while description.find(x)>=0:
                        all_col.add(x)
                        description = description.replace(x,"")
                
                respective_index = set()
                for x in all_col:
                    cc = search_cell_column_byname(columns,x)
                    icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                    #respective_index.add(icol)
                    respective_index.add(cc[1]["cellIndex"])
                    #print(cc)
                
                #print(respective_index,c_idx,new_cell_index)
                dependency_index = respective_index - set([new_cell_index])
                #for x in dependency_index:
                #    ##col_dep_writer.writerow([order,change_id,changes[1],c_idx,x])
                #    #col_dep_writer.writerow([order,change_id,changes[1],new_cell_index,x])
                for x in dependency_index:
                    cursor.execute("INSERT INTO col_dependency VALUES (?,?,?)",(state_id,[x[1] for x in ccexs_all].index(new_cell_index),[x[1] for x in ccexs_all].index(x)))

                #if order == 36:
                #    exit()
                #print(all_col,col)
                #break

                # remove column
                if col["name"] == changes[2]["columnName"]:
                    dataset[0]["cols"].pop(c_idx)
                #print(c_idx,col)
                #print(dataset[0]["cols"])

                # remove column on database state
                """
                temp_ccexs = ccexs.copy()
                #print(temp_ccexs)
                prev_col_schema_id = col_schema_id-1
                #print(temp_ccexs,new_cell_index,c_idx,dataset[0]["cols"])
                temp_ccexs.pop([x[0] for x in ccexs].index(new_cell_index))
                #print(temp_ccexs,new_cell_index,c_idx)
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1                
                conn.commit()       
                """

                pop_index = [x[1] for x in ccexs_all].index(new_cell_index)
                try:
                    next_sch = ccexs_all[pop_index+1]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all[pop_index-1][1]
                except:
                    prev_sch_idx = None                 

                #print(ccexs_all)

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1
                    ccexs_all[pop_index+1] = new_next
                
                ccexs_all.pop([x[1] for x in ccexs_all].index(new_cell_index))
                conn.commit()

                #print(ccexs_all)
                #exit()

                #if at>3:
                #    exit()
                #at+=1
                """
                try:
                    prev_sch = ccexs[c_idx-1]
                except:
                    prev_sch = [None,None]
                try:
                    next_sch = ccexs[c_idx+1]
                except:
                    next_sch = [None,None]
                try:
                    pos_sch = ccexs[c_idx]
                except:
                    pos_sch = [None,None]
                
                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,next_sch[0],state_id,"","",prev_sch[0],next_sch[1]))
                col_schema_id+=1
                #cursor.execute('''INSERT INTO column_schema VALUES
                #    (?,?,?,?,?,?,?)''',(col_schema_id,None,state_id,"","",pos_sch[0],pos_sch[1]))
                #col_schema_id+=1
                """

                # remove data
                #print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])
                    if dataset[2]["rows"][c_key]["cells"][new_cell_index] == c_val:
                        print(c_key,c_val)
                        dataset[2]["rows"][c_key]["cells"].pop(new_cell_index)
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None))
                        #cell_writer.writerow([order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None])

                """
                for r in dataset[2]["rows"]:
                    # record change of new column here
                    r["cells"].pop(c_idx)
                    #r["cells"][c_idx] = None
                """ 
                #print(dataset[2]["rows"])

                #for r in dataset[2]["rows"]
                #break
            elif changes[1] == "com.google.refine.model.changes.ColumnRemovalChange" :
                oldColumnIndex = int(changes[2]["oldColumnIndex"])
                oldColumn = json.loads(changes[2]["oldColumn"])
                cellIndex = oldColumn["cellIndex"]
                name = oldColumn["name"]                
                #print(dataset[0]["cols"])
                dataset[0]["cols"].insert(oldColumnIndex,oldColumn)
                #print(oldColumn)
                #print(dataset[0]["cols"])
                #print(changes[2])

                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])                
                    dataset[2]["rows"][c_key]["cells"][cellIndex] = c_val
                    ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex))
                    #cell_writer.writerow([order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex])
                #col_writer.writerow([order,change_id,changes[1],cellIndex,None])


                # add column on database state      
                """          
                temp_ccexs = ccexs.copy()
                prev_col_schema_id = col_schema_id-1
                #print(temp_ccexs)
                #print(oldColumnIndex,oldColumn)
                temp_ccexs.insert(oldColumnIndex,(cellIndex,None))
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                conn.commit()
                """
                
                try:
                    next_sch = ccexs_all[oldColumnIndex]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all[oldColumnIndex-1][1]
                except:
                    prev_sch_idx = -1

                #print(ccexs_all)
                new_pos = (col_schema_id,cellIndex,state_id,"",name,prev_sch_idx,-1)
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pos)
                col_schema_id+=1

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],cellIndex,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1
                    ccexs_all[oldColumnIndex] = new_next

                #print(oldColumnIndex)
                ccexs_all.insert(oldColumnIndex,new_pos)                
                                
                #print(ccexs_all)
                conn.commit()
                #exit()
                """
                try:
                    prev_sch = ccexs[c_idx-1]
                except:
                    prev_sch = [None,None]
                try:
                    next_sch = ccexs[c_idx+1]
                except:
                    next_sch = [None,None]
                try:
                    pos_sch = ccexs[c_idx]
                except:
                    pos_sch = [None,None]
                
                cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',(col_schema_id,next_sch[0],state_id,"","",prev_sch[0],next_sch[1]))
                col_schema_id+=1
                #cursor.execute('''INSERT INTO column_schema VALUES
                #    (?,?,?,?,?,?,?)''',(col_schema_id,None,state_id,"","",pos_sch[0],pos_sch[1]))
                #col_schema_id+=1
                """

                """        
                for i,r in enumerate(dataset[2]["rows"]):
                    # record change of new column here
                    try:
                        r["cells"].insert(oldColumnIndex,changes[2][val][i])
                    except:
                        r["cells"].insert(oldColumnIndex,None)
                """
                #break
            elif changes[1] == "com.google.refine.model.changes.ColumnSplitChange" :
                #print(changes[2])
                print(dataset[2]["rows"][0]["cells"])
                # get the cell index
                index_col = []
                for col_name in changes[2]["new_columns"]:
                    index_col.append(search_cell_column_byname(dataset[0]["cols"],col_name)[1]["cellIndex"])
                #print(index_col)
                #break
                # remove column metadata
                for ind in sorted(index_col)[::-1]:
                    icol, col = search_cell_column(dataset[0]["cols"],ind)
                    dataset[0]["cols"].pop(icol)

                ori_column = search_cell_column_byname(dataset[0]["cols"],changes[2]["columnName"])

                # remove cells on row data 
                # print(changes[2]["new_cells"])
                for c_key in changes[2]["new_cells"].keys():
                    #dataset[2]["rows"][c_key]["cells"]
                    for ind in sorted(index_col)[::-1]:
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]))
                        #cell_writer.writerow([order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]])
                        dataset[2]["rows"][c_key]["cells"].pop(ind)
                
                #for ind in sorted(index_col)[::-1]:                
                #    col_dep_writer.writerow([order,change_id,changes[1],ind,ori_column[1]["cellIndex"]])

                #print(dataset[2]["rows"][0]["cells"])
                # remove column on database state     
                """           
                temp_ccexs = ccexs.copy()
                print(index_col)
                for ind in sorted(index_col)[::-1]:
                    icol, col = search_cell_column(dataset[0]["cols"],ind)
                    #temp_ccexs.pop(icol)
                    temp_ccexs.pop([x[0] for x in temp_ccexs].index(ind))

                #temp_ccexs.pop([x[0] for x in ccexs].index(new_cell_index))
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1                
                conn.commit()
                """

                for ind in sorted(index_col)[::-1]:
                    print(ccexs_all)
                    pop_index = [x[1] for x in ccexs_all].index(ind)
                    try:
                        next_sch = ccexs_all[pop_index+1]                         
                    except:
                        next_sch = None

                    try:
                        prev_sch_idx = ccexs_all[pop_index-1][1]
                    except:
                        prev_sch_idx = -1                 

                    #print(ccexs_all)

                    if next_sch!=None:
                        new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                        cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',new_next)
                        col_schema_id+=1
                        ccexs_all[pop_index+1] = new_next
                    
                    ccexs_all.pop([x[1] for x in ccexs_all].index(ind))      
                    print(ccexs_all)                      
                      
                #print(ccexs_all)
                conn.commit()

                #exit()
                #break
            elif changes[1] == "com.google.refine.model.changes.ColumnRenameChange":
                #print(changes[2])
                index_col = search_cell_column_byname(dataset[0]["cols"],changes[2]["oldColumnName"])[1]
                print(index_col)
                index_col["name"] = changes[2]["oldColumnName"]
                print(changes[2])
                #exit()
                
                """
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                """
                #exit()
                #print(ccexs_all)                
                pop_index = [x[1] for x in ccexs_all].index(index_col["cellIndex"])
                old_pop = ccexs_all[pop_index]

                new_pop = (col_schema_id,old_pop[1],state_id,old_pop[3],changes[2]["oldColumnName"],old_pop[5],old_pop[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pop)
                col_schema_id+=1
                ccexs_all[pop_index] = new_pop
                
                #print(ccexs_all)       
                conn.commit()                    
                # should be metadata change
                #exit()

                #break            
            elif changes[1] == "com.google.refine.model.changes.CellChange":
                ch = changes[2]
                try:
                    r = int(ch["row"])
                except BaseException as ex:
                    print(ex)
                    continue
                #print(ch)
                c = int(ch["cell"])
                nv = json.loads(ch["new"])
                ov = json.loads(ch["old"])
                #print(dataset[2]["rows"][r])            
                #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][r]["cells"][c],nv)
                if dataset[2]["rows"][r]["cells"][c] == nv:
                    # log file recorded here
                    # 0, start, cell_no, row_no, null, 1
                    # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>,<row_depend>,<cell_depend>
                    #print("change exists")
                    dataset[2]["rows"][r]["cells"][c] = ov
                    ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c,r,nv,ov,c,r))
                    #cell_writer.writerow([order,change_id,changes[1],c,r,nv,ov,c,r])

                try:
                    cex = cursor.execute("SELECT content_id,cell_id FROM (SELECT a.content_id,a.cell_id,a.state_id FROM content a,cell b where a.cell_id=b.cell_id and b.col_id=? and b.row_id=?) order by state_id desc limit 1",(c,rcexs[r]))
                except BaseException as ex:
                    print(dataset[2]["rows"][r]["cells"])
                    print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                    raise ex
                #print(dataset[2]["rows"][r]["cells"])
                #print(ccexs,c,len(ccexs),len(dataset[2]["rows"][r]["cells"]))
                #exit()
                #print(len(dataset[2]["rows"][r]["cells"]))
                try:
                    cex = list(cex)[0]
                except BaseException as ex:
                    print((r,c),list(cex))
                    raise ex

                if type(ov["v"])==str:
                    ov["v"] = ov["v"].replace("\\","\\\\")

                cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,ov["v"]))
                cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cex[1],state_id,value_id,cex[0]))
                value_id+=1
                content_id+=1
                conn.commit()
                #exit()
                #break                    

            elif changes[1] == "com.google.refine.model.changes.ColumnMoveChange":
                columns = dataset[0]["cols"]
                temp = columns[int(changes[2]["newColumnIndex"])]
                columns[int(changes[2]["newColumnIndex"])] = columns[int(changes[2]["oldColumnIndex"])]
                columns[int(changes[2]["oldColumnIndex"])] = temp

                #changes[2]["oldColumnIndex"] = 7
                #print(changes[2])

                # should be metadata change
                #col_writer.writerow([order,change_id,changes[1],int(changes[2]["newColumnIndex"]),int(changes[2]["oldColumnIndex"])])

                """
                temp_ccexs = ccexs.copy()
                print(temp_ccexs)
                temp = temp_ccexs[int(changes[2]["newColumnIndex"])]
                temp_ccexs[int(changes[2]["newColumnIndex"])] = temp_ccexs[int(changes[2]["oldColumnIndex"])]
                temp_ccexs[int(changes[2]["oldColumnIndex"])] = temp
                print(temp_ccexs)
                prev_col_schema_id = col_schema_id-1
                for v,(vv,vy) in enumerate(temp_ccexs):
                    if v==0:
                        prev_vv = None                        
                    cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',(col_schema_id,vv,state_id,"","",prev_vv,prev_col_schema_id))
                    prev_vv = vv
                    col_schema_id+=1
                conn.commit()
                """

                # switch columnIndex if newColumn > oldColumn
                if changes[2]["newColumnIndex"] > changes[2]["oldColumnIndex"]:
                    temp = changes[2]["newColumnIndex"]
                    changes[2]["newColumnIndex"] = changes[2]["oldColumnIndex"]
                    changes[2]["oldColumnIndex"] = temp
                
                ccexs_all_c = ccexs_all.copy()
                pop_index = int(changes[2]["newColumnIndex"])
                new_pop = ccexs_all_c[pop_index]
                try:
                    next_sch = ccexs_all_c[pop_index+1]                         
                except:
                    next_sch = None

                try:
                    prev_sch_idx = ccexs_all_c[pop_index-1][1]
                except:
                    prev_sch_idx = -1
                
                pop_index2 = int(changes[2]["oldColumnIndex"])
                new_pop2 = ccexs_all_c[pop_index2]
                try:
                    next_sch2 = ccexs_all_c[pop_index2+1]                         
                except:
                    next_sch2 = None

                try:
                    prev_sch_idx2 = ccexs_all_c[pop_index2-1][1]
                except:
                    prev_sch_idx2 = -1
                
                print(pop_index,pop_index2,prev_sch_idx,prev_sch_idx2,new_pop,new_pop2,next_sch,next_sch2)
                if next_sch2!=None:
                    new_next2 = (col_schema_id,next_sch2[1],state_id,next_sch2[3],next_sch2[4],new_pop[1],next_sch2[0]) 
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next2)
                    ccexs_all[pop_index2+1] = new_next2
                    col_schema_id+=1

                if prev_sch_idx2==new_pop[1]:
                    new_popp = (col_schema_id,new_pop[1],state_id,new_pop[3],new_pop[4],new_pop2[1],new_pop[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_popp)
                    col_schema_id+=1
                    ccexs_all[pop_index2] = new_popp
                else:
                    if next_sch!=None:
                        new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],new_pop2[1],next_sch[0]) 
                        cursor.execute('''INSERT INTO column_schema VALUES
                        (?,?,?,?,?,?,?)''',new_next)
                        ccexs_all[pop_index2+1] = new_next
                        col_schema_id+=1

                    new_popp = (col_schema_id,new_pop[1],state_id,new_pop[3],new_pop[4],prev_sch_idx2,new_pop[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_popp)
                    col_schema_id+=1
                    ccexs_all[pop_index2] = new_popp                        

                new_popp2 = (col_schema_id,new_pop2[1],state_id,new_pop2[3],new_pop2[4],prev_sch_idx,new_pop2[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_popp2)
                col_schema_id+=1
                ccexs_all[pop_index] = new_popp2
                
                print(ccexs_all)

                conn.commit()
                #exit()            
                """
                #print(ccexs_all)

                if next_sch!=None:
                    new_next = (col_schema_id,next_sch[1],state_id,next_sch[3],next_sch[4],prev_sch_idx,next_sch[0])
                    cursor.execute('''INSERT INTO column_schema VALUES
                    (?,?,?,?,?,?,?)''',new_next)
                    col_schema_id+=1
                    ccexs_all[pop_index+1] = new_next
                
                ccexs_all.pop([x[1] for x in ccexs_all].index(new_cell_index))                

                old_pop = ccexs_all[pop_index]

                new_pop = (col_schema_id,old_pop[1],state_id,old_pop[3],changes[2]["oldColumnName"],old_pop[5],old_pop[0])
                cursor.execute('''INSERT INTO column_schema VALUES
                (?,?,?,?,?,?,?)''',new_pop)
                col_schema_id+=1
                ccexs_all[pop_index] = new_pop
                
                #print(ccexs_all)       
                conn.commit()         
                """
                #exit()

                #break
            elif changes[1] == "com.google.refine.model.changes.RowReorderChange":
                # create a new row set
                new_rows = dataset[2]["rows"]
                old_rows = np.array(new_rows)
                for i,li in enumerate(changes[2]["row_order"]):
                    old_rows[li] = new_rows[i]
                    #row_writer.writerow([order,change_id,changes[1],li,i])
                    if i == 0:
                        prev_vv = -1
                    temp_rid = rcexs[li]
                    cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,temp_rid,state_id,int(prev_vv)))
                    prev_vv = temp_rid
                    row_pos_id+=1
                conn.commit()
                #exit()

                # Write log file
                for i,li in enumerate(changes[2]["row_order"]):
                    for j,jj in enumerate(new_rows[i]["cells"]):
                        #print(j,jj,old_rows[i]["cells"][j])
                        
                        try:
                            ov = old_rows[i]["cells"][j]
                        except:
                            ov = None
                        
                        ##cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],j,i,jj,ov,j,i))       
                        #cell_writer.writerow([order,change_id,changes[1],i,j,jj,ov,i,j])
                


                dataset[2]["rows"] = old_rows.tolist()
                #break                
            elif changes[1] == "com.google.refine.model.changes.RowRemovalChange":  
                temp_rows = list(range(row_id))

                for i,idx in enumerate(changes[2]["row_idx_remove"]):
                    #print(idx)
                    #j = len(changes[2]["row_idx_remove"])-i-1
                    #print(j)
                    dataset[2]["rows"].insert(idx,changes[2]["old_values"][i])

                    """
                    for temp_col_id,y in enumerate(x["cells"]):
                        cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,temp_col_id,temp_row_id))
                        try:
                            val = y["v"]
                        except:
                            val = None
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,state_id,value_id,-1))
                        cell_id+=1
                        value_id+=1
                        content_id+=1
                    #(row_pos_id number, row_id number, state_id number, prev_row_id number)
                    """
                    # add row
                    cursor.execute("INSERT INTO row VALUES (?,?)",(row_id,array_id))
                    temp_rows.insert(idx,row_id)

                    # exchanges row                    
                    if i<len(changes[2]["row_idx_remove"])-1:
                        for ii in range(changes[2]["row_idx_remove"][i],changes[2]["row_idx_remove"][i+1]):
                            #row_writer.writerow([order,change_id,changes[1],ii+1+i,ii])           
                            pass
                            #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,ii+1+i,state_id,ii))
                            #row_pos_id+=1
                            
                    else:
                        for ii in range(changes[2]["row_idx_remove"][i],len(dataset[2]["rows"])-1):
                            #row_writer.writerow([order,change_id,changes[1],ii+1+i,ii])
                            pass
                            #cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,ii+1+i,state_id,ii))
                            #row_pos_id+=1
                    #print(idx,dataset[2]["rows"][idx])
                    
                    # add values
                    for v,vv in enumerate(changes[2]["old_values"][i]["cells"]):                        
                        # add one row
                        cursor.execute("INSERT INTO cell VALUES (?,?,?)",(cell_id,v,row_id))
                        try:
                            val = vv["v"]
                        except:
                            val = None                            
                        if type(val)==str:
                            val = val.replace("\\","\\\\")
                        cursor.execute("INSERT INTO value VALUES (?,?)",(value_id,val))
                        cursor.execute("INSERT INTO content VALUES (?,?,?,?,?)",(content_id,cell_id,state_id,value_id,-1))
                        cell_id+=1
                        value_id+=1
                        content_id+=1
                    #print(changes[2]["old_values"][i],row_id,idx)

                    row_id+=1
                    #exit()
                    
                conn.commit()                

                for v,vv in enumerate(temp_rows):
                    if v==0:
                        prev_vv = -1

                    cursor.execute("INSERT INTO row_position VALUES (?,?,?,?)",(row_pos_id,vv,state_id,int(prev_vv)))
                    prev_vv = vv
                    row_pos_id+=1
                conn.commit()
                #for i,idx in 
                #row_riter.writerow([order,change_id,ori_column[1]["cellIndex"],ind])
                #break        
            elif changes[1] == "com.google.refine.model.changes.RowStarChange":
                #print(changes[2])
                old_val = True if changes[2]["oldStarred"] == "true" else False
                new_val = True if changes[2]["newStarred"] == "true" else False
                #print(dataset[2]["rows"][int(changes[2]["row"])]["starred"])
                #print(dataset[2]["rows"][idx])
                #print(int(changes[2]["row"]),dataset[2]["rows"][int(changes[2]["row"])])
                if dataset[2]["rows"][int(changes[2]["row"])]["starred"] == new_val:
                    print("change starred")
                    dataset[2]["rows"][int(changes[2]["row"])]["starred"] = old_val
                #break
            else:
                print(changes[2])
                break            
            
            prev_state_id=state_id
            state_id+=1
        #break
        #print(dataset[0])
        #print(dataset[2]["rows"][0]["cells"])

    #pass
    print(dataset[0])
    print(dataset[2]["rows"][0]["cells"])
    
    prev_source_id = source_id
    source_id+=1
    prev_dataset_id=dataset_id        
    dataset_id+=1
    prev_array_id = array_id
    array_id+=1    
    conn.commit()
    #exit()

    # extract table to csv
    import os
    extract_folder = ".".join(file_name.split(".")[:-2])+".extract"
    try:
        os.mkdir(extract_folder)
    except:
        pass
    print("Extracting CSV:")
    import pandas as pd
    import csv
    tables = ["array","cell","column","column_schema","content","dataset","row","row_position","source","state","value","state_command","col_dependency"]
    table_files = []
    for table in tables:
        print("Extract {}".format(table))
        df = pd.read_sql_query("SELECT * from {}".format(table), conn)
        df.to_csv("{}/{}.csv".format(extract_folder,table),sep=",",index=False,header=None,quotechar='"',escapechar="\\",doublequote=False,quoting=csv.QUOTE_NONNUMERIC)
        table_files.append((table,"{}/{}.csv".format(extract_folder,table)))
    print("csv extracted at: {}".format(extract_folder))
    # prepare datalog files
    #print("prepare datalog files: {}/facts.pl".format(extract_folder))
    with open("{}/facts.pl".format(extract_folder),"w") as writer:
        for x,y in table_files:
            with open(y,"r") as file:
                for l in file:
                    # remove whitespace
                    l = l[:-1]
                    writer.write("{}({}).\n".format(x,l))
    print("prepared datalog files: {}/facts.pl".format(extract_folder))
                    
    conn.close()