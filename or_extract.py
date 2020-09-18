import tarfile
import os
import zipfile
import os
from tqdm import tqdm
import json
import numpy as np
import csv

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


if __name__ == "__main__":
    # extract project
    file_name = "airbnb_dirty-csv.openrefine.tar.gz"
    locex,_ = extract_project(file_name)
    # extract data
    dataset = read_dataset(locex)
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
    
    #backward
    for order,(change_id, change) in enumerate([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"][::-1]]):
        #print(change)
        if change.endswith(".zip"):

            locexzip,_ = open_change(hist_dir,change,target_folder=hist_dir)
            # read change
            changes = read_change(locexzip+"/change.txt")

            recipe_writer.writerow([order,change_id,changes[1],dataset[0]["cols"]])

            if changes[1] == "com.google.refine.model.changes.MassCellChange":
                for ch in changes[3]:
                    #print(ch)
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
                        dataset[2]["rows"][r]["cells"][c] = ov

                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],r,c,ov,nv,r,c))
                        cell_writer.writerow([order,change_id,changes[1],r,c,ov,nv,r,c])

                        #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][0]["cells"])
                columns = dataset[0]["cols"].copy()
                col_names = [x["name"] for x in columns]

                # add dependency column
                #col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                description = recipes[change_id]["description"]
                # find columns from description
                col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                print(col_names)
                all_col = set()
                for x in col_names:
                    while description.find(x)>=0:
                        all_col.add(x)
                        description = description.replace(x,"")
                
                respective_index = set()
                for x in all_col:
                    cc = search_cell_column_byname(columns,x)
                    icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                    respective_index.add(icol)
                    #print(cc)
                
                #print(respective_index,c_idx)
                dependency_index = respective_index - set([c])
                for x in dependency_index:
                    col_dep_writer.writerow([order,change_id,changes[1],c,x])

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
                #col_dep_writer.writerow([order,change_id,c_idx,new])                
                #print("recipe:",recipes[change_id])
                description = recipes[change_id]["description"]
                # find columns from description
                col_names = sorted(col_names,key=lambda x:len(x))[::-1]
                print(col_names)
                all_col = set()
                for x in col_names:
                    while description.find(x)>=0:
                        all_col.add(x)
                        description = description.replace(x,"")
                
                respective_index = set()
                for x in all_col:
                    cc = search_cell_column_byname(columns,x)
                    icol, col = search_cell_column(columns,cc[1]["cellIndex"])  
                    respective_index.add(icol)
                    #print(cc)
                
                #print(respective_index,c_idx)
                dependency_index = respective_index - set([c_idx])
                for x in dependency_index:
                    col_dep_writer.writerow([order,change_id,changes[1],c_idx,x])

                #print(all_col,col)
                #break

                # remove column
                if col["name"] == changes[2]["columnName"]:
                    dataset[0]["cols"].pop(c_idx)
                #print(c_idx,col)
                #print(dataset[0]["cols"])

                # remove data
                #print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])
                    if dataset[2]["rows"][c_key]["cells"][new_cell_index] == c_val:
                        print(c_key,c_val)
                        dataset[2]["rows"][c_key]["cells"].pop(new_cell_index)
                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None))
                        cell_writer.writerow([order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None])

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
                    #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex))
                    cell_writer.writerow([order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex])
                col_writer.writerow([order,change_id,changes[1],cellIndex,None])

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
                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]))
                        cell_writer.writerow([order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]])
                        dataset[2]["rows"][c_key]["cells"].pop(ind)
                
                for ind in sorted(index_col)[::-1]:                
                    col_dep_writer.writerow([order,change_id,changes[1],ind,ori_column[1]["cellIndex"]])

                print(dataset[2]["rows"][0]["cells"])
                #break
            elif changes[1] == "com.google.refine.model.changes.ColumnRenameChange":
                #print(changes[2])
                index_col = search_cell_column_byname(dataset[0]["cols"],changes[2]["oldColumnName"])[1]
                print(index_col)
                index_col["name"] = changes[2]["oldColumnName"]

                # should be metadata change

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
                    #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c,r,nv,ov,c,r))
                    cell_writer.writerow([order,change_id,changes[1],c,r,nv,ov,c,r])

                #break                    

            elif changes[1] == "com.google.refine.model.changes.ColumnMoveChange":
                columns = dataset[0]["cols"]
                temp = columns[int(changes[2]["newColumnIndex"])]
                columns[int(changes[2]["newColumnIndex"])] = columns[int(changes[2]["oldColumnIndex"])]
                columns[int(changes[2]["oldColumnIndex"])] = temp

                # should be metadata change
                col_writer.writerow([order,change_id,changes[1],int(changes[2]["newColumnIndex"]),int(changes[2]["oldColumnIndex"])])

                #break
            elif changes[1] == "com.google.refine.model.changes.RowReorderChange":
                # create a new row set
                new_rows = dataset[2]["rows"]
                old_rows = np.array(new_rows)
                for i,li in enumerate(changes[2]["row_order"]):
                    old_rows[li] = new_rows[i]
                    row_writer.writerow([order,change_id,changes[1],li,i])

                # Write log file
                for i,li in enumerate(changes[2]["row_order"]):
                    for j,jj in enumerate(new_rows[i]["cells"]):
                        #print(j,jj,old_rows[i]["cells"][j])
                        
                        try:
                            ov = old_rows[i]["cells"][j]
                        except:
                            ov = None
                        
                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],j,i,jj,ov,j,i))       
                        cell_writer.writerow([order,change_id,changes[1],j,i,jj,ov,j,i])
                


                dataset[2]["rows"] = old_rows.tolist()
                #break                
            elif changes[1] == "com.google.refine.model.changes.RowRemovalChange":                
                for i,idx in enumerate(changes[2]["row_idx_remove"]):
                    print(idx)
                    #j = len(changes[2]["row_idx_remove"])-i-1
                    #print(j)
                    dataset[2]["rows"].insert(idx,changes[2]["old_values"][i])

                    if i<len(changes[2]["row_idx_remove"])-1:
                        for ii in range(changes[2]["row_idx_remove"][i],changes[2]["row_idx_remove"][i+1]):                            
                            row_writer.writerow([order,change_id,changes[1],ii,ii+1])
                    else:
                        for ii in range(changes[2]["row_idx_remove"][i],len(dataset[2]["rows"])-1):
                            row_writer.writerow([order,change_id,changes[1],ii,ii+1])
                    #print(idx,dataset[2]["rows"][idx])
                
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
        #break
        #print(dataset[0])
        #print(dataset[2]["rows"][0]["cells"])
    #pass
    print(dataset[0])
    print(dataset[2]["rows"][0]["cells"])
    cell_changes.close()

    exit()

    #forward
    for order,(change_id, change) in enumerate([(x["id"],str(x["id"])+".change.zip") for x in dataset[1]["hists"]]):
        print(change)
        if change.endswith(".zip"):
            locexzip,_ = open_change(hist_dir,change,target_folder=hist_dir)
            # read change
            changes = read_change(locexzip+"/change.txt")
            if changes[1] == "com.google.refine.model.changes.MassCellChange":
                for ch in changes[3]:
                    #print(ch)
                    try:
                        r = int(ch["row"])
                    except BaseException as ex:
                        print(ex)
                        continue
                    print(ch)
                    c = int(ch["cell"])
                    nv = json.loads(ch["new"])
                    ov = json.loads(ch["old"])
                    #print(dataset[2]["rows"][r])            
                    #print(dataset[2]["rows"][r]["cells"][c],ch)
                    #print(dataset[2]["rows"][r]["cells"][c],nv)
                    if dataset[2]["rows"][r]["cells"][c] == ov:
                        # log file recorded here
                        # 0, start, cell_no, row_no, null, 1
                        # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>,<row_depend>,<cell_depend>
                        dataset[2]["rows"][r]["cells"][c] = nv

                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],r,c,ov,nv,r,c))

                        #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][0]["cells"])
            elif changes[1] == "com.google.refine.model.changes.ColumnAdditionChange":
                #print(changes[2])
                new_cell_index = int(changes[2]["newCellIndex"])
                #print(dataset[0])
                # remove cell_index from coll definition
                #print(new_cell_index)
                #c_idx, col = search_cell_column(dataset[0]["cols"],new_cell_index)
                #dataset[0]["cols"].pop(new_cell_index)
                
                # remove column
                dataset[0]["cols"].append(col)
                #print(c_idx,col)
                #print(dataset[0]["cols"])

                # remove data
                print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])
                    #dataset[2]["rows"][c_key]["cells"].insert(new_cell_index,c_val)
                    dataset[2]["rows"][c_key]["cells"].append(c_val)
                    #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,new_cell_index,None,c_val,c_key,None))

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
                dataset[0]["cols"].pop(oldColumnIndex)
                #print(oldColumn)
                #print(dataset[0]["cols"])
                #print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])                
                    dataset[2]["rows"][c_key]["cells"][cellIndex] = None
                    #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,cellIndex,c_val,None,c_key,cellIndex))

                #break
            elif changes[1] == "com.google.refine.model.changes.ColumnSplitChange" :
                #print(changes[2])
                print(dataset[2]["rows"][0]["cells"])
                # get the cell index
                index_col = []
                #print(changes[2]["new_columns"])
                #break
                #for col_name in changes[2]["new_columns"]:
                #    index_col.append(search_cell_column_byname(dataset[0]["cols"],col_name)[1]["cellIndex"])
                #print(index_col)
                #break
                # remove column metadata
                """
                for ind in range(changes[2]["columnNameCount"]):
                    #icol, col = search_cell_column(dataset[0]["cols"],ind)
                    dataset[0]["cols"].append(col)
                """
                len_col = len(dataset[2]["rows"][0]["cells"])

                idx, ori_column = search_cell_column_byname(dataset[0]["cols"],changes[2]["columnName"])
                for ic, newc in enumerate(changes[2]["new_columns"]):
                    tt = json.loads("""{"cellIndex": 0, "originalName": "", "constraints": "{}", "type": "", "format": "default", "title": "", "description": "", "name": ""}""")                    
                    tt["cellIndex"] = len_col+ic+1
                    tt["originalName"] = newc
                    tt["name"] = newc
                    dataset[0]["cols"].insert(idx+ic+1,tt)

                #dataset[0]["cols"].append(col)
                #print(dataset[0]["cols"])
                #break


                # remove cells on row data 
                # print(changes[2]["new_cells"])
                for c_key in changes[2]["new_cells"].keys():
                    #dataset[2]["rows"][c_key]["cells"]
                    for ind in range(int(changes[2]["columnNameCount"])):
                        #cell_changes.write("{},{},{},{},{},{},{},{},{}\n".format(order,change_id,changes[1],c_key,ind,None,dataset[2]["rows"][c_key]["cells"][ind],c_key,ori_column[1]["cellIndex"]))
                        if changes[2]["new_cells"][c_key][ind] is None:
                            dataset[2]["rows"][c_key]["cells"].append(None)
                        else:
                            dataset[2]["rows"][c_key]["cells"].append({"v": changes[2]["new_cells"][c_key][ind][1:-1]})
                        #dataset[2]["rows"][c_key]["cells"] = 

                print(dataset[2]["rows"][0]["cells"])
                #break
            else:
                print(changes[2])                
                break
        #break
        print(dataset[0]["cols"])
        print(dataset[2]["rows"][0]["cells"])
    #pass
