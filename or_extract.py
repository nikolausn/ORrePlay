import tarfile
import os
import zipfile
import os
from tqdm import tqdm
import json

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

    datafile = open(locex+"/data.txt","r",encoding="UTF-8")
    
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
    changefile = open(changefile,"r",encoding="UTF-8")
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
                new_columns = []
                if head == "columnNameCount":                    
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        new_columns.append(line)
                    header_dict["new_columns"] = new_columns

                # read new cells
                new_cells = {}
                new_rows = {}
                if head == "rowIndexCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        index = int(line)
                        new_cells[index] = [None for i in range(int(header_dict["columnNameCount"]))]
                        new_rows[index] = None
                if head == "tupleCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","")
                        for y in range(int(line)):
                            line = next(changefile).replace("\n","")
                            new_cells[y] = line
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
    # print(dataset)
    
    #print([str(x["id"])+".change.zip" for x in dataset[1]["hists"][::-1]])
    #exit()

    # read history file
    hist_dir = locex+"/history/"
    list_dir = os.listdir(hist_dir)
    #for change in sorted(list_dir)[::-1]:
    for change in [str(x["id"])+".change.zip" for x in dataset[1]["hists"][::-1]]:
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
                    if dataset[2]["rows"][r]["cells"][c] == nv:
                        # log file recorded here
                        # 0, start, cell_no, row_no, null, 1
                        # <change_id>,<operation_name,<cell_no>,<row_no>,<old_val>,<new_val>
                        dataset[2]["rows"][r]["cells"][c] = ov
                        #print(dataset[2]["rows"][r]["cells"][c],ch)
                #print(dataset[2]["rows"][0]["cells"])
            elif changes[1] == "com.google.refine.model.changes.ColumnAdditionChange":
                #print(changes[2])
                new_cell_index = int(changes[2]["newCellIndex"])
                #print(dataset[0])
                # remove cell_index from coll definition
                c_idx, col = search_cell_column(dataset[0]["cols"],new_cell_index)
                #dataset[0]["cols"].pop(new_cell_index)
                
                # remove column
                if col["name"] == changes[2]["columnName"]:
                    dataset[0]["cols"].pop(c_idx)
                #print(c_idx,col)
                #print(dataset[0]["cols"])

                # remove data
                print(changes[2])
                for c_key,c_val in changes[2]["val"].items():
                    #print(dataset[2]["rows"][c_key]["cells"][new_cell_index])
                    if dataset[2]["rows"][c_key]["cells"][new_cell_index] == c_val:
                        print(c_key,c_val)
                        dataset[2]["rows"][c_key]["cells"].pop(new_cell_index)

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

                # remove cells on row data 
                for c_key in changes[2]["new_cells"].keys():
                    dataset[2]["rows"][c_key]["cells"]
                    for ind in sorted(index_col)[::-1]:
                        dataset[2]["rows"][c_key]["cells"].pop(ind)
                print(dataset[2]["rows"][0]["cells"])
                #break
            else:
                break
        #break
        print(dataset[0])
        print(dataset[2]["rows"][0]["cells"])
    pass
