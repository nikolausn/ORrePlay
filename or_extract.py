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

    datafile = open(locex+"/data.txt","r")
    
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
    changefile = open(changefile,"r")
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
                val = line.split("=")[-1]
                data_dict[head] = val
            try:
                line=next(changefile).replace("\n","")
            except:
                break
        #print(data_row)
    elif command_name == "com.google.refine.model.changes.ColumnAdditionChange":
        line = next(changefile).replace("\n","")
        print(line)
        while line:
            if line=="/ec/":
                break
            else:
                head = line.split("=")[0]
                val = line.split("=")[-1]
                header_dict[head] = val
                rows = {}
                #print(head)
                if head == "newCellCount":
                    for x in range(int(val)):
                        line = next(changefile).replace("\n","").split(";")
                        val = json.loads(line[1])
                        row = int(line[0])
                        rows[row] = val
                    header_dict["val"] = rows
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


if __name__ == "__main__":
    # extract project
    file_name = "airbnb_dirty-csv.openrefine.tar.gz"
    locex,_ = extract_project(file_name)
    # extract data
    dataset = read_dataset(locex)
    # print(dataset)
    
    # read history file
    hist_dir = locex+"/history/"
    list_dir = os.listdir(hist_dir)
    for change in sorted(list_dir)[::-1]:
        # print(change)
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
                    c = int(ch["cell"])
                    nv = json.loads(ch["new"])
                    ov = json.loads(ch["old"])
                    #print(dataset[2]["rows"][r])            
                    #print(dataset[2]["rows"][r]["cells"][c],ch)
                    #print(dataset[2]["rows"][r]["cells"][c],nv)
                    if dataset[2]["rows"][r]["cells"][c] == nv:
                        dataset[2]["rows"][r]["cells"][c] = ov
                        print(dataset[2]["rows"][r]["cells"][c],ch)
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
                for r in dataset[2]["rows"]:
                    r["cells"].pop(c_idx)
                    
                #print(dataset[2]["rows"])

                #for r in dataset[2]["rows"]
            else:
                print(changes)
                break
        #break
    pass
