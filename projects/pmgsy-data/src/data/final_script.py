from numpy import nan
import pandas as pd
from pathlib import Path
import glob
#Required path in list F--> M --> Data Cleaning Step --> Consolidating the data
local=r"D:\Vanshika\bipp-datasets\projects\pmgsy-data\data\raw\1_physical-progress-of-works\unprocesses"
path=r'D:\Vanshika\bipp-datasets\projects\pmgsy-data'
f=[]
m=[]
def get_filePath(local):
    for path in Path(local).iterdir():
        if path.is_file():
            # print(path)
            f.append(path)
            #Data_cleaning(path)
        else:
            get_filePath(path)
    # print(f)

def filteringpathnames(list):
    for path in list:
        stri=str(path)
        if stri.find('All Collaborations')==-1:
            if stri.find('All Batches')==-1:
                m.append(path)
def datacleaning(path):
    stri=str(path)
    k=stri.split("\\")
    tehsil=k[-3]
    district=k[-4]
    state=k[-5]
    df=pd.read_csv(path)
    df1=df.iloc[2:,2:]
    df1.columns=df1.iloc[0]
    df1.reset_index(drop=True,inplace=True)
    df1['teshsil_name']=tehsil
    df1['district_name']=district
    df1['state_name']=state
    df1=df1.replace('-','')
    cols=['Length','Pavement Cost','No. of CD Works','CD Work Cost','LSB Cost','LSB State Cost','Completed Length','Expenditure Till Date','Total Cost','Population','SC/ST Population']
    for col in cols:
        df1[col]=pd.to_numeric(df1[col], errors='coerce')
        df1[col].fillna(0)
    df1.columns=df1.columns.str.lower()
    df1.drop([0],inplace=True)
    df1.drop([len(df1)],inplace=True) #dropping row with total
    renamed=k[-5]+'-'+k[-4]+'-'+k[-3]+'-'+k[-1]
    df1.to_csv(renamed+".csv",index=False)

get_filePath(local)
print(len(f))
filteringpathnames(f)
print(len(m))
for loc in m:
    datacleaning(loc)
    print('data cleaning..............')
all_files = glob.glob(path + "/*.csv")
#print(all_files)

li = []
for filename in all_files:
    print('converting to dataframe')
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)
print('concating')
frame = pd.concat(li, axis=0, ignore_index=True)
frame.to_csv('final_file1',index=False)
