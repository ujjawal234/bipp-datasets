import pandas as pd
import tabula as tb 
from tabula import read_pdf

# Read pdf into list of DataFrame
dfs = tb.read_pdf("All District_2016-17_All Crops.pdf",pandas_options={'header':None},lattice=True, pages='all')
df=pd.DataFrame(dfs[0])
print('Printinf df')
print(df)

df.loc[:5]

df.loc[5:]
dist_name=df.loc[5:][1]
dist_name


dict1={}
crop_name=[]
for i in range(14):
  p=2
  for j in range(3):
    df=dfs[i]
    key=df.loc[2][j+1]
    value=df.loc[5:][p]
    dict1[key]=value
    crop_name.append(key)
    p+=2
    
dict1



dict2={}

for i in range(14):
  p=3
  for j in range(3):
    df=dfs[i]
    key=df.loc[2][j+1]
    value=df.loc[5:][p]
    dict2[key]=value
    p+=2

crop_name


df_list=[] 

for i in crop_name:
  df_train=pd.DataFrame()
  df_train['Area']=dict1[i]
  df_train['Production']=dict2[i]
  df_train['crop_name']=(i)
  df_train.index=dist_name
  
  df_train.drop(df_train.tail(1).index,inplace=True) 
  
  df_list.append(df_train)
 
#  if Deletiing the last total column in page 14 
df_list.pop()


df_list[1]

df_all = pd.concat([df_list[i] for i in range(len(df_list))], axis=0)
df_all

df_all.index.name='District'

df_all

df_all=df_all[['crop_name','Area','Production']]

df_all

crop_type=[]
fruits_list=['Aonla/Gooseberry','Banana','Guava','Limes and Lemons','Litchi','Mango','Muskmelon','Papaya','Pineapple','Sweet Orange','Watermelon','Other Citrus']
vegetables_list=['Beans(All Including','Bitter Gourd','Bottle Gourd','Brinjal','Cabbage','Carrot','Cauliflower','Cucumber','Elephant Foot','Green Chilly','Kaddu/Pumpkin','Okra /Ladies Finger','Onion','Peas (Green)','Pointed Gourd','Potato','Radish','Sweet Potato','Tomato','Other Vegetables']
plantation_crop=['Coconut']
spices_list=['Coriander Seed','Fenugreek','Garlic','Ginger','Red Chilly','Tamarind','Turmeric']
aromatic_plants=['Other Aromatic']

for i in df_all['crop_name']:
  key=i
  
  if key in fruits_list:
    crop_type.append('fruits')
  if key in vegetables_list:
    crop_type.append('vegetables')
  if key in plantation_crop:
    crop_type.append('plantation_crop')
  if key in spices_list:
    crop_type.append('spices')
  if key in aromatic_plants:
    crop_type.append('aromatic_plants')
  


df_all['crop_type']=crop_type
df_all

df_all=df_all[['crop_name','crop_type','Area','Production']]

df_all


df_all.to_csv('parsed_2016_bihar_nhb_.csv')










print('Printinf df after making header')
print(df)

