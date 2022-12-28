#!/usr/bin/env python
# coding: utf-8

# In[3]:


from pathlib import Path

import pandas as pd
from fuzzywuzzy import process

# In[4]:


dir_path = Path.cwd()
raw_data_path = Path.joinpath(dir_path, "data", "raw")
interim_data_path = Path.joinpath(dir_path, "data", "interim")
processed_data_path = Path.joinpath(dir_path, "data", "processed")
ext_data_path = Path.joinpath(dir_path, "data", "external")


# In[5]:


lgd = pd.read_csv(Path.joinpath(ext_data_path, "lgd_district.csv"))


# In[6]:


lgd


# In[7]:


state = {}
district = {}


# In[8]:


for i in range(0, len(lgd["State Name(In English)"])):
    state[lgd["State Name(In English)"][i].rstrip()] = lgd["St_LGD_code"][i]


# In[9]:


state


# In[10]:


for j in range(0, len(lgd["District Name(In English)"])):
    district[
        lgd["State Name(In English)"][j].rstrip() + lgd["District Name(In English)"][j]
    ] = lgd["Dt_LGD_code"][j]


# In[11]:


len(district)


# In[12]:


cattle = pd.read_csv(Path.joinpath(processed_data_path, "cattle.csv"))
buffalo = pd.read_csv(Path.joinpath(processed_data_path, "buffalo.csv"))
goat = pd.read_csv(Path.joinpath(processed_data_path, "goat.csv"))
pig = pd.read_csv(Path.joinpath(processed_data_path, "pig.csv"))
sheep = pd.read_csv(Path.joinpath(processed_data_path, "sheep.csv"))


# In[13]:


buffalo


# In[14]:


cattle["state_dist"] = cattle["state_name"] + cattle["district_name"]
buffalo["state_dist"] = buffalo["state_name"] + buffalo["district_name"]
pig["state_dist"] = pig["state_name"] + pig["district_name"]
goat["state_dist"] = goat["state_name"] + goat["district_name"]
sheep["state_dist"] = sheep["state_name"] + sheep["district_name"]


# In[15]:


cattle["dist_lgd"] = ""
sheep["dist_lgd"] = ""
buffalo["dist_lgd"] = ""
pig["dist_lgd"] = ""
goat["dist_lgd"] = ""


# In[16]:


cattle["state_lgd"] = ""
sheep["state_lgd"] = ""
buffalo["state_lgd"] = ""
pig["state_lgd"] = ""
goat["state_lgd"] = ""


# In[17]:


c = {}
b = {}
p = {}
g = {}
s = {}


# In[18]:


for i in range(0, 2776):
    dist = cattle["state_dist"][i].upper()
    if dist in district:
        cattle["dist_lgd"][i] = district[dist]
    else:
        cattle["dist_lgd"][i] = "0"
        c[dist] = "0"
    st = cattle["state_name"][i].upper()
    if st in state:
        cattle["state_lgd"][i] = state[st]
    else:
        cattle["state_lgd"][i] = "0"


# In[19]:


for i in range(0, 1364):
    dist = buffalo["state_dist"][i].upper()
    if dist in district:
        buffalo["dist_lgd"][i] = district[dist]
    else:
        buffalo["dist_lgd"][i] = "0"
        b[dist] = "0"
    st = buffalo["state_name"][i].upper()
    if st in state:
        buffalo["state_lgd"][i] = state[st]
    else:
        buffalo["state_lgd"][i] = "0"


# In[20]:


for i in range(0, 1396):
    dist = goat["state_dist"][i].upper()
    if dist in district:
        goat["dist_lgd"][i] = district[dist]
    else:
        goat["dist_lgd"][i] = "0"
        g[dist] = "0"
    st = goat["state_name"][i].upper()
    if st in state:
        goat["state_lgd"][i] = state[st]
    else:
        goat["state_lgd"][i] = "0"


# In[21]:


for i in range(0, 1260):
    dist = sheep["state_dist"][i].upper()
    if dist in district:
        sheep["dist_lgd"][i] = district[dist]
    else:
        sheep["dist_lgd"][i] = "0"
        s[dist] = "0"
    st = sheep["state_name"][i].upper()
    if st in state:
        sheep["state_lgd"][i] = state[st]
    else:
        sheep["state_lgd"][i] = "0"


# In[22]:


for i in range(0, 1252):
    dist = pig["state_dist"][i].upper()
    if dist in district:
        pig["dist_lgd"][i] = district[dist]
    else:
        pig["dist_lgd"][i] = "0"
        p[dist] = "0"
    st = pig["state_name"][i].upper()
    if st in state:
        pig["state_lgd"][i] = state[st]
    else:
        pig["state_lgd"][i] = "0"


# In[23]:


print("Number of missing values (district):", end="\n")
print("Buffalo", len(buffalo[buffalo["dist_lgd"] == "0"]))
print("Cattle", len(cattle[cattle["dist_lgd"] == "0"]))
print("Pig", len(pig[pig["dist_lgd"] == "0"]))
print("Goat", len(goat[goat["dist_lgd"] == "0"]))
print("Sheep", len(sheep[sheep["dist_lgd"] == "0"]))


# In[24]:


print("Number of missing values (state):", end="\n")
print("Buffalo", len(buffalo[buffalo["state_lgd"] == "0"]))
print("Cattle", len(cattle[cattle["state_lgd"] == "0"]))
print("Pig", len(pig[pig["state_lgd"] == "0"]))
print("Goat", len(goat[goat["state_lgd"] == "0"]))
print("Sheep", len(sheep[sheep["state_lgd"] == "0"]))


# In[25]:


result = [process.extractOne(i, district) for i in b]


# In[28]:


result = pd.DataFrame(result, columns=["match", "score", "id"])
result.drop("id", axis=1, inplace=True)


# In[29]:


# In[ ]:
