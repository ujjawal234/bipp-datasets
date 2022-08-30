import json
import urllib.request

import pandas as pd

# requesting data
with urllib.request.urlopen("https://www.npci.org.in/files/npci/TdBd.json") as url:
    result = json.loads(url.read().decode())


df = pd.DataFrame(result["data"])

# converting to csv
df.to_csv(r"data\raw\npciBDTD.csv", index=False)
