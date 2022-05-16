# import pandas as pd
import re

text = "JAMGAON ®"

print(text.isascii())

print(text)

text1 = re.sub(r"[^A-Za-z0-9_]", "", text)
text1 = "".join(text1.split("\xa0"))
text1 = "|".join(text1)

print(text1)
