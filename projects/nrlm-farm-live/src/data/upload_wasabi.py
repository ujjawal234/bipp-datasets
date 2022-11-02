from pathlib import Path

from bipp.wasabi.wasabi_auth import wasabi_auth
from bipp.wasabi.wasabi_upload import wasabi_upload

dir_path = Path.cwd()
raw_path = dir_path.joinpath("data", "raw")
s3_client = wasabi_auth()

# files=list(raw_path.glob("*/*/*/*/*.csv"))
files = list(raw_path.glob(r"[!jsons]*/*/*/*/*.csv"))
bucket = "dev-data"
# wasabi_path_list=[]

# for i in files:
#     wasabi_path=str(i).split("\\")
#     wasabi_path="/".join([wasabi_path[i].lower() for i in [4,6,7,8,9,10,11]])
#     wasabi_path_list.append(wasabi_path)

for i in files:
    wasabi_path = str(i).split("\\")
    wasabi_path = "/".join(
        [wasabi_path[i].lower() for i in [4, 6, 7, 8, 9, 10, 11]]
    )

    wasabi_upload(
        s3=s3_client,
        bucket_name=bucket,
        wasabi_path=wasabi_path,
        local_file_path=i,
    )
