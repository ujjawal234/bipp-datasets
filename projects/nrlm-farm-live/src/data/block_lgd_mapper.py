from pathlib import Path

import numpy as np
import pandas as pd
from fuzzywuzzy import process

# defining directories
dir_path = Path.cwd()
interim_path = Path.joinpath(dir_path, "data", "interim")
external_path = dir_path.joinpath("data", "external")

# change in lgd file name accordingly
lgd_in_file = external_path.joinpath("Blocks.csv")

# change in file name accordingly
in_file = interim_path.joinpath("fl2_district_lgd_mapped.csv")

# path to hold iterated files produced during LGD WIP
lgd_iter = interim_path.joinpath("lgd_iter")
if not lgd_iter.exists():
    lgd_iter.mkdir(parents=True)
lgd_iter_file = lgd_iter.joinpath("lgd_iter_file.csv")


def lgd_master():
    def lgd_file_prep():
        # Importing LGD mapping index
        lgd = pd.read_csv(lgd_in_file, encoding="ISO8859")
        # lgd.drop("St_Cs2011_code", axis=1, inplace=True)

        # renaming the concerned columns
        lgd = lgd.rename(
            columns={
                "State Name (In English)": "state",
                "District Name  (In English)": "district",
                "Block Name (In English)": "block",
            }
        )

        lgd["state"] = lgd["state"].str.upper()
        lgd["district"] = lgd["district"].str.upper()
        lgd["block"] = lgd["block"].str.upper()

        # concatenating states, districts and blocks
        lgd["state_dist"] = lgd["state"] + lgd["district"]
        lgd["state_dist_block"] = lgd["state"] + lgd["district"] + lgd["block"]

        # print(lgd.columns)
        print("LGD file has been prepared")

        return lgd

    def data_file_prep():

        # importing the interim file
        df = pd.read_csv(in_file)

        # uppercasing the block names in state files
        df["block"] = df["block"].str.replace("_", " ").str.upper()
        df["state"] = df["state"].str.replace("_", " ").str.upper()
        df["district"] = df["district"].str.replace("_", " ").str.upper()

        print("Data file has been prepared")

        return df

    def block_name_clean():
        df = data_file_prep()
        conditions = [
            # ANDHRA PRADESH
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "Y.S.R.")
            & (df["block"] == "SAKN MANDAL"),
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "SRIKAKULAM")
            & (df["block"] == "LAXMINARSUPETA"),
            # #AP : ANATAPUR - SRI SATHYA SAI
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "ANANTAPUR")
            & (
                df["block"].isin(
                    [
                        "AMARAPURAM",
                        "BATHALAPALLE",
                        "BUKKAPATNAM",
                        "CHENNE KOTHAPALLE",
                        "CHILAMATHUR",
                        "DHARMAVARAM",
                        "GANDLAPENTA",
                        "GORANTLA",
                        "GUDIBANDA",
                        "HINDUPUR",
                        "KANAGANAPALLE",
                        "KOTHACHERUVU",
                        "LEPAKSHI",
                        "MADAKASIRA",
                        "MUDIGUBBA",
                        "NALLACHERUVU",
                        "NALLAMADA",
                        "NAMBULIPULIKUNTA",
                        "OBULADEVARACHERUVU",
                        "PENU KONDA",
                        "PUTTAPARTHI",
                        "RODDAM",
                        "SOMANDEPALLE",
                        "TALUPULA",
                    ]
                )
            )
            # #AP : CHITTOOR - TIRUPATI
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "CHITTOOR")
            & (
                df["block"].isin(
                    [
                        "CHANDRAGIRI",
                        "CHINNAGOTTIGALLU",
                        "K V B PURAM",
                        "NAGALAPURAM",
                        "NARAYANAVANAM",
                        "PICHATUR",
                        "BUCHINAIDU KHANDRIGA",
                        "RAMACHANDRAPURAM",
                        "RENIGUNTA",
                        "SATYAVEDU",
                        "SRIKALAHASTI",
                        "THOTTAMBEDU",
                        "TIRUPATI RURAL",
                        "VADAMALAPETA",
                        "VARADAIAHPALEM",
                        "YERPEDU",
                        "YERRAVARIPALEM",
                    ]
                )
            )
            # #AP : CHITTOOR - ANNAMAYYA
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "CHITTOOR")
            & (
                df["block"].isin(
                    [
                        "B KOTHAKOTA",
                        "GURRAMKONDA",
                        "KALAKADA",
                        "KALIKIRI",
                        "KAMBHAMVARIPALLE",
                        "KURABALAKOTA",
                        "MANDHANPALLE",
                        "MULAKALACHERUVU",
                        "NIMMANAPALLE",
                        "PEDDAMANDYAM",
                        "PEDDATHIPPASAMUDRAM",
                    ]
                )
            )
            # #AP : Y.S.R - ANNAMAYYA
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "Y.S.R.")
            & (
                df["block"].isin(
                    [
                        "CHINNAMANDEM",
                        "CHITVEL",
                        "GALIVEEDU",
                        "LAKKIREDDIPALLE",
                        "OBULAVARIPALLE",
                        "PENAGALURU",
                        "PULLAMPETA",
                        "RAJAMPET",
                        "RAYACHOTI",
                        "T SUNDUPALLE",
                    ]
                )
            )
            # #AP : EAST GODAVARI - KONASEEMA
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "EAST GODAVARI")
            & (
                df["block"].isin(
                    [
                        "AINAVILLI",
                        "AMBAJIPETA",
                        "ATREYAPURAM",
                        "KAPILESWARAPURAM",
                        "KATRENIKONA",
                        "KOTHAPETA",
                        "MALIKIPURAM",
                        "MAMIDIKUDURU",
                        "MUMMIDIVARAM",
                        "RAMACHANDRAPURAM",
                        "RAVULAPALEM",
                        "RAZOLE",
                        "SAKHINETIPALLE",
                        "UPPALAGUPTAM",
                    ]
                )
            )
            # #AP : EAST GODAVARI - Alluri Sitharama Raju
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "EAST GODAVARI")
            & (
                df["block"].isin(
                    [
                        "CHINTUR",
                        "DEVIPATNAM",
                        "MAREDUMILLI",
                        "NELLIPAKA",
                        "RAJAVOMMANGI",
                        "VARARAMACHANDRAPURAM",
                        "RAMPACHODAVARAM",
                    ]
                )
            )
            # #AP : EAST GODAVARI - KAKINADA
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "EAST GODAVARI")
            & (
                df["block"].isin(
                    [
                        "GOLLAPROLU",
                        "KAJULURU",
                        "KAKINADA RURAL",
                        "KIRLAMPUDI",
                        "KOTANANDURU",
                        "PEDAPUDI",
                        "PRATHIPADU",
                        "SAMALKOTA",
                        "THONDANGI",
                        "UKOTHAPALLI",
                        "YELESWARAM",
                    ]
                )
            )
            # #AP : GUNTUR - PALNADU
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "GUNTUR")
            & (
                df["block"].isin(
                    [
                        "AMARAVATHI",
                        "ACHAMPETA",
                    ]
                )
            )
            # #AP : GUNTUR - Bapatla
            (df["state"] == "ANDHRA PRADESH")
            & (df["district"] == "EAST GODAVARI")
            & (df["block"].isin([]))
            # ASSAM
            (df["state"] == "ASSAM")
            & (df["district"] == "CHARAIDEO")
            & (df["block"] == "MAHMORA SONARI"),
            (df["state"] == "ASSAM")
            & (df["district"] == "KAMRUP")
            & (df["block"] == "RANGIAPART"),
            # BIHAR
            (df["state"] == " BIHAR")
            & (df["district"] == "GAYA")
            & (df["block"] == "GAYA TOWN C D"),
            (df["state"] == "BIHAR")
            & (df["district"] == "JAMUI")
            & (df["block"] == "LAKSHMIPUR"),
            (df["state"] == "BIHAR")
            & (df["district"] == "MUZAFFARPUR")
            & (df["block"] == "BARURAJ MOTIPUR"),
            (df["state"] == "BIHAR")
            & (df["district"] == "MUZAFFARPUR")
            & (df["block"] == "DHOLI MORAUL"),
            (df["state"] == "BIHAR")
            & (df["district"] == "NALANDA")
            & (df["block"] == "BIHAR"),
            (df["state"] == "BIHAR")
            & (df["district"] == "PATNA")
            & (df["block"] == "PATNA RURAL"),
            (df["state"] == "BIHAR")
            & (df["district"] == "PURBI CHAMPARAN")
            & (df["block"] == "NARKATIA"),
            # CHATTISGARH
            (df["state"] == "CHHATTISGARH")
            & (df["district"] == "GAURELLA PENDRA MARWAHI")
            & (df["block"] == "PENDRA"),
            (df["state"] == "CHHATTISGARH")
            & (df["district"] == "RAJNANDAGON")
            & (df["block"] == "MOHLA"),
            # GUJARAT
            (df["state"] == "GUJARAT")
            & (df["district"] == "CHHOTAUDEPUR")
            & (df["block"] == "NASVADI"),
            # KARNATAKA
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU URBAN")
            & (df["block"] == "ANEKAL"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU URBAN")
            & (df["block"] == "BANGALORE NORTH"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "DHARWAD")
            & (df["block"] == "HUBLI"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "KOPPAL")
            & (df["block"] == "KUKKANURU"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU URBAN")
            & (df["block"] == "BANGALORE EAST"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "BENGALURU URBAN")
            & (df["block"] == "BANGALORE SOUTH"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "UTTARA KANNADA")
            & (df["block"] == "JOIDA"),
            (df["state"] == "KARNATAKA")
            & (df["district"] == "KALABURAGI")
            & (df["block"] == "GULBARGA"),
            # MADHYA PRADESH
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "AGAR MALWA")
            & (df["block"] == "BADOD"),
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "HOSHANGABAD")
            & (df["block"] == "MAKHAN NAGAR"),
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "HOSHANGABAD")
            & (df["block"] == "NARMADAPURAM"),
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "NARSINGHPUR")
            & (df["block"] == "SAIKHEDAGADARWARA"),
            (df["state"] == "MADHYA PRADESH")
            & (df["district"] == "CHHATARPUR")
            & (df["block"] == "GAURIHAR"),
            # MAHARASHTRA
            (df["state"] == "MAHARASHTRA")
            & (df["district"] == "BULDHANA")
            & (df["block"] == "DEULGAON RAJA"),
            (df["state"] == "MAHARASHTRA")
            & (df["district"] == "KOLHAPUR")
            & (df["block"] == "BAVDA"),
            (df["state"] == "MAHARASHTRA")
            & (df["district"] == "NASHIK")
            & (df["block"] == "TRIMBAKESHWAR"),
            (df["state"] == "MAHARASHTRA")
            & (df["district"] == "SANGLI")
            & (df["block"] == "WALWA"),
            # MANIPUR
            (df["state"] == "MANIPUR")
            & (df["district"] == "IMPHAL EAST")
            & (df["block"] == "IMPHAL EAST II KEIRAO"),
            (df["state"] == "MANIPUR")
            & (df["district"] == "IMPHAL EAST")
            & (df["block"] == "IMPHAL EAST I SAWOMBUNG"),
            (df["state"] == "MANIPUR")
            & (df["district"] == "IMPHAL EAST")
            & (df["block"] == "KHETRIGAO"),
            (df["state"] == "MANIPUR")
            & (df["district"] == "IMPHAL WEST")
            & (df["block"] == "IMPHAL WEST II WANGOI"),
            (df["state"] == "MANIPUR")
            & (df["district"] == "IMPHAL WEST")
            & (df["block"] == "IMPHAL WEST I HAORANGSABAL"),
            # SIKKIM
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "GANGTOK NANDOK"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "MANGAN")
            & (df["block"] == "DZONGU"),
            # TAMIL NADU
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "ERODE")
            & (df["block"] == "GOBI"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "ERODE")
            & (df["block"] == "TN PALAYAM"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "SALEM")
            & (df["block"] == "MAC CHOULTRY"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "SALEM")
            & (df["block"] == "PNPALAYAM"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "TIRUCHIRAPPALLI")
            & (df["block"] == "TPET"),
            (df["state"] == "TAMIL NADU")
            & (df["district"] == "TIRUVARUR")
            & (df["block"] == "THIRUTHURAIPOONDI"),
            # TRIPURA
            (df["state"] == "TRIPURA")
            & (df["district"] == "DHALAI")
            & (df["block"] == "AMBASSA"),
            (df["state"] == "TRIPURA")
            & (df["district"] == "DHALAI")
            & (df["block"] == "CHAWMANU"),
            # WEST BENGAL
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "ALIPURUDUAR")
            & (df["block"] == "MADARIHATBIRPARA"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "KALIMPONG ONE"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "KALIMPONG TWO"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "PASCHIM BARDHAMAN")
            & (df["block"] == "DURGAPUR FARIDPUR"),
            # PUNJAB
            (df["state"] == "PUNJAB")
            & (df["district"] == "TARAN TARAN")
            & (df["block"] == "KHADOOR SAHIB"),
            # UP
            (df["state"] == "UTTAR PRADESH")
            & (df["district"] == "MORADABAD")
            & (df["block"] == "DINGARPUR"),
            (df["state"] == "UTTAR PRADESH")
            & (df["district"] == "FIROZABAD")
            & (df["block"] == "KHERGARH"),
            # RAJASTHAN
            (df["state"] == "RAJASTHAN")
            & (df["district"] == "SRI GANGANAGAR")
            & (df["block"] == "SRI VIJAYNAGAR"),
            # JHARKHAND
            (df["state"] == "JHARKHAND")
            & (df["district"] == "CHATRA")
            & (df["block"] == "HUNTERGANJ"),
            # TELANAGANA
            (df["state"] == "TELANGANA")
            & (df["district"] == "KARIMNAGAR")
            & (df["block"] == "TIMMAPUR LMD COLONY"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "NALGONDA")
            & (df["block"] == "GUNDLA PALLE"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "RANGA REDDY")
            & (df["block"] == "CHOUDERGUDEM"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "SURYAPET")
            & (df["block"] == "JAJIREDDI GUDEM"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WANAPARTHY")
            & (df["block"] == "VEEPANGANDLA"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "NALLABELLY"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "YADADRI BHUVANAGIRI")
            & (df["block"] == "POCHAMPALLE"),
        ]

        options = [
            # ANDHRA PRADESH
            "SRI AVADUTHA KASINA",
            "L.N PETA",
            # #AP : ANATAPUR - SRI SATHYA SAI
            "SRI SATHYA SAI",
            # #AP : CHITTOOR - TIRUPATI
            "TIRUPATI",
            # #AP : CHITTOOR - Annamayya
            "ANNAMAYYA",
            # #AP : Y.S.R - ANNAMAYYA
            "ANNAMAYYA",
            # #AP : EAST GODAVARI - KONASEEMA
            "KONASEEMA",
            # #AP : EAST GODAVARI - Alluri Sitharama Raju
            "ALLURI SITHARAMA RAJU",
            # #AP : EAST GODAVARI - KAKINADA
            "KAKINADA",
            # #AP : GUNTUR - PALNADU
            "PALNADU",
            # ASSAM
            "SONARI",
            "RANGIA",
            # BIHAR
            "NAGAR",
            "LAXMIPUR",
            "MOTIPUR",
            "MURAUL",
            "BIHARSHARIF",
            "PATNA SADAR",
            "CHAWRADANO",
            # CHATTISGARH
            "GAURELLA-1",
            "MOHALA (TD)",
            # GUAJRAT
            "NASWADI",
            # KARNATAKA
            "ANEKAL",
            "BENGALURU NORTH",
            "HUBBALLI",
            "KUKUNOOR",
            "BENGALURU EAST",
            "BENGALURU SOUTH",
            "SUPA",
            "KALABURAGI",
            # MADHYA PRADESH
            "BAROD",
            "BABAI",
            "HOSHANGABAD",
            "SAINKHEDA",
            "BARIGARH",
            # MAHARASHTRA
            "D. RAJA",
            "GAGAN BAVADA",
            "TRIMBAK",
            "VALVA-ISLAMPUR",
            # MANIPUR
            "KEIRAO CD BLOCK",
            "Sawombung CD Block",
            "KSHETRIGAO CD BLOCK",
            "WANGOI",
            "HAORANGSABAL",
            # SIKKIM
            "NANDOK",
            "MANGAN",
            # TAMIL NADU
            "GOPICHETTIPALAIYAM",
            "THOOCKANAICKENPALAIYAM",
            "MACDONALDS CHOULTRY",
            "PEDDANAICKENPALAYAM",
            "TATTAYYANGARPETTAI",
            "TIRUTTURAIPPUNDI",
            # TRIPURA
            "Ganganagar",
            "MANU",
            # WEST BENGAL
            "MADARIHAT",
            "KALIMPONG-I",
            "KALIMPONG-II",
            "FARIDPUR - DURGAPUR",
            # PUNJAB
            "KHADUR-SAHIB-10",
            # UP
            "BILARI",
            "JASRANA",
            # RAJASTHAN
            "VIJAINAGAR",
            # JHARKHAND
            "SHALIGRAM RAM NARAYANPUR ALIAS HUNTERGANJ",
            # TELANGANA
            "THIMMAPUR (L.M.D.)",
            "GUNDLAPALLY (DINDI)",
            "Jilled Chowdergudem",
            "JAJI REDDI GUDEM (ARVAPALLY)",
            "WEEPANGANDLA",
            "NALLA BELLI",
            "B.POCHAMPALLY",
        ]

        df["block"] = np.select(conditions, options, default=df["block"])

        # for some blocks in SIKKIM
        conditions = [  # SIKKIM
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "PAKYONG"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "PARAKHA"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "REGU"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "DUGA"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GANGTOK")
            & (df["block"] == "RHENOCK"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "CHUMBONG CHA KUNG"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "DARAMDIN"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "KAALUK"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "MANGALBAREY"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "BAIGUNEY"),
            (df["state"] == "SIKKIM")
            & (df["district"] == "GYALSHING")
            & (df["block"] == "SORENG"),
            # TELANGANA
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "SHAYAMPET"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "PARKAL"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "ATMAKUR"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "DAMERA"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "BHEEMADEVARPALLE"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "DHARMASAGAR"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "ELKATHURTHI"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "HASANPARTHY"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "INAVOLE"),
            (df["state"] == "TELANGANA")
            & (df["district"] == "WARANGAL")
            & (df["block"] == "VELAIR"),
            # WEST BENGAL
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "DARJEELING PULBAZAR"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "JOREBUNGLOW SUKHIA POKHARI"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "KURSEONG"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "MIRIK"),
            (df["state"] == "WEST BENGAL")
            & (df["district"] == "KALIMPONG")
            & (df["block"] == "RANGLI RANGLIOT"),
        ]
        options = [  # SIKKIM
            "PAKYONG",
            "PAKYONG",
            "PAKYONG",
            "PAKYONG",
            "PAKYONG",
            "SORENG",
            "SORENG",
            "SORENG",
            "SORENG",
            "SORENG",
            "SORENG",
            # TELANGANA
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            "HANUMAKONDA",
            # WEST BENGAL
            "DARJEELING",
            "DARJEELING",
            "DARJEELING",
            "DARJEELING",
            "DARJEELING",
        ]
        df["district"] = np.select(conditions, options, default=df["district"])

        return df

    def data_name_concater():
        df = block_name_clean()

        # concatenating states, districts and blocks for states

        df["state_dist"] = df["state"] + df["district"]
        df["state_dist_block"] = df["state"] + df["district"] + df["block"]

        return df

    def lgd_mapper():

        lgd = lgd_file_prep()
        df = data_name_concater()

        # lgd=lgd.iloc[:,[0,1,2,3,6]]  #to be applied to remove block name columns while iterating over districts
        lgd.drop_duplicates("state_dist_block", inplace=True)

        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        print(df1.columns)

        tabs = df1["_merge"].value_counts()

        print(tabs)

        # filtering unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            ["state_data", "district_data", "block_data", "state_dist_block"]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(
            subset="state_dist_block"
        )

        return [not_lgd_mapped, lgd, df]

    def fuzzy_mapper():

        output = lgd_mapper()
        not_lgd_mapped = output[0]
        lgd = output[1]
        df = output[2]

        # Fuzzywuzzy for mapping
        print("******* Initiating Fuzzy Mapping ********")

        # creating a list of matches for unmerged state_dist_block names
        result = [
            process.extractOne(i, lgd["state_dist_block"])
            for i in not_lgd_mapped["state_dist_block"]
        ]

        # converting and editing the fuzzy matches to a dataframe
        result = pd.DataFrame(result, columns=["match", "score", "id"])
        result.drop("id", axis=1, inplace=True)

        # creating a proxy dataframe with names of unmerged original names of state_dist_block
        not_lgd_proxy_df = (
            pd.DataFrame(not_lgd_mapped["state_dist_block"], index=None)
            .reset_index()
            .drop("index", axis=1)
        )

        # creating a dataframe for fuzzy mapper
        mapper_df = pd.concat(
            [not_lgd_proxy_df, result],
            axis=1,
            ignore_index=True,
            names=["original", "match", "score"],
        )
        mapper_df = mapper_df[mapper_df[2] >= 90]

        # creating a dictionary for fuzzy mapper
        mapper_dict = dict(zip(mapper_df[0], mapper_df[1]))

        # applying the mapper dictionary on the original processed state file to correct for fuzzy matched names
        df["state_dist_block"] = df["state_dist_block"].replace(mapper_dict)

        print(
            "*******Fuzzy Mapping for has ended. Proceeding for second round of Data Merge*******",
        )

        # Second round of data merge with update fuzzy matched names along with exact original names in the state file
        # Merging lgd and state file
        df1 = pd.merge(
            df,
            lgd,
            how="outer",
            left_on="state_dist_block",
            right_on="state_dist_block",
            validate="m:1",
            indicator=True,
            suffixes=["_data", "_LGD"],
        )

        df1["_merge"].value_counts()

        # filtering fuzzy unmapped observations
        not_lgd_mapped = df1[(df1["_merge"] == "left_only")][
            [
                "state_data",
                "district_data",
                "block_data",
                "state_dist_block",
            ]
        ]

        not_lgd_mapped = not_lgd_mapped.drop_duplicates(
            subset="state_dist_block"
        )

        tabs = df1["_merge"].value_counts()

        print(tabs)

        df1 = df1[df1["_merge"] == "both"]
        df1 = df1.rename(
            columns={
                "state_data": "state",
                # 'District Code':"district_lgd_code",
                "district_data": "district",
                " Block Code": "block_lgd_code",
                "block_LGD": "block",
            }
        )

        final_col_list = [
            "year",
            "month",
            "state_lgd_code",
            "state",
            "district_lgd_code",
            "district",
            "block",
            "value_type",
            "no_of_mahila_kisans_es",
            "no_of_mahila_kisan_es_vd",
            "no_of_mahilakisan_supported_es_vd",
            "no_of_blocks_entered_es",
            "no_of_blocks_covered_es",
            "no_of_krishi_sakhis_es_vd",
            "no_of_pasu_sakhis_es_vd",
            "no_of_van_sakhis_es_vd",
            "no_of_krishi_udyog_vd",
            "no_of_districts_entered_oth",
            "no_of_villages_covered_oth",
            "no_of_other_livelihoods_oth",
            "no_of_custom_hiring_oth",
            "no_of_blocks_covered_oth",
            "areas_covered_under_organic_oth",
            "no_of_local_groups_oth",
            "no_of_local_groups_reg_pgs_portal_oth",
            "no_of_mahila_kisan_vd",
            "no_of_mahila_kisan_hh_agri_garden_vd",
            "no_of_villages_under_vd",
            "no_of_mahila_kisans_vd",
            "no_of_producer_groups_vd",
            "no_of_pgs_formalized_vd",
            "mahila_kisans_covered_by_vd",
            "no_of_produces_groups_vd",
            "no_of_large_size_vd",
            "no_of_mahila_kisans_shareholders_vd",
            "no_of_pgs_given_oth",
            "no_of_organic_vegetable_vd",
        ]

        df1 = df1[final_col_list]

        # print("Exporting not_lgd and LGD matched as CSV file to the Interim sub directory")
        not_lgd_mapped.to_csv(lgd_iter_file, index=False)
        lgd.to_csv(str(lgd_iter) + "/lgd.csv", index=False)
        # df1.to_csv(interim_path.joinpath("fl2_block_lgd_mapped.csv"), index=False)

        return df1

    fuzzy_mapper()

    return


lgd_master()
