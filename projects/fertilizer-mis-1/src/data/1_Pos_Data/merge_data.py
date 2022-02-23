# import libraries
import pandas as pd
import os

raw_path = 'data/raw'
years = ['2015','2016','2017','2018','2019','2020','2021']

for state in os.listdir(raw_path):
    if state == 'Andaman and Nicobar Islands':
        continue
    if len(state.split('.')) == 1:
        state_path = os.path.join(raw_path,state)
        for year in years:
            temp_path = 'data/interim'
            temp_path = os.path.join(temp_path, state)
            temp_path = os.path.join(temp_path, year+'.csv')
            if os.path.isfile(temp_path):
                print(temp_path)
                # df = pd.read_csv(temp_path)
                # columns = df.columns[:-3]
                # df = df[columns]
                # df.to_csv(temp_path, index=False, encoding='utf-8')
                continue
            dataframe = []
            for dis in os.listdir(state_path):
                dis_path = os.path.join(state_path,dis)
                dis_path = os.path.join(dis_path,year)
                if os.path.isdir(dis_path):
                    for month in os.listdir(dis_path):
                        month_path = os.path.join(dis_path, month)
                        for day in os.listdir(month_path):
                            day_path = os.path.join(month_path,day)
                            if len(dataframe)==0:
                                dataframe = pd.read_csv(day_path)
                                columns = dataframe.columns[:-3]
                                dataframe = dataframe[columns]
                            else:
                                df = pd.read_csv(day_path)
                                columns = df.columns[:-3]
                                df = df[columns]
                                dataframe = pd.concat([dataframe, df], ignore_index=True)
                            print(day_path)
            if not os.path.isdir(os.path.join('data/interim',state)):
                os.mkdir(os.path.join('data/interim',state))
            dataframe.to_csv(temp_path, index=False, encoding='utf-8')

# consolidate the data of Andaman and Nicobar Islands seperately    