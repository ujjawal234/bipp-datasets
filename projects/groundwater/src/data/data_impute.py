# %%
import pandas as pd
import sklearn
from sklearn.linear_model import LinearRegression
from sklearn.impute import IterativeImputer

# importing data

project_dir = str(Path(__file__).resolve().parents[2])
print(project_dir)
parent_folder = project_dir + "/data/raw/"
data = pd.read_csv(parent_folder + "/gwlevel.csv")
# %%
# keeping only variables that need to be imputed

x_data = data.iloc[:,-5:]
# %%
# observing the correlation
x_data.corr()
# %%
# the variables' correlation values are close to 1, using Linear Regression
lr = LinearRegression()

imp = IterativeImputer(estimator=lr, verbose=2, max_iter=30, imputation_order= 'roman')

# fitting MICE
imp.fit(x_data)

# storing the transformed array
imputed_data = imp.transform(x_data)

# %%
imputed_data = pd.DataFrame(imputed_data)

column_names = ['February', 'May', 'August', 'November', 'NMIS']

imputed_data.columns = column_names
# %%
# adding the imputed data to original dataframe
data_imputed = pd.concat([data, imputed_data], axis=1, join='inner')

# removing the original values
data_imputed = data_imputed.drop(['feb', 'may','aug','nov', 'nmis'], axis =1)
# %%
melted_data = pd.melt(data_imputed, id_vars=['state', 'district', 'teh_name', 'block_name', 'wlcode', 'wellid','year', 'id', 'lat', 'lon', 'NMIS'], value_vars=['February', 'May', 'August', 'November'], value_name="water_level", var_name = "Month")
# %%
# getting the other dataset to get the LGD Codes
non_imputed_data = pd.read_csv(parent_folder + '/Ground_water_level.csv')

# %%
# creating final dataset
melted_data.to_csv(parent_folder + "/groundwater_imputed.csv", index=False)
# %%
