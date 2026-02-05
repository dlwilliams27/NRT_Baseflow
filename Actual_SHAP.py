import shap
import pandas as pd
from scipy.stats import randint, uniform
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV, GridSearchCV
from sklearn.metrics import r2_score, root_mean_squared_error, mean_absolute_error
import pathlib as path
import numpy as np

#can use lists, numpy arrays, or pd df
dir= path.Path(r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\SHAP Results")
path=r"C:\Users\Delanie Williams\OneDrive - The University of Alabama\Research\Baseflow Project\CatchmentStats\StatswRegwAttr.csv"
data=pd.read_csv(path)
data.dropna(how='any', inplace=True)

#setting-up input and output datasets
print(data.shape)
inputs=data.iloc[:,:58]
outputs=data[['kge_12_o']] #could try rmse if this continues to not work
inputs['gage_id']=data['gage_id']
outputs['gage_id']=data['gage_id']
outputs['gage_id']=data['gage_id'].astype(str).str.zfill(8)
inputs['gage_id']=data['gage_id'].astype(str).str.zfill(8)
outputs.set_index('gage_id', inplace=True)
inputs.set_index('gage_id', inplace=True)
print(inputs.columns)
inputs.drop(columns=['high_prec_timing','low_prec_timing','geol_1st_class','geol_2nd_class','dom_land_cover'], inplace=True)

print(inputs.shape)


#initializing the model
X_train, X_test, y_train, y_test = train_test_split(inputs, outputs, test_size=0.25) #train test split maintains pandas indices internally
print(X_train.shape, X_test.shape, y_train.shape, y_test.shape)

#switching y-values to numpy array (flattened)-> output in (n,) format
y_train=y_train.squeeze()
y_test=y_test.squeeze()

#initializing
model = RandomForestRegressor()

#Parameter ranges to try for the RandomForestRegressor
param_grid= {
    'n_estimators': np.arange(50,200,15),
    'max_features': np.arange(0.1, 1, 0.1), #fraction, so 0.1 * n_features
    'max_depth': [3,5,7,9],
    'min_samples_split': np.arange(1, 11, 1),
    'bootstrap': [True],
    'max_samples' : [0.3, 0.5, 0.8] #specifically for bootstrap, fraction similar to max_features for each feature
}

#Hyperparameter tuning
random_search= GridSearchCV(
    model,
    param_grid=param_grid,
    scoring='r2',
    return_train_score=True)

#fitting the model with random searchCV, take the most time
best_model=random_search.fit(X_train, y_train)
best_score= random_search.best_score_
best_estimator= random_search.best_estimator_
best_params= random_search.best_params_

#initial training fit scores
print(f"Best Score: {best_score}") #cross validated training performance
print(f"Best Estimator: {best_estimator}")
print(f"Best Params: {best_params}")

#test statistics
y_pred=best_model.predict(X_test)
r2=r2_score(y_test, y_pred)
rmse=root_mean_squared_error(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)
print(f"The R^2 score: {r2}")
print(f"The root mean squared error: {rmse}") #square root of (average squared btwn pred and actual values) (penalizes larger errors more heavily)
print(f"The mean absolute error: {mae}") #average dif btwn actual and pred.

#SHAP stuff
explainer = shap.TreeExplainer(best_estimator)
shap_values = explainer(X_test)
shap.summary_plot(shap_values, X_test)

#retrieving test data
results_df = pd.DataFrame({
    'y_test': y_test, 'y_pred': y_pred}, index=y_test.index)

output_path = dir / 'RF_Results_Yearly.csv'
output_path.parent.mkdir(parents=True, exist_ok=True)
results_df.to_csv(output_path, index=True)






