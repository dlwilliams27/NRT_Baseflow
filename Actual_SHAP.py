
import shap
import pandas as pd
from random import randint
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import RandomizedSearchCV

#can use lists, numpy arrays, or pd df
path=r""
data=pd.read_csv(path)
inputs=data[0:7] #change based off column ids
outputs=data[7:]

#initializing the model
X_train, X_test, y_train, y_test = train_test_split(inputs, outputs, test_size=0.25)
model = RandomForestRegressor()

#dictionary for the randomsearchcv, link hyper-parameters with ranges to try
param_grid= {
    'n_estimators': randint(20,200),
    "criterion": "squared_error",
    'min_samples_split': randint(1,10),
    'min_samples_leaf': randint(1,10)
}

#instance of the class
random_search= RandomizedSearchCV(
    model,
    param_distributions=param_grid,
    n_iter=50,
    cv=None,
    scoring='neg_root_mean_squared_error',
    n_jobs=-1
)

#fitting the model with random searchCV, take the most time
model_fit=random_search.fit(X_train, y_train)
print(model_fit.cv_results_)
print(f"the best model parameters are {model_fit.best_params_} with a score of {model_fit.best_score_}")

#doing SHAP on the optimized model
explain=shap.TreeExplainer(model_fit)
shap_values=explain(X_test)

#visualizing
shap.plots.beeswarm(shap_values)
shap.plots.bar(shap_values)