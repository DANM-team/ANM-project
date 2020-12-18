"""
HYPERPARAMETERS TUNING USING GENETIC ALGORITHM
"""
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold
from scipy.optimize import differential_evolution

class Tuner:
    def __init__(self, params_lim, model_init, fixed_params, eval_metrics, kfold_splits, dtypes, seed=None):
        self.params_lim = params_lim
        self.params = list(params_lim.keys())
        self.model_init = model_init
        self.fixed_params = fixed_params
        self.kfold_splits = kfold_splits
        self.dtypes=dtypes
        self.seed = seed

    def _function(self, hyperparams, X, y):
        # Assign hyper-parameters
        model_params = {}
        for _name, _value in zip(self.params, hyperparams):
            if self.dtypes[_name] == 'int':
                model_params[_name] = int(_value)
            else:
                model_params[_name] = _value

        ## Using kfold cross validation
        if self.seed:
            kf = KFold(n_splits=self.kfold_splits, shuffle=True, random_state=self.seed)
        else :
            kf = KFold(n_splits=self.kfold_splits, shuffle=False)

        y_pred_total = []
        y_test_total = []
        # kf-fold cross-validation loop
        for train_index, test_index in kf.split(X):
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            y_train, y_test = y.iloc[train_index], y.iloc[test_index]

            # Fit xgboost with (X_train, y_train), and predict X_test
            model = self.model_init(**self.fixed_params, **model_params)
            y_pred = model.fit(X_train, y_train).predict(X_test)
            # Append y_pred and y_test values of this k-fold step to list with total values
            y_pred_total.append(y_pred)
            y_test_total.append(y_test)
        # Flatten lists with test and predicted values
        y_pred_total = [item for sublist in y_pred_total for item in sublist]
        y_test_total = [item for sublist in y_test_total for item in sublist]

        # Calculate error metric of test and predicted values: rmse
        rmse = np.sqrt(mean_squared_error(y_test_total, y_pred_total))

        # log message
        print(">Intermediate results: rmse: {}, {}".format(rmse, model_params))
        return rmse
    
    def tune_hyperparameters(self, X, y, popsize=10, mutation=0.5, recombination=0.7, tol=0.1, workers=1, maxiter=100, init=None):
        # params boundaries
        boundaries = [value for value in self.params_lim.values()]

        # extra variables
        extra_variables = (X, y)

        ## set up Differential Evolution solver
        if init: 
            solver = differential_evolution(
                func=self._function, 
                bounds=boundaries, 
                args=extra_variables, 
                strategy='best1bin',
                popsize=popsize, 
                mutation=mutation, 
                recombination=recombination, 
                tol=tol, 
                seed=self.seed, 
                workers=workers, 
                maxiter=maxiter,
                init=init
                )
        else:
            solver = differential_evolution(
                func=self._function,
                bounds=boundaries,
                args=extra_variables,
                strategy='best1bin',
                popsize=popsize,
                mutation=mutation,
                recombination=recombination,
                tol=tol,
                seed=self.seed,
                workers=workers,
                maxiter=maxiter
            )

        ## calculate best hyperparameters and resulting rmse
        best_hyperparams = solver.x
        best_rmse = solver.fun

        ## print final results
        print("Converged hyperparameters: {}".format(best_hyperparams))
        print("Minimum rmse: {:.6f}".format(best_rmse))
