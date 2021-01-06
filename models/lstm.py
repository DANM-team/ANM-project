# import packages
import math

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.utils import Sequence
from datetime import timedelta
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import KFold

import numpy as np
import pandas as pd
import time

import pickle

from ..utils.datautils import DataUtils, DataStats

import os

if __name__ == "__main__":
    BATCH_SIZE = 128
    WIN_PERIOD = 1
    WIN_LENGTH = 5
    TIME_UNIT = "min"

    training_data_path = "AIOps挑战赛数据"
    save_dir = "./lstm/"
    filename_format = "lstm_{}"
    models = {}
    scalers = {}
    dates = ["2020_05_22", "2020_05_23", "2020_05_24", "2020_05_25","2020_05_26","2020_05_27","2020_05_28", "2020_05_29","2020_05_30","2020_05_31"]

    data_utils = DataUtils(verbose=True)
    host, failures = data_utils.load_data(training_data_path, dates, None, 'host')
    nodes = host['cmdb_id'].unique()

    host['cmdb_id'] = data_utils.node_le.fit_transform(host['cmdb_id'])

    for node in nodes:
        print(f"===== {node} =====")
        train = host[host['cmdb_id'] == data_utils.node_le.transform([node])[0]]
        kpi = train['name'].unique()

        # prepare data
        print("Preparing data")
        scaler = StandardScaler()
        tensor = data_utils.transform_to_lstm_data(train, kpi, WIN_PERIOD, TIME_UNIT, WIN_LENGTH, scaler)
        X = tensor[:, 1:, :]
        y = tensor[:, 0, :]

        # create the Keras model.
        ts_inputs = tf.keras.Input(shape=(tensor.shape[1]-1, tensor.shape[2]))
        x = layers.LSTM(units=50, activation='tanh')(ts_inputs)
        x = layers.Dropout(0.2)(x)
        outputs = layers.Dense(tensor.shape[2], activation='linear')(x)
        model = tf.keras.Model(inputs=ts_inputs, outputs=outputs)

        # specify the training configuration.
        model.compile(optimizer=tf.keras.optimizers.Adam(learning_rate=0.01),
                    loss=tf.keras.losses.MeanSquaredLogarithmicError(),
                    metrics=['msle'])

        # fit model 
        print("Fitting")
        model.fit(x=X, y=y, batch_size=BATCH_SIZE, epochs=20, validation_split=0.8, workers=2, use_multiprocessing=True)

        # save model and scalers
        print("Saving")
        models[node] = model
        scalers[node] = scaler
        filename = save_dir + filename_format.format(node)
        model.save(filename)
    
    # pickle.dump(models, open("all_lstm_models.pkl", 'wb'))
    pickle.dump(scalers, open("all_scalers.pkl", 'wb'))

    
    
