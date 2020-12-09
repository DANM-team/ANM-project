################################################################################
#                                 DETECTOR
################################################################################
"""
  
"""

import pandas as pd
import numpy as np

import datetime     # timedelta
import time         # time.time()
import pickle       

import os           # os.listdir()

import dask.dataframe as dd

class Detector():
    def __init__(self):
        # get models
        self.models = {}    # dict indexed by _nodes
        for _file in os.listdir(os.fsencode('./')):
            filename = os.fsdecode(_file)
            if filename.endswith('.sav'):
                node = os.path.basename(filename).split('.')[0]
                self.models[node] = pickle.load(open(filename, 'rb'))

        # data
        self.esb_df = pd.DataFrame(columns=['service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time', 'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])
    
    def appendData(self, message):
        data = json.loads(message.value.decode('utf8'))
        if message.topic == 'platform-index':
            temp = []
            for stack in data['body']:
                for item in data['body'][stack]:
                    temp.append({
                        'item_id': item['itemid'],
                        'name': item['name'],
                        'bomc_id': item['bomc_id'],
                        'timestamp': item['timestamp'],
                        'value': item['value'],
                        'cmdb_id': item['cmdb_id']
                    })
            temp_df = pd.DataFrame(temp)
            self.host_df = pd.concat([self.host_df, temp_df])

            # convert timestamps to datetime object
            self.host_df['timestamp'] = pd.to_datetime(self.host_df['timestamp'], unit='ms', errors='ignore')
                #   errors :If ‘ignore’, then invalid parsing will return the input.
                #   because it tries to convert a datetime into a datetime
            
        elif message.topic == 'business-index':
            temp = []
            for key in data['body']:
                for item in data['body'][key]:
                    temp.append({
                        'service_name': item['serviceName'],
                        'startTime': item['startTime'],
                        'avg_time': item['avg_time'],
                        'num': item['num'],
                        'succee_num': item['succee_num'],
                        'succee_rate': item['succee_rate']
                    })
            temp_df = pd.DataFrame(temp)
            self.esb_df = pd.concat([self.esb_df, temp_df])

            # convert timestamps to datetime object
            self.esb_df['start_time'] = pd.to_datetime(self.esb_df['startTime'], unit='ms', errors='ignore')
                #   errors :If ‘ignore’, then invalid parsing will return the input.
                #   because it tries to convert a datetime into a datetime

            # 'time' is like an index from the begining of timestamps (hbos doesn't handle datetime objects, so we use floats)
            self.esb_df['time'] = (self.esb_df['start_time'] - self.esb_df['start_time'].min()) / datetime.timedelta(seconds=1)  # should be 0 to 24h in seconds

        else:  # message.topic == 'trace'
            temp_df = pd.DataFrame({
                'call_type': data['callType'],
                'start_time': data['startTime'],
                'elapsed_time': data['elapsedTime'],
                'success': data['success'],
                'trace_id': data['traceId'],
                'id': data['id'],
                'pid': data['pid'],
                'cmdb_id': [data['cmdb_id']]
            })
                            

            if 'serviceName' in data:
                temp_df['service_name'] = data['serviceName']
            if 'dsName' in data:
                temp_df['ds_name'] = data['dsName']
            self.trace_df = pd.concat([self.trace_df, temp_df])

            # convert timestamps to datetime object
            self.trace_df['start_time'] = pd.to_datetime(self.trace_df['start_time'], unit='ms', errors='ignore')
                #   errors :If ‘ignore’, then invalid parsing will return the input.
                #   because it tries to convert a datetime into a datetime
        
        del temp_df
    
    def flushCache(self):
        # delete accumulated data (empty dataframes)
        self.esb_df = pd.DataFrame(columns=['service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time', 'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])        

    def _makePredictions(self):
        N_IN = 5  # use data up to 5 minutes before detection
        FREQ = 'min'
        predictions_df = pd.DataFrame(
            columns=['timestamp', 'target_node', 'target_kpi', 'target_value', 'prediction'])

        # === gather data ===
        data = self.host_df[['name', 'cmdb_id', 'timestamp', 'value']]
        data['timestamp'] = data['timestamp'].dt.round(
            '60s')  # reduce precision of timestamp
        data['hour'] = data['timestamp'].dt.hour

        # === for each node: ===
        for _node in data['cmdb_id'].unique():
            # === transform data to be interpretable by xgboost ===
            d = data[data['cmdb_id'] == _node][[
                'name', 'timestamp', 'value', 'hour']]

            ## pivot data: create columns out of every kpi 'name'
            pivot_d = pd.pivot_table(d, index='timestamp',
                                     columns='name', values='value', dropna=True)
            percent_missing = pivot_d.isnull().sum() * 100 / len(pivot_d)
            # drop columns that have too much missing values
            pivot_d = pivot_d.loc[:, percent_missing < 50]

            ## shift data to have past values of kpis as columns
            arr_of_df_with_past_values = []
            for i in range(N_IN, 0, -1):
                temp_df = pivot_d.shift(periods=i, freq=FREQ)
                temp_df.columns = ["{} (t-{}min)".format(_n, i)
                                   for _n in temp_df.columns]
                # drop rows that are empty
                temp_df.dropna(axis=0, how='all', inplace=True)
                arr_of_df_with_past_values.append(temp_df)

            # === make a prediction for every kpi ===
            for _kpi in pivot_d.columns:
                ## current values of other kpis (not the one we are going to predict)
                test_df = pd.concat(arr_of_df_with_past_values +
                                    [pivot_d.loc[:, pivot_d.columns != _kpi]])
                test_df['target_kpi'] = _kpi        # fills target_kpi colum
                test_df['target_value'] = pivot_d[_kpi]

                ## categorize data
                test_df['target_kpi'] = test_df['target_kpi'].astype(
                    'category')
                test_df = pd.get_dummies(
                    test_df, columns=['target_kpi'], prefix=['tgt_kpi'])

                test_df = test_df.reset_index()
                timestamp = test_df.reset_index()['timestamp'].max()
                # drop timestamp column
                test_df = test_df.drop(columns=['timestamp'])

                ## make prediction
                testX = test_df.loc[:, test_df.columns != 'target_value']
                predict_y = self.models[_node].predict(testX)

                ## save predictions
                predictions_df.append({
                    'timestamp': [timestamp],
                    'target_node': [_node],
                    'target_kpi': [_kpi],
                    'target_value': test_df.loc['target_value'],
                    'prediction': predict_y
                })

        # Finally, set self.predictions
        self.predictions = predictions_df        

    def detect(self):
        # make predictions
        self._makePredictions()

        # draw conclusions
        self.predictions['abs_error'] = (
            self.predictions['target_value'] - self.predictions['prediction']).abs()
        self.predictions['squared_error'] = (
            self.predictions['target_value'] - self.predictions['prediction']) ** 2
        self.predictions['relative_error'] = 100 * (
            self.predictions['target_value'] - self.predictions['prediction']).abs() / self.predictions['target_value']

        THRESHOLD = 50  # 50% of relative error
        conclusions = self.predictions[self.predictions['relative_error'] > THRESHOLD]
        return conclusions.rename({'target_node': 'node', 'target_kpi': 'kpi'})

        
            
                  


################################################################################
#                                 CONSUMER
################################################################################
'''
Example for data consuming.
'''

import requests
import json

from kafka import KafkaConsumer
# b;*dw|+M6Kv3

# Three topics are available: platform-index, business-index, trace.
# Subscribe at least one of them.
AVAILABLE_TOPICS = set(['platform-index', 'business-index', 'trace'])
CONSUMER = KafkaConsumer('platform-index', 'business-index', 'trace',
                         bootstrap_servers=['172.21.0.8', ],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')

def submit(ctx):
    '''Submit answer into stdout'''
    # print(json.dumps(data))
    assert (isinstance(ctx, list))
    for tp in ctx:
        assert(isinstance(tp, list))
        assert(len(tp) == 2)
        assert(isinstance(tp[0], str))
        assert(isinstance(tp[1], str) or (tp[1] is None))
    data = {'content': json.dumps(ctx)}
    r = requests.post(
        'http://172.21.0.8:8000/standings/submit/', data=json.dumps(data))


def main():
    '''Consume data and react'''
    # Check authorities
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    detector = Detector()
    step_timestamp = 0      # timestamp in milliseconds
    submitted_anomalies = {
        'timestamp': [],
        'node': [],
        'kpi': []
        }

    i = 0
    msg_count = 0     # useful to reduce number of log messages (there are a lot of trace message coming in)
    last_message = None
    for message in CONSUMER:
        # log message reveived by kafka
        i += 1
        data = json.loads(message.value.decode('utf8'))
        timestamp = data['timestamp'] if message.topic == 'platform-index' else data['startTime']
        if message.topic == last_message:
            msg_count += 1
        else:
            if msg_count > 1:
                print("{index} {msg} ({count}) {time}".format(index=i-1, msg=last_message, count=msg_count, time=timestamp))
            print(i, message.topic, timestamp)
            trace_msg_count = 0
            last_message = message.topic            

        # append data to detector
        detector.appendData(message)
        
        # run anomaly detection every 5 minutes
        if ((timestamp - step_timestamp) % 5*60*1000 < 10):     # 10ms tolerance (ie. every 5 minutes modulo +/-10ms)
            # check anomaly using the data from the last 5min
            result = detector.detect()           

            # submit if anomaly
            if len(result) > 0:
                for _r in result.iterrows():
                    # log submit
                    print("Anomaly detected at the following timestamp {}".format(_r['timestamp']))
                    print("/!\\ SUBMITTING: {}".format(_r['content']))
                    submit(_r['content'])

                    # update submitted anomalies and save as csv
                    submitted_anomalies['timestamp'].append(str(_r['timestamp']))
                    submitted_anomalies['node'].append(_r['content'][0])
                    submitted_anomalies['kpi'].append(_r['content'][1])
                    pd.DataFrame(submitted_anomalies).to_csv('submitted_anomalies')     # save into file to keep trace of submitted anomalies

            # flush data
            print("FLUSHING CACHE")
            step_timestamp = timestamp     # update step timestamp
            detector.flushCache()
        



################################################################################
#                                DRIVER CODE
################################################################################
if __name__ == '__main__':
    main()
