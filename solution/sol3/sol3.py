################################################################################
#                                 DETECTOR
################################################################################
"""
  Using LSTM
"""

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.utils import Sequence
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import LabelEncoder

import pandas as pd
import numpy as np

import datetime     # timedelta
import time         # time.time()
import pickle       
import os           # os.listdir()



from datautils import DataUtils, DataStats

class Detector():
    def __init__(self):
        # get models
        print("Loading models")
        self.models = {}    # dict indexed by _nodes
        nodes = ['db_003', 'db_008', 'db_001', 'db_007', 'db_009', 'db_010',
            'db_002', 'db_004', 'db_013', 'db_011', 'db_006', 'db_005',
            'db_012', 'container_001', 'container_002', 'docker_008',
            'docker_005', 'docker_002', 'docker_004', 'docker_007',
            'docker_003', 'docker_001', 'docker_006', 'redis_012', 'redis_011',
            'redis_009', 'redis_010', 'redis_002', 'redis_004', 'redis_001',
            'redis_008', 'redis_003', 'redis_007', 'redis_005', 'redis_006',
            'os_007', 'os_012', 'os_006', 'os_022', 'os_015', 'os_008',
            'os_020', 'os_011', 'os_013', 'os_016', 'os_019', 'os_021',
            'os_014', 'os_018', 'os_017', 'os_001', 'os_009', 'os_002',
            'os_010', 'os_004', 'os_003', 'os_005']
        for node in nodes:
            self.models[node] = tf.keras.models.load_model(f"lstm2/lstm_{node}")

        # get scalers
        print("Loading scalers")
        self.scalers = pickle.load(open("./all_scalers.pkl", 'rb'))

        # detector params
        self.WIN_PERIOD = 1
        self.WIN_LENGTH = 5
        self.TIME_UNIT = "min"

        # data
        self.esb_df = pd.DataFrame(columns=['service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time', 'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])

        # # label encoder
        # self.node_le = LabelEncoder()
        # self.kpi_le = LabelEncoder()

        # Data Utils
        self.data_utils = DataUtils()

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

    def detect(self):
        print("Starting detection...")
        THRESHOLD = 0.5
        data = self.host_df
        data['cmdb_id'] = self.data_utils.node_le.fit_transform(data['cmdb_id'])

        submission = []
        for node in data['cmdb_id'].unique():
            node_name = self.data_utils.node_le.inverse_transform([node])[0]
            
            # prepare data
            train = data[data['cmdb_id'] == node]
            kpi = train['name'].unique()
            tensor = self.data_utils.transform_to_lstm_data(train, kpi, self.WIN_PERIOD, self.TIME_UNIT, self.WIN_LENGTH, self.scalers[node_name])
            X = tensor[:, 1:, :]
            y = tensor[:, 0, :]
            assert X.shape[0] > 0, "X is empty!"

            # make prediction
            y_predicted = self.models[node_name].predict(X)

            # evaluate
            mse = ((y-y_predicted)**2).mean(axis=0)
            msle = ((np.log1p(y) - np.log1p(y_predicted))**2).mean(axis=0)

            anomalous_kpis = np.where(msle > THRESHOLD)
            for k in kpi[anomalous_kpis]:
                submission.append({'timestamp': data['timestamp'].max(), 'content': [node_name, k]})  
        return pd.DataFrame(submission)


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
    step_timestamp = int(datetime.datetime.now().timestamp()*1000)      # timestamp in milliseconds
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
                print(f"{i-1} {last_message} (+{msg_count}) {timestamp}")
            print(i, message.topic, timestamp)
            trace_msg_count = 0
            last_message = message.topic            

        # append data to detector
        detector.appendData(message)
        
        # run anomaly detection every 5 minutes
        if ((timestamp - step_timestamp) > 6*60*1000):
            try:
                # check anomaly using the data from the last 5min
                result = detector.detect()

                print("Detected {} anomalies in the last 6min.".format(len(result)))           

                # submit if anomaly
                if len(result) > 0:
                    for _r in result.iterrows():
                        # log submit
                        print("Anomaly detected at the following timestamp {} with relative error of {:.2f}".format(_r['timestamp'], _r['relative_error']))
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
            
            except AssertionError as ae:
                print(ae)
            
            except KeyError as ke:
                print(f"KeyError: {ke}")
                print("\t >Ignoring, proceeding...")



################################################################################
#                                DRIVER CODE
################################################################################
if __name__ == '__main__':
    main()
