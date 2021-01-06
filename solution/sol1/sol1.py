################################################################################
#                                 DETECTOR
################################################################################
"""
This solution is quite naive, and we don't actually expect good results but is worth a try nonetheless.
We are detecting anomalies using Histogram-Based Outlier Score (HBOS) on esb['avg_time'].
Then we make the assumption that the slowest traces are most likely to contain the root cause, thus we analyze them and retrieve the most common nodes of those traces. 
Finally, we use a simple correlation between esb['avg_time'] and the KPIs that describe the retrieved nodes' behavior. 
The most 2 correlated KPIs are then submit as the answer.    
"""

import pandas as pd
import numpy as np

import datetime     # timedelta
import time         # time.time()

from kenchi.outlier_detection.statistical import HBOS   # this modules implements HBOS

class Detector():
    def __init__(self):
        # training data from esb file to build model
        x_train = pd.read_csv('esb.csv', header=0).rename(columns={'startTime': 'start_time'})[['start_time', 'avg_time']]
        x_train['start_time'] = pd.to_datetime(x_train['start_time'], unit='ms')
        x_train['time'] = (x_train['start_time'] - x_train['start_time'].min()) / \
            datetime.timedelta(seconds=1)  # should be 0 to 24h in seconds
        
        # construct model
        self.hbos = HBOS(contamination=0.01, novelty=True).fit(x_train[['time', 'avg_time']])

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

    def detectESB(self):
        # check if esb_df is not empty
        if self.esb_df.shape[0] > 0:
            # if succee_rate drops below 1, there is an anomaly
            failed_esb_res = self.esb_df[self.esb_df['succee_rate'] < 1.0]['start_time']
            if not failed_esb_res.empty:
                return failed_esb_res.tolist()
            try :
                # make a prediction
                y_pred = self.hbos.predict(self.esb_df[['time', 'avg_time']])

                # results : timestamp of anomalies
                res = self.esb_df['start_time'].iloc[np.where(y_pred < 1)].tolist()

                return res
            except ValueError as e:
                print(e)
                print(self.esb_df)
                print(self.esb_df.info())
                return None
        else :
            # print("esb_df empty, cannot predict value!")
            return None
        
        
    def findRootCause(self, timestamp):
        res = []
        # ===== TRACE ANALYSIS =====
        n_slowest = 5     # retrieve the slowest n traces

        # select root of traces that starts in the time interval [anomaly -10min; anomaly +1min]
        d = self.trace_df[(self.trace_df['pid'] == 'None') & (
            timestamp - self.trace_df['start_time'] < datetime.timedelta(minutes=10)) & (self.trace_df['start_time'] - timestamp < datetime.timedelta(minutes=1))]
        d = d[['elapsed_time', 'trace_id']]

        # sort traces by elapsed time (in first position, the largest elapsed_time because it's most likely this one that causes problem)
        d = d.sort_values(by='elapsed_time', ascending=False)

        # 5 slowest traces
        slowest_traces = self.trace_df[self.trace_df['trace_id'].isin(
            d['trace_id'].iloc[:n_slowest])]
        slowest_traces = slowest_traces[['cmdb_id', 'trace_id']]

        # get the common (shared) nodes
        common_nodes = slowest_traces['cmdb_id'].unique()
        for tid in slowest_traces['trace_id'].values:
            nodes = slowest_traces[slowest_traces['trace_id'] == tid]['cmdb_id']
            common_nodes = common_nodes[np.isin(element=common_nodes, test_elements=nodes)]
        
        # if the previous failed (ie there is no exact common nodes), then check for the most common nodes (majority element only)
        if len(common_nodes) <= 0: 
            # alternative to common nodes: most common nodes
            #       threshold = how to quantify "most common". We need to determine some way of saying "this is common" and "this is not"
            #       currently, threshold = every nodes which represent more than 30% of the total 
            threshold = 0.30
            frequencies = slowest_traces['cmdb_id'].value_counts(normalize=True).sort_values(ascending=False) # count values and sort by descending occurence (most frequent first)
            if not frequencies.empty :
                common_nodes = frequencies[frequencies > threshold].index.tolist()
            del frequencies
        
        # log results
        if len(common_nodes) > 0:
            print("I've retrived the most common node of the slowest {} traces : {}".format(
                n_slowest, common_nodes))

        # ===== KPI anomaly detection =====
        window = [20, 2]    # time window (-20min, +2min)

        for _node in common_nodes:
            # get the associated host_kpi_df (time interval = [T-20min, T+2min])
            h = self.host_df[(self.host_df['cmdb_id'] == _node) & (timestamp - self.host_df['timestamp'] <
                                                                datetime.timedelta(minutes=window[0])) & (self.host_df['timestamp'] - timestamp < datetime.timedelta(minutes=window[1]))]
            h = h[['cmdb_id', 'name', 'bomc_id', 'value', 'timestamp']]
            h = h[h.groupby('name')['value'].transform('std') > 0] # remove flat plots (because they obviously are not the anomaly since it doesn't change)
            h = h.set_index('timestamp').sort_index()

            # get esb['avg_time'] corresponding to the same time interval
            e = self.esb_df[(timestamp - self.esb_df['start_time'] < datetime.timedelta(minutes=window[0]))
                    & (self.esb_df['start_time'] - timestamp < datetime.timedelta(minutes=window[1]))]
            e = e[['start_time', 'avg_time']]
            e = e.set_index('start_time').sort_index()

            # merge those 2 dataframe on start_time and timestamp
            merged_df = pd.merge_asof(h, e, right_on='start_time',
                                    left_on='timestamp', tolerance=datetime.timedelta(seconds=10))
            # fill nan values in avg_time series with previous value (ordered by timestamp)
            merged_df['avg_time'] = merged_df['avg_time'].fillna(method='ffill')

            # compute correlation with esb['avg_time'] of each kpi
            abs_correlations_sorted = merged_df.groupby('name').corrwith(merged_df['avg_time']).abs()
                
            if abs_correlations_sorted.empty:
                kpi = None
            else :
                abs_correlations_sorted.sort_values('value', ascending=False)   # sort by correlation value
                kpi = abs_correlations_sorted.iloc[0].reset_index()['name']
            
            # append result to res 
            res.append([_node, kpi])

            # log results
            print("I found that the kpi most susceptible of being the cause of the anomaly of node {} is {}".format(
                res[-1][0],
                res[-1][1]))
        return res                


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
    submitted_anomalies = {}

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

        # flush every 30min
        if (timestamp - step_timestamp >= 30*60*1000):
            print("FLUSHING CACHE")
            step_timestamp = timestamp     # update step timestamp
            detector.flushCache()

        # append data to
        detector.appendData(message)
        
        # run anomaly detection every minute
        if ((timestamp - step_timestamp) % 1*60*1000 < 10):     # 10ms tolerance (ie. every minute modulo +/-10ms)
            # check anomaly
            tmsp = detector.detectESB()           

            # if anomaly, find root Cause
            if tmsp != None and len(tmsp) > 0:
                for _t in tmsp:
                    if str(_t) not in submitted_anomalies.keys():
                        # log results
                        res = detector.findRootCause(_t)
                        if len(res) > 0:
                            # log submit
                            print("Anomaly detected at the following timestamp {}".format(_t))
                            print("/!\\ SUBMITTING: {}".format(res))
                            submit(res)
                            submitted_anomalies[str(_t)] = res
                            pd.DataFrame(submitted_anomalies).to_csv('submitted_anomalies')     # save into file to keep trace of submitted anomalies

        



################################################################################
#                                DRIVER CODE
################################################################################
if __name__ == '__main__':
    main()
