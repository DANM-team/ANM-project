################################################################################
#                                 DETECTOR
################################################################################
"""
ESB detection : moving average.
Node detection : traces are sorted by elapsed_time (slowest first), then the most common nodes from all of these traces are retrieved.
KPI detection: correlation of KPI with esb['avg_time']. The most correlated are the anomalous KPIs.  
"""

from kafka import KafkaConsumer
import json
import requests
import pandas as pd
import numpy as np

import datetime     # timedelta
import time         # time.time()

class Detector():
    def __init__(self):
        # data
        self.esb_df = pd.DataFrame(columns=[
                                   'service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(
            columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time',
                                              'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])
        
        self.anomaly_scores = None

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
            self.host_df['timestamp'] = pd.to_datetime(
                self.host_df['timestamp'], unit='ms', errors='ignore')
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
            self.esb_df['start_time'] = pd.to_datetime(
                self.esb_df['startTime'], unit='ms', errors='ignore')
            #   errors :If ‘ignore’, then invalid parsing will return the input.
            #   because it tries to convert a datetime into a datetime

            # 'time' is like an index from the begining of timestamps (hbos doesn't handle datetime objects, so we use floats)
            self.esb_df['time'] = (self.esb_df['start_time'] - self.esb_df['start_time'].min(
            )) / datetime.timedelta(seconds=1)  # should be 0 to 24h in seconds

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
            self.trace_df['start_time'] = pd.to_datetime(
                self.trace_df['start_time'], unit='ms', errors='ignore')
            #   errors :If ‘ignore’, then invalid parsing will return the input.
            #   because it tries to convert a datetime into a datetime

        del temp_df

    def flushCache(self):
        # delete accumulated data (empty dataframes)
        self.esb_df = pd.DataFrame(columns=[
                                   'service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(
            columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time',
                                              'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])

    def detectESB(self):
        # check if esb_df is not empty
        if self.esb_df.shape[0] > 0:
            # if succee_rate drops below 1, there is an anomaly
            failed_esb_res = self.esb_df[self.esb_df['succee_rate']< 1.0]['start_time']
            if not failed_esb_res.empty:
                return failed_esb_res.tolist()
            try:
                # calculate moving average
                ema = self.esb_df['avg_time'].ewm(alpha=0.8, adjust=False).mean()

                # make the test over EMA
                test = (ema - self.esb_df['avg_time'])**2 > 0.01
                esb_detected_anomalies = self.esb_df.loc[test, :]

                # results : timestamp of anomalies
                res = esb_detected_anomalies['start_time'].tolist()

                return res
            except ValueError as e:
                print(e)
                print(self.esb_df)
                print(self.esb_df.info())
                return None
        else:
            # print("esb_df empty, cannot predict value!")
            return None

    def updateAnomalyScore(self, timestamp):
        #  === ANOMALY SCORE ===
        traces = self.trace_df[self.trace_df['start_time'] >= datetime.datetime.fromtimestamp(timestamp/1e3)]
        
        if 'node_name' not in traces.columns:
            traces['node_name'] = traces['cmdb_id'] # CSF, FlyRemote, OSB, RemoteProcess
            traces.loc[trace_df['call_type'].isin(['LOCAL', 'JDBC']), 'node_name'] = traces.loc[trace_df['call_type'].isin(['LOCAL', 'JDBC']), 'ds_name']

        # custom function to apply after groupby
        #       this function calculates all interesting scoring features
        def f(x):
            d = pd.Series({
                'start_time': x['start_time'].min(),

                # elapsed_time
                'elapsed_time': x['elapsed_time'].mean(),
                'childs_sum_elapsed_time': x['elapsed_time_child'].sum(),
                'childs_average_elapsed_time': x['elapsed_time_child'].mean(),
                'parent_average_elapsed_time': x['elapsed_time_parent'].mean(),
                
                # call_time
                'parent_call_time': x['elapsed_time_parent'].mean() - x['elapsed_time_cousin'].sum(),
                'incoming_call_time': (x['elapsed_time_parent'].mean() - x['elapsed_time_cousin'].sum()) / x['id_cousin'].count(),
                'outgoing_call_time': x['elapsed_time'].mean() - x['elapsed_time_child'].sum(),
                'mean_outgoing_call_time': (x['elapsed_time'].mean() - x['elapsed_time_child'].sum()) / x['pid_child'].count() if x['pid_child'].count() > 0 else 0,
                
                # number of ...
                'number_of_childs': x['pid_child'].count(),
                'number_of_cousins': x['id_cousin'].count(),
                
                # success_rate
                'child_success_rate': x['success_child'].mean(),
                'parent_success_rate': x['success_parent'].mean(),
                'cousin_success_rate': x['success_cousin'].mean(),
                'success': x['success'].mean(),

                # names
                'childs_name': x['node_name_child'].dropna().unique().tolist(),
                'parents_name': x['node_name_parent'].dropna().unique().tolist(),
                'cousins_name': x['node_name_cousin'].dropna().unique().tolist(),
            })
            return d
        # get childs, parents and cousins
        childs = traces.merge(traces[['trace_id', 'pid', 'elapsed_time', 'node_name', 'success']], how='left', left_on=['trace_id', 'id'], right_on=['trace_id', 'pid'], suffixes=('', '_child'))
        parents = traces.merge(traces[['trace_id', 'id', 'elapsed_time', 'node_name', 'success']], how='left', left_on=['trace_id', 'pid'], right_on=['trace_id', 'id'], suffixes=('', '_parent'))
        cousins = traces.merge(traces[['trace_id', 'pid', 'id', 'elapsed_time', 'node_name', 'success']], how='left', left_on=['trace_id', 'pid'], right_on=['trace_id', 'pid'], suffixes=('', '_cousin'))
        
        # concat in one single dataframe
        s = pd.concat([childs, parents, cousins])

        print(f"Merging complete parents, childs and cousin are now reunited ! Applying custom scoring function on dataframe of shape {s.shape}")
        
        # groupby and apply custom function
        anomaly_score = s.groupby(['node_name', 'trace_id', 'id']).apply(f)
        anomaly_score.fillna(0, inplace=True) # nan are nodes without childs, or without parents. Thus all ..._time related to either of them is 0 (and not NaN)

        if self.anomaly_scores is None:
            self.anomaly_scores = anomaly_score
        else:
            self.anomaly_scores = self.anomaly_scores.append(anomaly_score)
        
    def findRootCause(self, timestamp):        
        # SCORING FEATURES
        res = []
        window = [20, 2]    # time window (-20min, +2min)
        traces = self.trace_df[(timestamp - self.trace_df['start_time'] < datetime.timedelta(minutes=window[0])) 
            & (self.trace_df['start_time'] - timestamp < datetime.timedelta(minutes=window[1]))]            

        # anomaly scores
        anomaly_scores = self.anomalyScore(traces)

        # calculate mean value of last n minutes (20min)
        past_mean_scores = anomaly_scores.mean(axis=0, level=0) # level0 is node_name (eventually the name of node we will submit)
        
        # recent anomaly scores (-1min, +1min)
        recent_scores = anomaly_scores[(anomaly_scores['star_time']-timestamp).abs() < datetime.timedelta(minutes=1)]
        recent_scores = recent_scores.reset_index(['trace_id', 'id'], drop=True)
        
        # recent scores_features (X-mean)/mean
        recent_scores_features = (recent_scores.loc[:, past_mean_scores.columns] - past_mean_scores) / past_mean_scores
        recent_scores_features = recent_scores_features.dropna().mean(level=0) # take mean of the changes indicator of the last minute

        # ROOT CAUSE ANALYSIS
        






        scores = recent_scores.reset_index(['trace_id', 'id'], drop=True)
        anomalies = []
        for node in scores.loc['cmdb_id'].unique():
            node_anomaly_scores = (scores.loc[node, past_mean_scores.columns] - past_mean_scores.loc[node, :]) / past_mean_scores.loc[node, :]

            THRESHOLDS = {
                'network_loss' : 0.8,
                'cpu_fault' : 0.8,
                'network_delay' : 0.8,
                'db_connect_limit_OR_close': 0.8
            }

            if 'os' in node or 'docker' in node:
                # Network delay (docker/os)

                # Network loss (docker/os)
                # if sent_queue or receive_queue is anomalous:
                if node_anomaly_scores['outgoing_call_time'] > THRESHOLDS['network_loss'] and node_anomaly_scores['incoming_call_time'] > THRESHOLDS['network_loss']:
                    if 'os' in node:
                        anomalies.append([node, "Sent_queue"])
                        anomalies.append([node, "Received_queue"])
                    else : # docker
                        anomalies.append([node, None])
                

                # CPU fault (docker)
                if node_anomaly_scores['outgoing_call_time'] > THRESHOLDS['cpu_fault'] and node_anomaly_scores['incoming_call_time'] > THRESHOLDS['cpu_fault']:
                    anomalies.append([node, "container_cpu_used"])
            
            
            elif 'db' in node:
                if node_anomaly_scores['parent_success'] > THRESHOLDS['db_connect_limit_OR_close']:
                    on_off_state = self.host_df[(self.host_df['cmdb_id'] == node)] # get host kpis
                    on_off_state = on_off_state.loc[on_off_state['name'] == 'On_Off_State', ['value', 'timestamp']] # get on_off_state values of this host
                    on_off_state = on_off_state[(on_off_state['timestamp'] - timestamp) < datetime.timedelta(minutes=1)] # get values in (-1min, +1min) interval around anomaly timestamp
                    on_off_state = on_off_state['value'].mean() >= 1 # True if on_off_state == 1, False otherwise

                    # DB connection limit (db)
                    if on_off_state:
                        anomalies.append([node, "Proc_User_Used_Pct"])
                        anomalies.append([node, "Proc_Used_Pct"])
                        anomalies.append([node, "Sess_Connect"])

                    # DB close (db)
                    else:
                        anomalies.append([node, "On_Off_State"])
                        anomalies.append([node, "tnsping_result_time"])


            
        
        return res


################################################################################
#                                 CONSUMER
################################################################################
'''
Example for data consuming.
'''


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
    step_timestamp = int(datetime.datetime.now().timestamp()*1000)  # timestamp in milliseconds
    flush_timestamp = int(datetime.datetime.now().timestamp()*1000)
    submitted_anomalies = {}

    i = 0
    # useful to reduce number of log messages (there are a lot of trace message coming in)
    msg_count = 0
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
                print("{index} {msg} ({count}) {time}".format(
                    index=i-1, msg=last_message, count=msg_count, time=timestamp))
            print(i, message.topic, timestamp)
            trace_msg_count = 0
            last_message = message.topic

        # flush every 30min
        if (timestamp - flush_timestamp >= 30*60*1000):
            print("FLUSHING CACHE")
            flush_timestamp = timestamp     # update step timestamp
            detector.flushCache()

        # append data to
        detector.appendData(message)

        # run anomaly detection every minute
        if ((timestamp - step_timestamp) > 1*60*1000):
            print(f"== Running Anomaly Detection ({timestamp}) ==")
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
                            print(
                                "Anomaly detected at the following timestamp {}".format(_t))
                            print("/!\\ SUBMITTING: {}".format(res))
                            submit(res)
                            submitted_anomalies[str(_t)] = res
                            # save into file to keep trace of submitted anomalies
                            pd.DataFrame(submitted_anomalies).to_csv(
                                'submitted_anomalies')
            # update step_timestamp
            step_timestamp = timestamp


################################################################################
#                                DRIVER CODE
################################################################################
if __name__ == '__main__':
    main()
