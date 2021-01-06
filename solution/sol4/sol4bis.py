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
        
        # thresholds
        print("Loading all time statistics")
        self.all_time_statistics = pd.read_csv("alltime_statistics.csv", index_col="node_name")

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
            temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'], unit='ms')
            # self.host_df = pd.concat([self.host_df, temp_df])
            self.host_df = self.host_df.append(temp_df, ignore_index=True)

        elif message.topic == 'business-index':
            temp = []
            for key in data['body']:
                for item in data['body'][key]:
                    temp.append({
                        'service_name': item['serviceName'],
                        'start_time': item['startTime'],
                        'avg_time': item['avg_time'],
                        'num': item['num'],
                        'succee_num': item['succee_num'],
                        'succee_rate': item['succee_rate']
                    })
            temp_df = pd.DataFrame(temp)
            temp_df['start_time'] = pd.to_datetime(temp_df['start_time'], unit='ms')
            # self.esb_df = pd.concat([self.esb_df, temp_df])
            self.esb_df = self.esb_df.append(temp_df, ignore_index=True)

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
            temp_df['start_time'] = pd.to_datetime(temp_df['start_time'], unit='ms')
            # self.trace_df = pd.concat([self.trace_df, temp_df])
            self.trace_df = self.trace_df.append(temp_df, ignore_index=True)

        del temp_df

    def flushCache(self):
        # delete accumulated data (empty dataframes)
        self.esb_df = pd.DataFrame(columns=[
                                   'service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
        self.host_df = pd.DataFrame(
            columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])
        self.trace_df = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time',
                                              'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])
    
    def deleteOldCacheData(self):
        # == only keep 20min of records all the time (except when starting of course) ==
        cache_saving_time_window = datetime.timedelta(minutes=20)
        
        # esb
        esb_condition = self.esb_df['start_time'] >= (self.esb_df['start_time'].max() - cache_saving_time_window)
        self.esb_df = self.esb_df[esb_condition]

        # host
        host_condition = self.host_df['timestamp'] >= (self.host_df['timestamp'].max() - cache_saving_time_window)
        self.host_df = self.host_df[host_condition]

        # trace
        trace_condition = self.trace_df['start_time'] >= (self.trace_df['start_time'].max() - cache_saving_time_window)
        self.trace_df = self.trace_df[trace_condition]


    def detectESB(self):
        # check if esb_df is not empty
        if self.esb_df.shape[0] > 0:
            # if succee_rate drops below 1, there is an anomaly
            failed_esb_res = self.esb_df[self.esb_df['succee_rate']< 1.0]['start_time']
            if not failed_esb_res.empty:
                return failed_esb_res.tolist()
            try:
                # calculate moving average
                self.esb_df['EMA'] = self.esb_df['avg_time'].ewm(alpha=0.8, adjust=False).mean()

                # make the test over EMA
                test = (self.esb_df['EMA'] - self.esb_df['avg_time'])**2 > 0.01
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
            nodes = slowest_traces[slowest_traces['trace_id']
                                   == tid]['cmdb_id']
            common_nodes = common_nodes[np.isin(
                element=common_nodes, test_elements=nodes)]

        # if the previous failed (ie there is no exact common nodes), then check for the most common nodes (majority element only)
        if len(common_nodes) <= 0:
            # alternative to common nodes: most common nodes
            #       threshold = how to quantify "most common". We need to determine some way of saying "this is common" and "this is not"
            #       currently, threshold = every nodes which represent more than 30% of the total
            threshold = 0.30
            frequencies = slowest_traces['cmdb_id'].value_counts(normalize=True).sort_values(
                ascending=False)  # count values and sort by descending occurence (most frequent first)
            if not frequencies.empty:
                common_nodes = frequencies[frequencies >
                                           threshold].index.tolist()
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
            # remove flat plots (because they obviously are not the anomaly since it doesn't change)
            h = h[h.groupby('name')['value'].transform('std') > 0]
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
            merged_df['avg_time'] = merged_df['avg_time'].fillna(
                method='ffill')

            # compute correlation with esb['avg_time'] of each kpi
            abs_correlations_sorted = merged_df.groupby(
                'name').corrwith(merged_df['avg_time']).abs()

            if abs_correlations_sorted.empty:
                kpi = None
            else:
                abs_correlations_sorted = abs_correlations_sorted.reset_index().sort_values(
                    'value', ascending=False)   # sort by correlation value
                kpi = abs_correlations_sorted.iloc[0]['name']

            # append result to res
            res.append([_node, kpi])

            # log results
            print("I found that the kpi most susceptible of being the cause of the anomaly of node {} is {}".format(
                res[-1][0],
                res[-1][1]))
        return res
    
    def checkDB_On_Off_State(self):
        print("Checking db On_Off_State kpi ...")
        res = []
        if self.host_df.shape[0] > 0:
            # print(f"most recent timestamp of host {self.host_df['timestamp'].max()} ({self.host_df['timestamp'].max().timestamp()})")
            # print(f"step timestamp {datetime.datetime.fromtimestamp(step_timestamp/1e3)} ({step_timestamp})")
            # tmsp = datetime.datetime.fromtimestamp(step_timestamp/1e3) - datetime.timedelta(hours=8) # because of gmt+8
            tmsp = self.host_df['timestamp'].max() - datetime.timedelta(minutes=1)
            host = self.host_df[self.host_df['timestamp'] >= tmsp]

            # test on db hosts to check for "db close" error
            anomalous_on_off_state_idx = np.where(host.loc[host['name'] == 'On_Off_State', 'value'] < 1)
            anomalous_db = host.iloc[anomalous_on_off_state_idx]['cmdb_id'].unique()
            for db in anomalous_db:
                res.append([db, "On_Off_State"])
                res.append([db, "tnsping_result_time"])
        print(f"\t... found {len(res)} anomalies")
        return res
    
    def check_Sent_queue_Received_queue(self):
        print("Checking os Sent_queue & Received_queue kpi ...")
        res = []
        if self.host_df.shape[0] > 0:
            tmsp = self.host_df['timestamp'].max() - datetime.timedelta(minutes=1)
            host = self.host_df[self.host_df['timestamp'] >= tmsp]

            threshold = 100
            anomalous_os_received_idx = np.where(host.loc[host['name'] == 'Received_queue', 'value'] > threshold)[0]
            anomalous_os_sent_idx = np.where(host.loc[host['name'] == 'Sent_queue', 'value'] > threshold)[0]
            if len(anomalous_os_received_idx) > 1:
                for node in host.loc[host['name'] == 'Received_queue', 'cmdb_id'].iloc[anomalous_os_received_idx]:
                    res.append([node, "Received_queue"])
            if len(anomalous_os_sent_idx) > 1:
                for node in host.loc[host['name'] == 'Sent_queue', 'cmdb_id'].iloc[anomalous_os_sent_idx]:
                    res.append([node, "Sent_queue"])
        print(f"\t... found {len(res)} anomalies")
        return res
    
    def checkHosts(self):
        print("Checking nodes elapsed_time ...")
        res = []
        if self.trace_df.shape[0] > 0:
            # tmsp = datetime.datetime.fromtimestamp(step_timestamp/1e3) - datetime.timedelta(hours=8) # because of gmt+8
            tmsp = self.trace_df['start_time'].max() - datetime.timedelta(minutes=1)
            # print(f"most recent timestamp of trace_df {self.trace_df['start_time'].max()} ({self.trace_df['start_time'].max().timestamp()})")
            # print(f"tmsp = {tmsp}")
            # print(f"step timestamp {datetime.datetime.fromtimestamp(step_timestamp/1e3)} ({step_timestamp})")

            long_time_traces = self.trace_df.copy()
            traces = self.trace_df[self.trace_df['start_time'] >= tmsp]
            # print(f"host check with traces of shape {traces.shape} (trace_df is of shape {self.trace_df.shape})")
            if 'node_name' not in traces.columns:
                traces.loc[:, 'node_name'] = traces['cmdb_id'] # CSF, FlyRemote, OSB, RemoteProcess
                # traces.loc[traces['call_type'].isin(['LOCAL', 'JDBC']), 'node_name'] = traces.loc[traces['call_type'].isin(['LOCAL', 'JDBC']), 'ds_name']
                condition = traces['ds_name'].fillna('').str.contains('db')
                traces.loc[condition, 'node_name'] = traces.loc[condition, 'ds_name']

            if 'node_name' not in long_time_traces.columns:
                long_time_traces.loc[:, 'node_name'] = long_time_traces['cmdb_id'] # CSF, FlyRemote, OSB, RemoteProcess
                # long_time_traces.loc[long_time_traces['call_type'].isin(['LOCAL', 'JDBC']), 'node_name'] = long_time_traces.loc[long_time_traces['call_type'].isin(['LOCAL', 'JDBC']), 'ds_name']
                condition = long_time_traces['ds_name'].fillna('').str.contains('db')
                long_time_traces.loc[condition, 'node_name'] = long_time_traces.loc[condition, 'ds_name']

            long_time_traces = long_time_traces[['node_name', 'elapsed_time']]
            traces = traces[['node_name', 'elapsed_time']]

            for node in list(traces['node_name'].dropna().unique()):
                threshold = self.all_time_statistics.loc[node, 'quantile99_99'] if node in self.all_time_statistics.index else np.inf
                print(f"Examining {node} with threshold {threshold}")
                ts = traces.loc[traces['node_name'] == node]
                long_time_ts = long_time_traces.loc[long_time_traces['node_name'] == node]
                # zscore = (ts - long_time_ts.mean()) / long_time_ts.std()

                if 'os' in node:
                    # threshold = 2
                    # anomalous_idx = np.where(zscore > threshold)[0]
                    anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                    if len(anomalous_idx) > 3:
                        res.append([node, "Sent_queue"])
                        res.append([node, "Received_queue"])
                
                elif 'docker' in node:
                    if node in ['docker_004']:
                        # threshold = 5
                        # anomalous_idx = np.where(zscore > threshold)[0]
                        anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                        if len(anomalous_idx) > 3:
                            res.append([node, "container_cpu_used"])

                    elif node in ['docker_005']:
                        # threshold = 6
                        # anomalous_idx = np.where(zscore > threshold)[0]
                        anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                        if len(anomalous_idx) > 3:
                            res.append([node, None])
                        
                    elif node in ['docker_007']:
                        # threshold = 5
                        # anomalous_idx = np.where(zscore > threshold)[0]
                        anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                        if len(anomalous_idx) > 3:
                            res.append([node, None])

                    elif node in ['docker_006']:
                        # threshold = 3
                        # anomalous_idx = np.where(zscore > threshold)[0]
                        anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                        if len(anomalous_idx) > 3:
                            res.append([node, "container_cpu_used"])
                    
                    else:
                        # threshold = 3
                        # anomalous_idx = np.where(zscore > threshold)[0]
                        anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                        if len(anomalous_idx) > 3:
                            res.append([node, np.random.choice([None, "container_cpu_used"])])
                
                elif 'db' in node:
                    # threshold = 6
                    # anomalous_idx = np.where(zscore > threshold)[0]
                    anomalous_idx = np.where(ts['elapsed_time'] > threshold)[0]
                    if len(anomalous_idx) > 3:
                        # kpi = np.random.choice([["Proc_User_Used_Pct", "Proc_Used_Pct", "Sess_Connect"], ["On_Off_State", "tnsping_result_time"]])
                        kpi = ["Proc_User_Used_Pct", "Proc_Used_Pct", "Sess_Connect"]
                        for k in kpi:
                            res.append([node, k])

        print(f"\t... found {len(res)} anomalies")
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
                print(f"{i-1} {last_message} (+{msg_count}) {timestamp}")
            print(i, message.topic, timestamp)
            msg_count = 0
            last_message = message.topic  

        # # flush every 30min
        # if (timestamp - flush_timestamp >= 30*60*1000):
        #     print("FLUSHING CACHE")
        #     flush_timestamp = timestamp     # update step timestamp
        #     detector.flushCache()

        # append data to
        detector.appendData(message)

        # run anomaly detection every minute
        if ((timestamp - step_timestamp) > 1*60*1000):
            print(f"== Running Anomaly Detection ({timestamp}/{step_timestamp}) ==")
            res = []

            # check esb data for anomalies
            # tmsp = detector.detectESB()
            # if tmsp != None and len(tmsp) > 0:
            #     for _t in tmsp:
            #         res += detector.findRootCause(_t)
                        

            # check db status
            res += detector.checkDB_On_Off_State()

            # check sent queue & received queue
            res += detector.check_Sent_queue_Received_queue()
            
            # check hosts
            res += detector.checkHosts()

            # finally, submit found anomalies
            if len(res) > 0:
                # log submit
                print("Anomaly detected at the following timestamp {}".format(step_timestamp))
                print("/!\\ SUBMITTING: {}".format(res))
                for _res in res:
                    if str(step_timestamp) in submitted_anomalies.keys() and submitted_anomalies[str(step_timestamp)] != _res: # check if this anomaly has not been submitted already
                        submit([_res])
                        submitted_anomalies[str(step_timestamp)] = _res
                    # save into file to keep trace of submitted anomalies
                pd.DataFrame(submitted_anomalies).to_csv('submitted_anomalies.csv')
            
            
            
            print("FREEING CACHE FROM OLD DATA")
            detector.deleteOldCacheData()

            # update step_timestamp
            step_timestamp = timestamp


################################################################################
#                                DRIVER CODE
################################################################################
if __name__ == '__main__':
    main()