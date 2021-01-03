'''
Example for data consuming.
'''
import requests
import json


import pandas as pd
import numpy as np

import datetime as dt
import time

from kafka import KafkaConsumer


# Three topics are available: platform-index, business-index, trace.
# Subscribe at least one of them.
AVAILABLE_TOPICS = set(['platform-index', 'business-index', 'trace'])
CONSUMER = KafkaConsumer('platform-index', 'business-index', 'trace',
                         bootstrap_servers=['172.21.0.8', ],
                         auto_offset_reset='latest',
                         enable_auto_commit=False,
                         security_protocol='PLAINTEXT')


class PlatformIndex():  # pylint: disable=too-few-public-methods
    '''Structure for platform indices'''

    __slots__ = ['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id']

    def __init__(self, data):
        self.item_id = data['itemid']
        self.name = data['name']
        self.bomc_id = data['bomc_id']
        self.timestamp = data['timestamp']
        self.value = data['value']
        self.cmdb_id = data['cmdb_id']


class BusinessIndex():  # pylint: disable=too-few-public-methods
    '''Structure for business indices'''

    __slots__ = ['service_name', 'start_time', 'avg_time', 'num',
                 'succee_num', 'succee_rate']

    def __init__(self, data):
        self.service_name = data['serviceName']
        self.start_time = data['startTime']
        self.avg_time = data['avg_time']
        self.num = data['num']
        self.succee_num = data['succee_num']
        self.succee_rate = data['succee_rate']


class Trace():  # pylint: disable=invalid-name,too-many-instance-attributes,too-few-public-methods
    '''Structure for traces'''

    __slots__ = ['call_type', 'start_time', 'elapsed_time', 'success',
                 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name']

    def __init__(self, data):
        self.call_type = data['callType']
        self.start_time = data['startTime']
        self.elapsed_time = data['elapsedTime']
        self.success = data['success']
        self.trace_id = data['traceId']
        self.id = data['id']
        self.pid = data['pid']
        self.cmdb_id = data['cmdb_id']

        if 'serviceName' in data:
            # For data['callType']
            #  in ['CSF', 'OSB', 'RemoteProcess', 'FlyRemote', 'LOCAL']
            self.service_name = data['serviceName']
        if 'dsName' in data:
            # For data['callType'] in ['JDBC', 'LOCAL']
            self.ds_name = data['dsName']


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
    r = requests.post('http://172.21.0.8:8000/standings/submit/', data=json.dumps(data))


def main():
    '''Consume data and react'''
    # Check authorities
    assert AVAILABLE_TOPICS <= CONSUMER.topics(), 'Please contact admin'

    esb = pd.DataFrame(columns=['service_name', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate'])
    trace = pd.DataFrame(columns=['call_type', 'startTime', 'elapsed_time', 'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])
    host = pd.DataFrame(columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])

    i = 0
    step_timestamp = int(dt.datetime.now().timestamp()*1000)
    flush_timestamp = int(dt.datetime.now().timestamp()*1000)

    anomaly_last_step = False
    for message in CONSUMER:
        data = json.loads(message.value.decode('utf8'))
        timestamp = data['timestamp'] if message.topic == 'platform-index' else data['startTime']

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
            host = pd.concat([host, temp_df])

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
            esb = pd.concat([esb, temp_df])

        else:  # message.topic == 'trace'
            temp_df = pd.DataFrame({
                'call_type': data['callType'],
                'startTime': data['startTime'],
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
            trace = pd.concat([trace, temp_df])

        del temp_df

        if ((timestamp - step_timestamp) > 5*60*1000):
            print("detecting anomaly", timestamp)
            step_timestamp = timestamp
            esb['EMA'] = esb['avg_time'].ewm(alpha=0.8, adjust=False).mean()
            for index, row in esb.iterrows():
                # if anomaly
                if (esb['EMA'].iloc[index] - row['avg_time'])**2 > 0.01:
                    # if there was already an anomaly detected last step we consider it is the same anomaly
                    if anomaly_last_step:
                        anomaly_last_step = False
                    else:
                        anomaly_last_step = True
                        local_anomaly_traces = trace.loc[trace['call_type']=="OSB" and trace.startTime > timestamp, ['trace_id']]
                        local_anomaly_spans = trace.loc[trace['trace_id'].isin(local_anomaly_traces['trace_id']), ['start_time', 'trace_id', 'id', 'pid', 'elapsed_time', 'success', 'cmdb_id']].sort_values(by=['traceId'])

                        hosts = {}
                        for index, trace in local_anomaly_traces.iterrows():
                            local_trace = local_anomaly_spans.loc[local_anomaly_spans['trace_id'] == trace['trace_id']].sort_values(by=['start_time'])
                            spans = {}
                            for index, span in local_trace.iterrows():
                                spans[str(span['id'])] = [span['elapsed_time'], int(span['success'] == 'True'), span['cmdb_id']]
                                if (span['pid'] in spans.keys()):
                                    spans[str(span['pid'])][0] -= span['elapsed_time']


                            for span, value in spans.items():
                                if (value[2] not in hosts.keys()):
                                    hosts[str(value[2])] = [value[0], value[1]]
                                else:
                                    hosts[str(value[2])][0] = (hosts[str(value[2])][0]+value[0])/2
                                    hosts[str(value[2])][1] += value[1]
                        print(len(hosts), hosts, "\n")

                        max = 0
                        anomalous_cmdb_id = ""
                        for cmdb_id, value in hosts.items():
                            if value[0] > max:
                                max = value[0]
                                anomalous_cmdb_id = cmdb_id

                        submit([[anomalous_cmdb_id, None]])
                        print(dt.datetime.fromtimestamp(timestamp/1000.0), anomalous_cmdb_id, "\n")

        if((timestamp - flush_timestamp) > 180*60*1000):
            print("flush", timestamp)
            flush_timestamp = timestamp
            esb = pd.DataFrame(columns=['service_name', 'start_time', 'startTime', 'avg_time', 'num', 'succee_num', 'succee_rate', 'time'])
            trace = pd.DataFrame(columns=['call_type', 'start_time', 'elapsed_time', 'success', 'trace_id', 'id', 'pid', 'cmdb_id', 'service_name', 'ds_name'])
            host = pd.DataFrame(columns=['item_id', 'name', 'bomc_id', 'timestamp', 'value', 'cmdb_id'])

if __name__ == '__main__':
    main()