import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

esb1 = pd.read_csv('training_data/2020_04_11/esb/esb.csv')
esb2 = pd.read_csv('training_data/2020_04_20/esb/esb.csv')
esb3 = pd.read_csv('training_data/2020_04_21/esb/esb.csv')
esb4 = pd.read_csv('training_data/2020_04_22/esb/esb.csv')
esb5 = pd.read_csv('training_data/2020_04_23/esb/esb.csv')
esb6 = pd.read_csv('training_data/2020_05_04/esb/esb.csv')
esb7 = pd.read_csv('training_data/2020_05_22/esb/esb.csv')
esb8 = pd.read_csv('training_data/2020_05_23/esb/esb.csv')
esb9 = pd.read_csv('training_data/2020_05_24/esb/esb.csv')
esb10 = pd.read_csv('training_data/2020_05_25/esb/esb.csv')
esb11 = pd.read_csv('training_data/2020_05_26/esb/esb.csv')
esb12 = pd.read_csv('training_data/2020_05_27/esb/esb.csv')
esb13 = pd.read_csv('training_data/2020_05_28/esb/esb.csv')
esb14 = pd.read_csv('training_data/2020_05_29/esb/esb.csv')
esb15 = pd.read_csv('training_data/2020_05_30/esb/esb.csv')
esb16 = pd.read_csv('training_data/2020_05_31/esb/esb.csv')
#esb.startTime = pd.to_datetime(esb['startTime'], unit='ms')

esb_list = []
esb_list.append([esb1, esb2, esb3, esb4, esb5, esb6, esb7, esb8, esb9, esb10, esb11, esb12, esb13, esb14, esb15, esb16])

print(esb_list)
for esb in esb_list[0]:
    esb['EMA'] = esb['avg_time'].ewm(alpha=0.8, adjust=False).mean()

# esb1.plot(x='startTime', y=['avg_time', 'EMA'])
# plt.show()

    anomaly_timestamps = pd.DataFrame(columns = ['start', 'end'])
    anomaly = False
    for index, row in esb.iterrows():
        #print(row['avg_time'], esb['EMA_0.1'].iloc[index], (esb['EMA_0.1'].iloc[index] - row['avg_time'])**2)
        if (esb['EMA'].iloc[index] - row['avg_time'])**2 > 0.01:
            if not anomaly:
                anomaly = True
                anomaly_timestamps.loc[len(anomaly_timestamps)] = [row['startTime'], row['startTime']]
        else:
            if anomaly == True:
                anomaly_timestamps.loc[len(anomaly_timestamps)-1, 'end'] = row['startTime']
                anomaly = False

    indexes_to_drop = []
    for index, row in anomaly_timestamps.iterrows():
        if index < len(anomaly_timestamps)-1:
            if  anomaly_timestamps['start'].iloc[index+1] - row['end'] <= 300000 :
                row['end'] = anomaly_timestamps['end'].iloc[index+1]
                indexes_to_drop.append(index+1)

    anomaly_timestamps.drop(anomaly_timestamps.index[indexes_to_drop], inplace=True)
    anomaly_timestamps = anomaly_timestamps.reset_index(drop=True)

    anomaly_dates = anomaly_timestamps.copy()
    anomaly_dates.start = pd.to_datetime(anomaly_timestamps['start'], unit='ms')
    anomaly_dates.end = pd.to_datetime(anomaly_timestamps['end'], unit='ms')
    print(anomaly_timestamps.shape[0], anomaly_dates, "\n")

# db_oracle_11g = pd.read_csv('training_data/2020_04_11/host/db_oracle_11g.csv')
# dcos_container = pd.read_csv('training_data/2020_04_11/host/dcos_container.csv')
# dcos_docker = pd.read_csv('training_data/2020_04_11/host/dcos_docker.csv')
# mw_redis = pd.read_csv('training_data/2020_04_11/host/mw_redis.csv')
# os_linux = pd.read_csv('training_data/2020_04_11/host/os_linux.csv')

# dcos_docker.timestamp = pd.to_datetime(dcos_docker['timestamp'], unit='ms')
# dcos_docker = dcos_docker.loc[dcos_docker['cmdb_id'].isin(['docker_002'])]
# dcos_docker.loc[dcos_docker['name'].isin(['container_cpu_used'])].sort_values(by='timestamp').plot(x='timestamp', y='value')
# plt.show()

# trace_csf = pd.read_csv('training_data/2020_04_11/trace/trace_csf.csv')
# trace_fly_remote = pd.read_csv('training_data/2020_04_11/trace/trace_fly_remote.csv')
# trace_jdbc = pd.read_csv('training_data/2020_04_11/trace/trace_jdbc.csv')
# trace_local = pd.read_csv('training_data/2020_04_11/trace/trace_local.csv')
# trace_osb = pd.read_csv('training_data/2020_04_11/trace/trace_osb.csv')
# trace_remote_process = pd.read_csv('training_data/2020_04_11/trace/trace_remote_process.csv')



# for anomaly in anomaly_timestamps:
#     local_anomaly_traces = trace_osb.loc[(trace_osb.startTime > (anomaly[0])) & (trace_osb.startTime < anomaly[1]), ['traceId']]
#     local_anomaly_spans = pd.concat([trace_csf, trace_fly_remote, trace_jdbc, trace_local, trace_osb, trace_remote_process])
#     local_anomaly_spans = local_anomaly_spans.loc[local_anomaly_spans['traceId'].isin(local_anomaly_traces['traceId']), ['startTime', 'traceId', 'id', 'pid', 'elapsedTime', 'success', 'cmdb_id']].sort_values(by=['traceId'])

#     hosts = {}
#     for index, trace in local_anomaly_traces.iterrows():
#         local_trace = local_anomaly_spans.loc[local_anomaly_spans['traceId'] == trace['traceId']].sort_values(by=['startTime'])
#         spans = {}
#         for index, span in local_trace.iterrows():
#             spans[str(span['id'])] = [span['elapsedTime'], int(span['success'] == 'True'), span['cmdb_id']]
#             if (span['pid'] in spans.keys()):
#                 spans[str(span['pid'])][0] -= span['elapsedTime']

#         for span, value in spans.items():
#             if (value[2] not in hosts.keys()):
#                 hosts[str(value[2])] = [value[0], value[1]]
#             else:
#                 hosts[str(value[2])][0] = (hosts[str(value[2])][0]+value[0])/2
#                 hosts[str(value[2])][1] += value[1]
    
#     print(len(hosts), hosts, "\n")

#     max = 0
#     anomalous_cmdb_id = ""
#     for cmdb_id, value in hosts.items():
#         if value[0] > max:
#             max = value[0]
#             anomalous_cmdb_id = cmdb_id
#     print(dt.datetime.fromtimestamp(anomaly[0]/1000.0), anomalous_cmdb_id, "\n", "\n")

#traces = pd.concat([trace_csf, trace_fly_remote, trace_jdbc, trace_local, trace_osb, trace_remote_process])
#traces.startTime = pd.to_datetime(traces['startTime'], unit='ms')
#traces = traces.sort_values(by=['startTime'])
#print(traces)

#traces.loc[(traces.startTime > (anomaly_timestamps[0][0] - 100000)) & (traces.startTime < anomaly_timestamps[0][1] + 100000), ['startTime', 'elapsedTime']].plot(x='startTime', y='elapsedTime')
#traces.plot(x='startTime', y=['elapsedTime'])
#plt.show()





