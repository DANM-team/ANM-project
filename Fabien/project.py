import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ANOMALY DETECTION

# ESB data, adding exponential moving average
esb = pd.read_csv('training_data/2020_04_11/esb/esb.csv')
esb['EMA'] = esb['avg_time'].ewm(alpha=0.8, adjust=False).mean()
esb['time'] = pd.to_datetime(esb['startTime'], unit='ms')

# Observing correspondency of the anomalous running times of traces with esb anomalies
trace_osb = pd.read_csv('training_data/2020_04_11/trace/trace_osb.csv')
trace_osb.sort_values(by=['startTime'])
trace_osb['time'] = pd.to_datetime(trace_osb['startTime'], unit='ms')

# Plot ESB average time, its exponential moving average and running time of traces
# ax = esb.plot(x='time', y=['avg_time', 'EMA'])
# trace_osb.plot(secondary_y=True, ax=ax, x='time', y=['elapsedTime'])
# plt.show()

# Finding the anomalous timestamps based on the square difference of ESB and exponential moving average
anomaly_timestamps = pd.DataFrame(columns = ['start', 'end'])
anomaly = False
for index, row in esb.iterrows():
    if (esb['EMA'].iloc[index] - row['avg_time'])**2 > 0.01:
        if not anomaly:
            anomaly = True
            anomaly_timestamps.loc[len(anomaly_timestamps)] = [row['startTime'], row['startTime']]
    else:
        if anomaly == True:
            anomaly_timestamps.loc[len(anomaly_timestamps)-1, 'end'] = row['startTime']
            anomaly = False

# Sometimes, data points are considered normal inside the anomaly, we put them back into it
indexes_to_drop = []
for index, row in anomaly_timestamps.iterrows():
    if index < len(anomaly_timestamps)-1:
        if  anomaly_timestamps['start'].iloc[index+1] - row['end'] <= 300000 :
            row['end'] = anomaly_timestamps['end'].iloc[index+1]
            indexes_to_drop.append(index+1)
anomaly_timestamps.drop(anomaly_timestamps.index[indexes_to_drop], inplace=True)
anomaly_timestamps = anomaly_timestamps.reset_index(drop=True)

# Showing the datetimes of the anomalies
anomaly_dates = anomaly_timestamps.copy()
anomaly_dates.start = pd.to_datetime(anomaly_timestamps['start'], unit='ms')
anomaly_dates.end = pd.to_datetime(anomaly_timestamps['end'], unit='ms')
print("Datetimes of anomalies")
print(anomaly_timestamps.shape[0], anomaly_dates, "\n")

# ANOMALY LOCATION

# Writing a spans CSV file concatening spans from all call types
# trace_csf = pd.read_csv('training_data/2020_04_11/trace/trace_csf.csv')
# trace_fly_remote = pd.read_csv('training_data/2020_04_11/trace/trace_fly_remote.csv')
# trace_jdbc = pd.read_csv('training_data/2020_04_11/trace/trace_jdbc.csv')
# trace_local = pd.read_csv('training_data/2020_04_11/trace/trace_local.csv')
# trace_remote_process = pd.read_csv('training_data/2020_04_11/trace/trace_remote_process.csv')

# spans = pd.concat([trace_csf, trace_fly_remote, trace_jdbc, trace_local, trace_osb, trace_remote_process])
# spans['time'] = pd.to_datetime(spans['startTime'], unit='ms')
# spans = spans.sort_values(by=['traceId'])
# spans.to_csv('spans.csv')

spans = pd.read_csv('spans.csv', index_col=0)

# # To check the relations between call types: which calls which
# call_type_chain = pd.DataFrame(0, columns = ['CSF', 'FlyRemote', 'JDBC', 'LOCAL', 'OSB', 'RemoteProcess'], index = ['CSF', 'FlyRemote', 'JDBC', 'LOCAL', 'OSB', 'RemoteProcess'])
# # To check where there is a change of host
# parent_host = pd.DataFrame(0, columns = ['CSF', 'FlyRemote', 'JDBC', 'LOCAL', 'OSB', 'RemoteProcess'], index = ['same', 'different'])
# # This matrix contains all the elapsed times of the calls sorted according to the call type of the span
# elapsed_time_calls = pd.DataFrame('', columns = ['docker_001', 'docker_002', 'docker_003', 'docker_004', 'docker_005', 'docker_006', 'docker_007', 'docker_008', 'os_021', 'os_022'], index = ['docker_001', 'docker_002', 'docker_003', 'docker_004', 'docker_005', 'docker_006', 'docker_007', 'docker_008', 'db_003', 'db_007', 'db_009', 'csf', 'osb', 'fly_remote'])
# elapsed_time_calls = elapsed_time_calls.applymap(list)

# # Ensure that all spans have one unique parent, excepting osb spans
# no_pid_match = 0
# pid_match = 0
# many_pid_match = 0

# # We choose the traces on which we want to build the matrix, here it is from 4h05 to 4h35 Beijing time, without anomalies
# for index, trace in trace_osb.loc[(trace_osb['startTime'] > 1586549100000) & (trace_osb['startTime'] < 1586550900000)].iterrows():
#     # All the spans in the trace, sorted by time
#     local_trace = spans.loc[spans['traceId'] == trace['traceId']].sort_values(by=['startTime'])

#     # For each span
#     for ind, span in local_trace.iterrows():
#         # If it has a parent
#         if span['pid'] != 'None':
#             # We retrieve the span elapsed time in its parent's
#             parent_span = local_trace.loc[local_trace['id'] == span['pid']]
            
#             local_trace.loc[local_trace['id'] == span['pid'], 'elapsedTime'] -= span['elapsedTime']

#             # Investigating on spans call-chain architecture
#             if parent_span.empty:
#                 no_pid_match += 1

#             elif len(parent_span.index) == 1:
#                 pid_match += 1
#                 call_type_chain.loc[[str(span['callType'])], [str(parent_span['callType'].iloc[0])]] += 1

#                 if span['cmdb_id'] == parent_span['cmdb_id'].iloc[0]:
#                     parent_host.loc[['same'], [str(span['callType'])]] += 1
#                 else:
#                     parent_host.loc[['different'], [str(span['callType'])]] += 1

#             else:
#                 many_pid_match += 1

#     # Placing the elapsed time into the corresponding cell  
#     for ind, span in local_trace.iterrows():
#         parent_span = local_trace.loc[local_trace['id'] == span['pid']]

#         if span['callType'] == 'OSB':
#             elapsed_time_calls.at['osb', str(span['cmdb_id'])].append(span['elapsedTime'])
        
#         elif span['callType'] == 'FlyRemote':
#             elapsed_time_calls.at['fly_remote', str(span['cmdb_id'])].append(span['elapsedTime'])
        
#         elif span['callType'] == 'CSF':
#             elapsed_time_calls.at['csf', str(span['cmdb_id'])].append(span['elapsedTime'])

#         elif span['callType'] == 'LOCAL':
#             elapsed_time_calls.at[str(span['cmdb_id']), str(span['cmdb_id'])].append(span['elapsedTime'])
        
#         elif span['callType'] == 'JDBC':
#             elapsed_time_calls.at[str(span['dsName']), str(span['cmdb_id'])].append(span['elapsedTime'])
        
#         elif span['callType'] == 'RemoteProcess':
#             elapsed_time_calls.at[ str(span['cmdb_id']), str(parent_span['cmdb_id'].iloc[0])].append(span['elapsedTime'])
# print(elapsed_time_calls)

# # Function returning the percentile of a list
# def give_percentiles(x, p):
#     if len(x) == 0:
#         return 0
#     else:
#         return np.percentile(np.array(x), p)

# # Storing matrices containing the percentiles of the spans call types, for different percentiles
# elapsed_time_calls_999 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 99.9))
# elapsed_time_calls_995 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 99.5))
# elapsed_time_calls_990 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 99))
# elapsed_time_calls_950 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 95))
# elapsed_time_calls_900 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 90))
# elapsed_time_calls_010 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 1))
# elapsed_time_calls_001 = elapsed_time_calls.applymap(lambda x: give_percentiles(x, 0.1))

# elapsed_time_calls_999.to_csv('elapsed_time_calls_999.csv')
# elapsed_time_calls_995.to_csv('elapsed_time_calls_995.csv')
# elapsed_time_calls_990.to_csv('elapsed_time_calls_990.csv')
# elapsed_time_calls_950.to_csv('elapsed_time_calls_950.csv')
# elapsed_time_calls_900.to_csv('elapsed_time_calls_900.csv')
# elapsed_time_calls_010.to_csv('elapsed_time_calls_010.csv')
# elapsed_time_calls_001.to_csv('elapsed_time_calls_001.csv')

# # Printing interesting informations
# print(no_pid_match, many_pid_match, pid_match, '\n')
# print(call_type_chain, '\n')
# print(parent_host, '\n')
# print(elapsed_time_calls_999, '\n')

# Loading the percentile matrix
elapsed_time_calls = pd.read_csv('elapsed_time_calls_999.csv', index_col=0)

for index, anomaly in anomaly_timestamps.iterrows():
    print(anomaly_dates['start'][index], '\n')

    # Counting matrix of all anomalous spans
    anomalous_calls = pd.DataFrame(0, columns = ['docker_001', 'docker_002', 'docker_003', 'docker_004', 'docker_005', 'docker_006', 'docker_007', 'docker_008', 'os_021', 'os_022'], index = ['docker_001', 'docker_002', 'docker_003', 'docker_004', 'docker_005', 'docker_006', 'docker_007', 'docker_008', 'db_003', 'db_007', 'db_009', 'csf', 'osb', 'fly_remote'])

    for index, trace in trace_osb.loc[(trace_osb.startTime > (anomaly['start'])) & (trace_osb.startTime < anomaly['start'] + 60000), ['traceId']].iterrows():
        
        local_trace = spans.loc[spans['traceId'] == trace['traceId']].sort_values(by=['startTime'])

        # Preprocessing
        for ind, span in local_trace.iterrows():
            if span['pid'] != 'None':
                parent_span = local_trace.loc[local_trace['id'] == span['pid']]
                
                local_trace.loc[local_trace['id'] == span['pid'], 'elapsedTime'] -= span['elapsedTime']
        
        # Attribution to the corresponding row and column if higher than the threshold
        for ind, span in local_trace.iterrows():
            parent_span = local_trace.loc[local_trace['id'] == span['pid']]

            if span['callType'] == 'OSB':
                if span['elapsedTime'] > elapsed_time_calls.at['osb', str(span['cmdb_id'])]:
                    anomalous_calls.at['osb', str(span['cmdb_id'])] += 1
            
            elif span['callType'] == 'FlyRemote':
                if span['elapsedTime'] > elapsed_time_calls.at['fly_remote', str(span['cmdb_id'])]:
                    anomalous_calls.at['fly_remote', str(span['cmdb_id'])] += 1
            
            elif span['callType'] == 'CSF':
                if span['elapsedTime'] > elapsed_time_calls.at['csf', str(span['cmdb_id'])]:
                    anomalous_calls.at['csf', str(span['cmdb_id'])] += 1

            elif span['callType'] == 'LOCAL':
                if span['elapsedTime'] > elapsed_time_calls.at[str(span['cmdb_id']), str(span['cmdb_id'])]:
                    anomalous_calls.at[str(span['cmdb_id']), str(span['cmdb_id'])] += 1
            
            elif span['callType'] == 'JDBC':
                if span['elapsedTime'] > elapsed_time_calls.at[str(span['dsName']), str(span['cmdb_id'])]:
                    anomalous_calls.at[str(span['dsName']), str(span['cmdb_id'])] += 1
            
            elif span['callType'] == 'RemoteProcess':
                if span['elapsedTime'] > elapsed_time_calls.at[ str(span['cmdb_id']), str(parent_span['cmdb_id'].iloc[0])]:
                    anomalous_calls.at[ str(span['cmdb_id']), str(parent_span['cmdb_id'].iloc[0])] += 1

    print(anomalous_calls)

    # We sort the dataframe to get the maximum values
    flat = anomalous_calls.to_numpy().flatten()
    flat.sort()
    max_row = anomalous_calls.where(anomalous_calls == flat[0]).dropna().index[0]
    max_column = anomalous_calls.where(anomalous_calls == flat[0]).dropna().columns[0]
    second_row = anomalous_calls.where(anomalous_calls == flat[1]).dropna().index[0]
    second_column = anomalous_calls.where(anomalous_calls == flat[1]).dropna().columns[0]

    print(flat, anomalous_calls.where(anomalous_calls == flat[0]).dropna())

    # Docker host
    if(max_column[0] == 'd'):
        # Container CPU Used
        if(max_row == max_column):
            print([[max_column, 'container_cpu_used']])

        # Network problem
        elif(second_column[0] == 'd' and second_row == max_row):
            print([[second_row, None]])
        
    






