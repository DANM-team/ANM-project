import pandas as pd
import numpy as np

import datetime
import math
import gc

from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler, MinMaxScaler

class DataUtils:
    def __init__(self, verbose=False):
        self.verbose = verbose
        self.node_le = LabelEncoder()
        self.kpi_le = LabelEncoder()
        self.data_stats = DataStats()

    def reduce_mem_usage(self, df):
        """ Function to reduce the DF size """
        numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
        start_mem = df.memory_usage().sum() / 1024**2
        for col in df.columns:
            col_type = df[col].dtypes
            if col_type in numerics:
                c_min = df[col].min()
                c_max = df[col].max()
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        df[col] = df[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        df[col] = df[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        df[col] = df[col].astype(np.int32)
                    elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                        df[col] = df[col].astype(np.int64)
                else:
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        df[col] = df[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        df[col] = df[col].astype(np.float32)
                    else:
                        df[col] = df[col].astype(np.float64)
        end_mem = df.memory_usage().sum() / 1024**2
        if self.verbose:
            print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(
                end_mem, 100 * (start_mem - end_mem) / start_mem))
        return df
    
    def load_data(self, training_data_path, dates, nrows=None, dataset='all'):
        assert dataset in ['all', 'host', 'esb', 'trace']
        arr_host = []
        arr_esb = []
        arr_trace = [] 
        arr_failures = []

        for date in dates:
            if dataset in ['all', 'esb']:
                # ESB
                print("{:=^30}".format('  ESB  '))
                filename = training_data_path + "/{}/业务指标/esb.csv".format(date)
                print("opening {}".format(filename))
                esb_df = pd.read_csv(filename, header=0, nrows=nrows)

                esb_df.rename(columns={
                    'serviceName': 'service_name',
                    'startTime': 'start_time'},
                    inplace=True)

                esb_df['start_time'] = pd.to_datetime(esb_df['start_time'], unit='ms')
                esb_df['time'] = (esb_df['start_time'] - esb_df['start_time'].min()) / \
                    datetime.timedelta(seconds=1)  # should be 0 to 24h in seconds
                arr_esb.append(esb_df)

            if dataset in ['all', 'host']:
                # HOST
                print("{:=^30}".format('  host  '))
                host_df_lst = []
                for s_name in ["db_oracle_11g", "dcos_container", "dcos_docker", "mw_redis", "os_linux"]:
                    filename = training_data_path + "/" + date + "/平台指标/" + s_name + ".csv"
                    print("opening {}".format(filename))
                    temp_df = pd.read_csv(filename, header=0)
                    # rename columns so that is follows convention some_special_name
                    temp_df.rename(columns={'itemid': 'item_id'}, inplace=True)

                    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'], unit='ms')

                    # changing data type to save memory
                    temp_df['item_id'] = temp_df['item_id'].astype(int)
                    temp_df[['cmdb_id', 'bomc_id', 'name']] = temp_df[[
                        'cmdb_id', 'bomc_id', 'name']].astype('category')

                    host_df_lst.append(temp_df)
                    del temp_df

                host_df = pd.concat(host_df_lst)
                del host_df_lst

                arr_host.append(host_df)

            if dataset in ['all', 'trace']:
                # TRACE
                print("{:=^30}".format('   trace   '))
                trace_df_lst = []
                for c_type in ["csf", "fly_remote", "jdbc", "local", "osb", "remote_process"]:
                    filename = training_data_path + "/" + date + "/调用链指标/trace_" + c_type + ".csv"
                    print("opening {}".format(filename))
                    # chunks = pd.read_csv(filename, header=0, chunksize=100000)
                    # temp_df = pd.concat(chunks)
                    temp_df = pd.read_csv(filename, header=0, nrows=nrows)
                    temp_df.rename(
                        columns={
                            'callType': 'call_type',
                            'startTime': 'start_time',
                            'elapsedTime': 'elapsed_time',
                            'traceId': 'trace_id',
                            'serviceName': 'service_name',
                            'dsName': 'ds_name'},
                        inplace=True)  # rename columns so that is follows convention some_special_name except for Id
                    trace_df_lst.append(temp_df)
                    del temp_df

                trace_df = pd.concat(trace_df_lst)
                arr_trace.append(trace_df)

            # FAILURES
            print("{:=^30}".format('   failures   '))
            filename = "故障整理（预赛）.csv"
            print(f"opening {filename}")
            # read csv
            failures_df = pd.read_csv(filename, index_col='index')

            # interpret as datetime objects
            failures_df['start_time'] = pd.to_datetime(
                failures_df['start_time'], format='%Y/%m/%d %H:%M', infer_datetime_format=True)
            failures_df['log_time'] = pd.to_datetime(
                failures_df['log_time'], format='%Y/%m/%d %H:%M', infer_datetime_format=True)

            # load failures for the day date
            FAILURE_date = datetime.datetime.strptime(
                date, "%Y_%m_%d").date() - datetime.timedelta(days=1)
            failures_df = failures_df[failures_df['start_time'].dt.date == FAILURE_date]
            arr_failures.append(failures_df)
        

        if dataset == 'all':
            return pd.concat(arr_esb), pd.concat(arr_host), pd.concat(arr_trace), pd.concat(arr_failures)
        else: 
            dataset_arr_dict = {'esb': arr_esb, 'host': arr_host, 'trace': arr_trace}
            return pd.concat(dataset_arr_dict[dataset]), pd.concat(arr_failures)
    
    def add_timeseries_features(self, df, w_period, w_time_unit, w_length):
        """
        Convert Timeseries data to supervised dataset by shifting data.
        Params: 
            - df: host_df
            - w_period: int, window period ie. how frequent you want to sample data
            - w_time_unit: str, time unit for w_perdiod
            - w_length: int, window length ie. how far in the past we want to see
        
        Returns:
            - train_df: transformed df with new features and target_kpi/node/value
            - supervised_dataset: transformed with new features only (train_df.shape[0] == supervised_dataset.shape[0]*#of_kpis)
        
        Example:
            (df, 1, 'min', 5) you want your model to predict the next value based on the previous 5minutes with data sampling every minute (thus based on 4 values)
        
        Note: this code is designed for the global xgboost model
        """

        # Check parameters
        supported_time_unit = ['h', 'min', 's', 'ms']
        assert w_time_unit in supported_time_unit, "Only {} are supported for w_time_unit".format(supported_time_unit)

        # Gather data
        temp_df = df.loc[:, ['cmdb_id', 'name', 'timestamp', 'value']]
        temp_df['timestamp'] = temp_df['timestamp'].dt.round('30s')  # reduce precision of timestamp


        # Create features: all kpis ('name') become features
        temp_df = pd.pivot_table(temp_df, index=['timestamp', 'cmdb_id'], columns='name', values='value', dropna=True)
        temp_df.reset_index(['cmdb_id'], inplace=True)

        # Fill missing data using forward fill
        temp_df.fillna(method='ffill', inplace=True)

        # Drop rows with Nan values
        temp_df.dropna(axis=0, how='any',inplace=True)

        # Reduce mem usage
        temp_df = self.reduce_mem_usage(temp_df)
        
        # Shift data and create even more features (e.g. 'CPU_used (t-3min)')
        #  while creating supervised_dataset
        supervised_dataset = temp_df.reset_index()
        for i in range(1, w_length + w_period, w_period):
            s = temp_df.shift(periods=i, freq=w_time_unit)
            s.columns = ["{}(t-{}{:s})".format(_n, i, w_time_unit) for _n in s.columns]
            s.reset_index(inplace=True)

            supervised_dataset = pd.merge(supervised_dataset, s, left_on=['cmdb_id', 'timestamp'], right_on=['cmdb_id(t-{}{:s})'.format(i, w_time_unit), 'timestamp'])
            supervised_dataset.drop('cmdb_id(t-{}{:s})'.format(i, w_time_unit), axis=1, inplace=True)

        # Drop Nan and adding time features
        supervised_dataset.dropna(how='any', inplace=True)          
        supervised_dataset['hour'] = supervised_dataset['timestamp'].dt.hour
        supervised_dataset.drop('timestamp', axis=1, inplace=True)

        # Check missing data
        percent_missing = self.data_stats.missing_statistics(temp_df)
        # print(percent_missing)

        return supervised_dataset
    
    def to_multiindex(self, df, unique_kpi, w_period, w_time_unit, w_length):
        # pandas multi index
        time_idx = ["t"] + [f"t-{i}{w_time_unit}" for i in range(1, w_length + w_period, w_period)]
        idx = pd.MultiIndex.from_product([time_idx, unique_kpi], names=['time', 'kpi'])
        
        # values
        vals = df.loc[:, ~df.columns.isin(['hour', 'cmdb_id'])].values

        # transform to multi index
        multiindex_supervised_dataset = pd.DataFrame(vals, columns=idx)

        return multiindex_supervised_dataset
    
    def to_tensor(self, multi_df, w_length):
        # shape = (samples, timesteps, features)
        shape = (multi_df['t'].shape[0], w_length+1, multi_df['t'].shape[1])
        tensor = multi_df.values.reshape(shape)
        return tensor
    
    def transform_to_lstm_data(self, df, unique_kpi, w_period, w_time_unit, w_length, scaler=None):
        # supervised dataset
        supervised_dataset = self.add_timeseries_features(df, w_period, w_time_unit, w_length)
        supervised_dataset.drop('cmdb_id', axis=1, inplace=True)

        if scaler:
            scaled_supervised_dataset = pd.DataFrame(scaler.fit_transform(supervised_dataset.values), columns=supervised_dataset.columns)
            multi_supervised_dataset = self.to_multiindex(scaled_supervised_dataset, unique_kpi, w_period, w_time_unit, w_length)
        else :
            multi_supervised_dataset = self.to_multiindex(supervised_dataset, unique_kpi, w_period, w_time_unit, w_length)

        tensor = self.to_tensor(multi_supervised_dataset, w_length)
        return tensor
            
    def create_ts_files(self, df, history_length, step_size, lag_unit, target_step, data_folder, num_rows_per_file):
        """ like add_timeseries_features but creates files along the way """
        # Check parameters
        supported_time_unit = ['h', 'min', 's', 'ms']
        assert lag_unit in supported_time_unit, f"Only {supported_time_unit} are supported for w_time_unit"

        # Gather data
        temp_df = df.loc[:, ['cmdb_id', 'name', 'timestamp', 'value']]
        temp_df['timestamp'] = temp_df['timestamp'].dt.round('30s')  # reduce precision of timestamp

        # Create features: all kpis ('name') become features
        temp_df = pd.pivot_table(temp_df, index=[
                                 'timestamp', 'cmdb_id'], columns='name', values='value', dropna=True)
        temp_df.reset_index(['cmdb_id'], inplace=True)

        # Fill missing data using forward fill
        temp_df.fillna(method='ffill', inplace=True)

        # Drop rows with Nan values
        temp_df.dropna(axis=0, how='any', inplace=True)

        # Reduce mem usage
        temp_df = self.reduce_mem_usage(temp_df)

        # CREATE FILES
        num_rows = len(temp_df)
        num_files = math.ceil(num_rows/num_rows_per_file)

        print(f'Creating {num_files} files.')
        for i in range(num_files):
            filename = f'{data_folder}/ts_file{i}.pkl'
            
            if i % 10 == 0:
                print(f'{filename}')
            
            left_idx = i * num_rows_per_file
            right_idx = min(left_idx + num_rows_per_file, num_rows)


            # Shift data and create even more features (e.g. 'CPU_used (t-3min)')
            #  while creating supervised_dataset
            supervised_dataset = temp_df.iloc[left_idx:right_idx].reset_index()
            for i in range(history_length, 0, -step_size):
                s = temp_df.shift(periods=i, freq=lag_unit)
                s.columns = ["{}(t-{}{:s})".format(_n, i, lag_unit)
                            for _n in s.columns]
                s.reset_index(inplace=True)

                supervised_dataset = pd.merge(supervised_dataset, s, left_on=['cmdb_id', 'timestamp'], right_on=[
                                            'cmdb_id(t-{}{:s})'.format(i, lag_unit), 'timestamp'])
                supervised_dataset.drop(
                    'cmdb_id(t-{}{:s})'.format(i, lag_unit), axis=1, inplace=True)

            # Drop Nan and adding time features
            supervised_dataset.dropna(how='any', inplace=True)
            supervised_dataset['hour'] = supervised_dataset['timestamp'].dt.hour
            supervised_dataset.drop('timestamp', axis=1, inplace=True)

            supervised_dataset.to_pickle(filename)

        num_ts = temp_df.shape[1]
        num_timesteps = history_length
        return num_timesteps, num_ts
    
    def create_training_data(self, supervised_dataset, save_dir="./"):
        """
        Returns:
            - filenames: list of filename where training data is saved
                
        """
        unique_kpi = supervised_dataset.columns.str.extract(r'([a-zA-Z_\s]+)')[0].unique()
        unique_kpi = unique_kpi[~np.isin(unique_kpi, ['hour', 'cmdb_id', 'target_node', 'target_value', 'target_kpi'])]
        
        # Create train_df
        print(">The derived supervised_dataset has {} rows, now creating train_df with {}x more rows ({}).".format(
            supervised_dataset.shape[0], 
            unique_kpi.shape[0],
            unique_kpi.shape[0] * supervised_dataset.shape[0])
            )
        supervised_dataset.rename({ 'cmdb_id':  'target_node'}, axis=1, inplace=True)

        names = supervised_dataset.columns.tolist() + ['target_value', 'target_kpi']
        train_df = pd.DataFrame(columns=names)
        filenames = []
        i = 1
        for _kpi in unique_kpi:
            temp_df = supervised_dataset.rename({   _kpi: 'target_value'}, axis=1)
            temp_df['target_kpi'] = _kpi
            train_df = train_df.append(temp_df, ignore_index=True)

            memory = train_df.memory_usage().sum() / 1024**2
            if memory > 1000: # save every 1Go
                filename = save_dir + "training_data{}".format(i)
                print(">Saving {:.1f} MB of data into {:s}".format(memory, filename))
                train_df.to_parquet(filename, index=None)
                filenames.append(filename)
                i += 1
                del train_df
                train_df = pd.DataFrame(columns=names)
                gc.collect()
        train_df.rename({'cmdb_id':'target_node'}, axis=1, inplace=True)

        return filenames
    
    def scale_data(self, supervised_dataset):
        unique_kpi = supervised_dataset.columns.str.extract(r'([a-zA-Z_\s]+)')[0].unique()
        unique_kpi = unique_kpi[~np.isin(unique_kpi, ['hour', 'cmdb_id', 'target_node', 'target_value', 'target_kpi'])]
        
        self.scaler = MinMaxScaler(feature_range=(1, len(unique_kpi)))
        return self.scaler.fit_transform(supervised_dataset)        
    
    def create_testing_data(self, supervised_dataset, node, kpi):
        test_df = supervised_dataset[supervised_dataset['cmdb_id'] == node]
        test_df.rename({ 'cmdb_id': 'target_node', kpi: 'target_value'}, axis=1, inplace=True)
        test_df['target_kpi'] = kpi
        return test_df
            
#################################################################################
#                           DATA STATISTICS CLASS
#################################################################################
class DataStats:
    def __init__(self):
        pass

    # source: https://www.kaggle.com/aitude/ashrae-missing-weather-data-handling
    def missing_statistics(self, df):
        statitics = pd.DataFrame(df.isnull().sum()).reset_index()
        statitics.columns = ['COLUMN NAME', "MISSING VALUES"]
        statitics['TOTAL ROWS'] = df.shape[0]
        statitics['% MISSING'] = round(
            (statitics['MISSING VALUES']/statitics['TOTAL ROWS'])*100, 2)
        return statitics

    def infinite_statistics(self, df):
        statitics = pd.DataFrame(df[df.abs() >= np.inf].sum()).reset_index()
        statitics.columns = ['COLUMN NAME', "INFINITE VALUES"]
        statitics['TOTAL ROWS'] = df.shape[0]
        statitics['% INFINITE'] = round(
            (statitics['INFINITE VALUES']/statitics['TOTAL ROWS'])*100, 2)
        return statitics
