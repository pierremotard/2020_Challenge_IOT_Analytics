import h5py
import os
import pandas as pd
import numpy as np

uniq_chn = set()
directory = "competitionfiles"
for filename in os.listdir(directory):
    f = h5py.File(os.path.join(directory, filename), 'r')
    for ch_name in f['DYNAMIC DATA']:
        uniq_chn.add(ch_name)
uniq_chn = list(uniq_chn)

def hd5_to_df(filename, directory, resample=False):
    if filename.endswith(".hdf"): 
        f = h5py.File(os.path.join(directory, filename), 'r')
        d = dict()
        for key, item in f['DYNAMIC DATA'].items():
            d[key] = np.array(item['MEASURED'])
            df = pd.DataFrame(d)

        first_ts = f['DYNAMIC DATA'].attrs['FIRST ACQ TIMESTAMP']
        second_ts = f['DYNAMIC DATA'].attrs['LAST ACQ TIMESTAMP']
        dts = pd.to_datetime(first_ts.decode("utf-8"))
        dtf = pd.to_datetime(second_ts.decode("utf-8"))

        ts = np.linspace(dts.value, dtf.value, len(f['DYNAMIC DATA']['ch_1']['MEASURED']))
        ts_ind = pd.to_datetime(ts)
        df = df.set_index(ts_ind)
    
    for name in uniq_chn:
        if name not in df.columns:
            df[name] = np.NaN

    if resample:
        df = df.resample('5s').mean()
    return df

def get_channel_data(ch_name, filename, directory):
    if filename.endswith(".hdf"):
        f = h5py.File(os.path.join(directory, filename), 'r')
        if ch_name not in f['DYNAMIC DATA']: return np.array([])
        return np.array(f['DYNAMIC DATA'][ch_name]['MEASURED'])
    else:
        print('Invalid Filename')
        return None
    
def get_all_channel_data(ch_name, file_list, directory):
    channel_data = []
    for filename in file_list:
        if filename.endswith(".hdf"):
            channel_data.extend(list(get_channel_data(ch_name, filename, directory)))
    return np.array(channel_data)

def get_machine_dict(directory):
    categ = {}
    for filename in os.listdir(directory):
        f = h5py.File(os.path.join(directory, filename), 'r')
        chanIDs = f['DYNAMIC DATA']
        currLen = len(chanIDs.keys())
        if currLen == 152 or currLen == 120 or currLen == 238:
            try:
                if categ['M' + str(currLen)] != None:
                    categ['M' + str(currLen)].append(filename)
                else:
                    categ['M' + str(currLen)] = [filename]
            except KeyError:
                categ['M' + str(currLen)] = [filename]
    
    return categ

def get_min_channel(ch_name, directory):
    files = [filename for filename in os.listdir(directory)]
    return [get_channel_data(ch_name, file, directory).min() for file in files]

def get_max_channel(ch_name, directory):
    files = [filename for filename in os.listdir(directory)]
    return [get_channel_data(ch_name, file, directory).max() for file in files]

def get_avg_channel(ch_name, directory):
    files = [filename for filename in os.listdir(directory)]
    return [get_channel_data(ch_name, file, directory).mean() for file in files]

