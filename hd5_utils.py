import h5py
import os
import pandas as pd
import numpy as np

def hd5_to_df(filename, directory):
    if filename.endswith(".hdf"): 
        f = h5py.File(os.path.join(directory, filename), 'r')
        d = dict()
        for key, item in f['DYNAMIC DATA'].items():
            d[key] = np.array(item['MEASURED'])
        df = pd.DataFrame(d)
        return df
    else:
        print('Invalid Filename')
        return None

def get_channel_data(ch_name, filename, directory):
    if filename.endswith(".hdf"):
        f = h5py.File(os.path.join(directory, filename), 'r')
        return np.array(f['DYNAMIC DATA'][ch_name]['MEASURED'])
    else:
        print('Invalid Filename')
        return None
    

def get_dict_3_categ(directory):
    categ = {}
    for filename in os.listdir(directory):
        f = h5py.File(os.path.join(directory, filename), 'r')
        chanIDs = f['DYNAMIC DATA']
        currLen = len(chanIDs.keys())
        if currLen == 152 or currLen == 120 or currLen == 238:
            try:
                if categ[currLen] != None:
                    categ[currLen].append(filename)
                else:
                    categ[currLen] = [filename]
            except KeyError:
                categ[currLen] = [filename]
