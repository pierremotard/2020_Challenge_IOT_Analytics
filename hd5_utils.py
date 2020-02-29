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