import h5py
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

directory = "competitionfiles"
for filename in os.listdir(directory):
    if filename.endswith(".hdf"): 
        f = h5py.File(os.path.join(directory, filename), 'r')
        chanIDs = f['DYNAMIC DATA']
        df = pd.DataFrame.from_dict(f['DYNAMIC DATA'])
        print(df.head(10))
        break
    else:
        continue
