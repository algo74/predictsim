import numpy as np

def np_array_to_file(a, filename):
    predout=np.reshape(a,(-1,1))
    np.savetxt(filename,a,fmt="%d")

def array_to_file(a, filename):
    na=np.array(a)
    na=np.reshape(na,(-1,1))
    np.savetxt(filename,na,fmt="%d")