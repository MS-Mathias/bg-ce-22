import time
from joblib import Parallel, delayed
import math

def sqrt_func(i, j):
    time.sleep(0.001)
    return math.sqrt(i**j)

for x in range(10):
    start = time.time()
    
    Parallel(n_jobs=8)(delayed(sqrt_func)(i, j) for i in range(5) for j in range(2))
    end = time.time()
    print(end-start)
    help(delayed)