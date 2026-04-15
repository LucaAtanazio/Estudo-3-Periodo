from mpi4py import MPI
import numpy as np
import time

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

N = 1_000_000

if rank == 0:
    data = np.arange(N, dtype=np.float64)
    chunks = np.array_split(data, size)
else:
    chunks = None

comm.Barrier()
start = time.time()

local_data = comm.scatter(chunks, root=0)
local_sum = np.sum(local_data)

total_sum = comm.reduce(local_sum, op=MPI.SUM, root=0)

comm.Barrier()
end = time.time()

elapsed = end - start

all_times = comm.gather(elapsed, root=0)

if rank == 0:
    print(f"Soma total: {total_sum}")
    print(f"Tempos por processo: {all_times}")
    print(f"Tempo máximo observado: {max(all_times):.6f} s")