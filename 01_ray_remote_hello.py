import ray
import os
import socket


# local execution
def hello_from_local():
    print( f"Running on {socket.gethostname()} in pid {os.getpid()}" )
    return 

hello_from_local()

# remote execution
@ray.remote
def hello_from():
    return f"Running on {socket.gethostname()} in pid {os.getpid()}"

future = hello_from.remote()
print(ray.get(future))

os.system("/bin/bash -c 'read -s -n 1 -p \"Press any key to continue...\"'")
print()
