import sys
import math
from collections import defaultdict
import multiprocessing as mp 
from operator import add
from os import SEEK_END
from time import time

class SerialPool:
    def imap(self, func, it, chunksize="ignored"):
        for x in it:
            yield func(x)

def merge_dict(acc: dict, new: dict, fn:"callable"):
    for k, v in new.items():
        if k in acc:
            acc[k] = fn(acc[k], v)
        else:
            acc[k] = v

class State:
    def __init__(self):
        self.mins = defaultdict(lambda: float('inf'))
        self.maxs = defaultdict(lambda: float('-inf'))
        self.tots = defaultdict(lambda: 0.0)
        self.cnts = defaultdict(lambda: 0)

    def proc(self, name, temp):
        self.cnts[name] += 1
        self.tots[name] += temp
        self.mins[name] = min(self.mins[name], temp)
        self.maxs[name] = max(self.maxs[name], temp)

    def freeze(self):
        for d in [self.mins, self.maxs, self.tots, self.cnts]:
            if isinstance(d, defaultdict):
                d.default_factory = None

    def merge(self, st):
        merge_dict(self.cnts, st.cnts, add)
        merge_dict(self.tots, st.tots, add)
        merge_dict(self.mins, st.mins, min)
        merge_dict(self.maxs, st.maxs, max)

def file_size(f) -> int:
    x = f.tell()
    ret = f.seek(0, SEEK_END)
    f.seek(x)
    return ret

def seek_next_line(f) -> int:
    f.readline()  # advance to the end of current line
    return f.tell()

def gen_chunks(filename: str, chunks: int):
    """Yield [lo, hi) positions for each chunk. Both lo and hi will point to the beginning of a line (or EOF)."""
    f = open(filename, 'rb')
    sz = file_size(f)
    chunk_sz = sz // chunks
    print(f"file size: {sz}, chunks {chunks}")
    x = 0
    f.seek(x)
    while x < sz:
        y = x + chunk_sz
        if y >= sz:
            yield (x, sz)
            return
        f.seek(y)
        y = seek_next_line(f)
        yield (x, y)
        x = y

def dochunk(tup) -> State:
    filename, lo, hi = tup 
    f = open(filename)
    f.seek(lo)
    st = State()
    # Process each line until reaching the end of this chunk
    while f.tell() < hi:
        line = f.readline()
        if not line or not line.strip():
            continue
        name, tempstr = line.strip().split(';')
        temp = float(tempstr)
        st.proc(name, temp)
    print(f"Chunk processed from {lo} to {hi} (file pointer at {f.tell()})")
    st.freeze()
    return st

def round_to_infinity(x: float) -> float:
    """
    Rounds the given number to one decimal place according to the IEEE 754
    "round toward +∞" (i.e. round to infinity) rule.
    """
    return math.ceil(x * 10) / 10

def main(filename, chunk_count):
    print(f"Reading from {filename}. chunks: {chunk_count}")
    start = time()
    pool = mp.Pool()
    acc = State()
    tups = ((filename, lo, hi) for lo, hi in gen_chunks(filename, chunk_count))
    for i, st in enumerate(pool.imap(dochunk, tups)):
        print(f"Processed chunk {i+1}/{chunk_count} in {time() - start:.2f} seconds")
        acc.merge(st)
    
    with open("output.txt", "w") as f_out:
        names = sorted(acc.cnts.keys())
        for name in names:
            avg = acc.tots[name] / acc.cnts[name]
            rounded_min = round_to_infinity(acc.mins[name])
            rounded_avg = round_to_infinity(avg)
            rounded_max = round_to_infinity(acc.maxs[name])
            f_out.write(f"{name}={rounded_min:.1f}/{rounded_avg:.1f}/{rounded_max:.1f}\n")
    print("Output written to output.txt")
    
if __name__ == "__main__":
    filename = "testcase.txt"    
    chunk_count = mp.cpu_count() 
    if len(sys.argv) > 1:
        filename = sys.argv[1]
    if len(sys.argv) > 2:
        chunk_count = int(sys.argv[2])
    main(filename, chunk_count)
