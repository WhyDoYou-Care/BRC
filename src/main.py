import multiprocessing
import os
import mmap
import math
from typing import List, Dict, Tuple
from dataclasses import dataclass

@dataclass
class City:
    min: float
    max: float
    sum: float
    count: int

file_path = "testcase.txt"

def is_new_line(position: int, mm: mmap.mmap) -> bool:
    if position == 0:
        return True
    else:
        mm.seek(position - 1)
        return mm.read(1) == b"\n"

def next_line(position: int, mm: mmap.mmap) -> int:
    mm.seek(position)
    mm.readline()
    return mm.tell()

def process_chunk(chunk_start: int, chunk_end: int) -> Dict[bytes, City]:
    chunk_size = chunk_end - chunk_start
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), length=chunk_size, access=mmap.ACCESS_READ, offset=chunk_start)
        if chunk_start != 0:
            next_line(0, mm)
        result: Dict[bytes, City] = {}
        for line in iter(mm.readline, b""):
            location, temp_str = line.split(b";")
            measurement = float(temp_str)
            if location not in result:
                result[location] = City(measurement, measurement, measurement, 1)
            else:
                _result = result[location]
                if measurement < _result.min:
                    _result.min = measurement
                if measurement > _result.max:
                    _result.max = measurement
                _result.sum += measurement
                _result.count += 1
        mm.close()
        return result

def identify_chunks(num_processes: int) -> List[Tuple[int, int]]:
    chunk_results = []
    with open(file_path, "r+b") as file:
        mm = mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ)
        file_size = os.path.getsize(file_path)
        chunk_size = file_size // num_processes
        chunk_size += mmap.ALLOCATIONGRANULARITY - (chunk_size % mmap.ALLOCATIONGRANULARITY)
        start = 0
        while start < file_size:
            end = start + chunk_size
            if end < file_size:
                end = next_line(end, mm)
            if start == end:
                end = next_line(end, mm)
            if end > file_size:
                end = file_size
                chunk_results.append((start, end))
                break
            chunk_results.append((start, end))
            start += chunk_size
        mm.close()
        return chunk_results

def round_to_infinity(x: float) -> float:
    return math.ceil(x * 10) / 10

def main() -> None:
    num_processes = os.cpu_count() or 1
    chunk_results = identify_chunks(num_processes)
    with multiprocessing.Pool(num_processes) as pool:
        ret_dicts = pool.starmap(process_chunk, chunk_results)
    shared_results: Dict[bytes, City] = {}
    for return_dict in ret_dicts:
        for station, data in return_dict.items():
            if station in shared_results:
                _result = shared_results[station]
                if data.min < _result.min:
                    _result.min = data.min
                if data.max > _result.max:
                    _result.max = data.max
                _result.sum += data.sum
                _result.count += data.count
            else:
                shared_results[station] = data
    with open("output.txt", "w", encoding="utf8") as out_file:
        for location, measurements in sorted(shared_results.items(), key=lambda kv: kv[0].decode("utf8")):
            min_val = round_to_infinity(measurements.min)
            mean_val = round_to_infinity(measurements.sum / measurements.count)
            max_val = round_to_infinity(measurements.max)
            out_file.write(f"{location.decode('utf8')}={min_val:.1f}/{mean_val:.1f}/{max_val:.1f}\n")

if __name__ == "__main__":
    main()
