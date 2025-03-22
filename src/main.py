import mmap
import multiprocessing
import os
import math

# Determine CPU count and page size (using sysconf if available)
CPU_COUNT = os.cpu_count()
try:
    MMAP_PAGE_SIZE = os.sysconf("SC_PAGE_SIZE")
except AttributeError:
    MMAP_PAGE_SIZE = mmap.ALLOCATIONGRANULARITY

def to_int(x: bytes) -> int:
    # Convert a temperature string (e.g., b"76.2", b"-9.9") to an integer representation (temp*10)
    n = len(x)
    if n == 5:  # e.g., b"-99.9"
        return -100 * x[1] - 10 * x[2] - x[4] + 5328
    elif n == 4 and x[0] == 45:  # e.g., b"-9.9" (45 is ASCII for '-')
        return -10 * x[1] - x[3] + 528
    elif n == 4:  # e.g., b"99.9"
        return 100 * x[0] + 10 * x[1] + x[3] - 5328
    else:  # n == 3, e.g., b"9.9"
        return 10 * x[0] + x[2] - 528

def process_line(line: bytes, result: dict) -> None:
    idx = line.find(b";")
    if idx == -1:
        return
    city = line[:idx]
    # Convert the temperature string to an int (temperature * 10)
    temp_int = to_int(line[idx + 1 : -1])
    if city in result:
        item = result[city]
        item[0] += 1      # count
        item[1] += temp_int   # sum
        item[2] = min(item[2], temp_int)
        item[3] = max(item[3], temp_int)
    else:
        result[city] = [1, temp_int, temp_int, temp_int]

def align_offset(offset: int, page_size: int) -> int:
    return (offset // page_size) * page_size

def process_chunk(file_path: str, start_byte: int, end_byte: int) -> dict:
    # Align the start offset to the system page size
    offset = align_offset(start_byte, MMAP_PAGE_SIZE)
    result = {}
    with open(file_path, "rb") as file:
        length = end_byte - offset
        with mmap.mmap(file.fileno(), length, access=mmap.ACCESS_READ, offset=offset) as mmapped_file:
            mmapped_file.seek(start_byte - offset)
            for line in iter(mmapped_file.readline, b""):
                process_line(line, result)
    return result

def reduce(results: list) -> dict:
    final = {}
    for result in results:
        for city, item in result.items():
            if city in final:
                final[city][0] += item[0]
                final[city][1] += item[1]
                final[city][2] = min(final[city][2], item[2])
                final[city][3] = max(final[city][3], item[3])
            else:
                final[city] = item
    return final

def round_to_infinity(x: float) -> float:
    # Round upward (ceiling) to one decimal place per IEEE 754 "round to infinity"
    return math.ceil(x * 10) / 10

def read_file_in_chunks(file_path: str) -> None:
    file_size_bytes = os.path.getsize(file_path)
    base_chunk_size = file_size_bytes // CPU_COUNT
    chunks = []

    with open(file_path, "r+b") as file:
        with mmap.mmap(file.fileno(), length=0, access=mmap.ACCESS_READ) as mmapped_file:
            start_byte = 0
            for _ in range(CPU_COUNT):
                end_byte = min(start_byte + base_chunk_size, file_size_bytes)
                newline_index = mmapped_file.find(b"\n", end_byte)
                if newline_index == -1:
                    end_byte = file_size_bytes
                else:
                    end_byte = newline_index + 1
                chunks.append((file_path, start_byte, end_byte))
                start_byte = end_byte

    with multiprocessing.Pool(processes=CPU_COUNT) as pool:
        results = pool.starmap(process_chunk, chunks)

    final = reduce(results)
    # Write the output sorted alphabetically by city (decoded as UTF-8)
    with open("output.txt", "w", encoding="utf8") as out_file:
        for city, vals in sorted(final.items(), key=lambda x: x[0].decode("utf8")):
            count, total, min_val, max_val = vals
            # Compute mean using integer arithmetic; division converts to float here only for output
            mean_val = total / count
            # Convert back to the original temperature by dividing by 10 and applying rounding
            out_file.write(f"{city.decode('utf8')}={round_to_infinity(min_val/10):.1f}/{round_to_infinity(mean_val/10):.1f}/{round_to_infinity(max_val/10):.1f}\n")

if __name__ == "__main__":
    read_file_in_chunks("testcase.txt")
