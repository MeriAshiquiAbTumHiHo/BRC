import math
import threading
import os

def round_inf(x):
    return math.ceil(x * 10) / 10

def process_chunk(lines, result_list, index):
    data = {}
    for line in lines:
        if ';' not in line:
            continue
        city, temp = line.split(';', 1)
        try:
            temp = float(temp)
        except ValueError:
            continue  # Skip invalid lines
        if city in data:
            mn, total, mx, count = data[city]
            data[city] = (min(mn, temp), total + temp, max(mx, temp), count + 1)
        else:
            data[city] = (temp, temp, temp, 1)
    result_list[index] = data

def merge_data(data_list):
    final = {}
    for data in data_list:
        if data is None:
            continue
        for city, (mn, total, mx, count) in data.items():
            if city in final:
                f_mn, f_total, f_mx, f_count = final[city]
                final[city] = (min(f_mn, mn), f_total + total, max(f_mx, mx), f_count + count)
            else:
                final[city] = (mn, total, mx, count)
    return final

def main():
    if not os.path.exists("testcase.txt"):
        print("Error: 'testcase.txt' not found!")
        return

    with open("testcase.txt", "r", buffering=2**20) as f:
        lines = f.read().splitlines()

    if not lines:
        print("Error: 'testcase.txt' is empty!")
        return

    num_threads = min(16, (len(lines) // 10000) + 1)
    chunk_size = len(lines) // num_threads + 1
    chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    threads = []
    result_list = [None] * len(chunks)

    for i, chunk in enumerate(chunks):
        t = threading.Thread(target=process_chunk, args=(chunk, result_list, i))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    final_data = merge_data(result_list)
    
    output_lines = []
    for city in sorted(final_data):
        mn, total, mx, count = final_data[city]
        avg = round_inf(total / count)
        output_lines.append(f"{city}={mn:.1f}/{avg:.1f}/{mx:.1f}\n")

    try:
        with open("output.txt", "w", buffering=2**20) as f:
            f.writelines(output_lines)
    except Exception as e:
        print(f"Error writing to 'output.txt': {e}")

if __name__ == "__main__":
    main()
