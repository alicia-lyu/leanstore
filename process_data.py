import os, re

def collect_data(exec):
    p = f"./build/{exec}"
    sum_f = open(f"{p}.csv", "w")
    for exp in os.listdir(p):
        pattern = r"([\d.]+)-in-([\d.]+)"
        m = re.match(pattern, exp)
        if not m:
            print(f"Skipping {exp}")
            continue
        scale = m.group(1)
        dram = m.group(2)
        data_files = sorted(os.listdir(os.path.join(p, exp)))
        size_path = os.path.join(p, exp, "size.csv")
        size_dict = {}
        for line in open(size_path, "r"):
            line = line.strip()
            if not line:
                continue
            key, value = line.split(",")
            size_dict[key] = value
        for d in data_files:
            d_pattern = r"(maintain|query|point-query)-(\w+)\.csv"
            match = re.match(d_pattern, d)
            if not match:
                print(f"Skipping {d}")
                continue
            tx = match.group(1)
            method = match.group(2)
            with open(os.path.join(p, exp, d), "r") as f:
                if sum_f.tell() == 0:
                    sum_f.write("method,tx,dram,scale,")
                    header = f.readline().strip().strip(",")
                    sum_f.write(header + ",size(mib)\n")
                latest_line = f.readlines()[-1].strip().strip(",")
                sum_f.write(f"{method},{tx},{dram},{scale}," + latest_line + f",{size_dict[method]}\n")
    sum_f.close()
    
if __name__ == "__main__":
    collect_data("basic_join")
    collect_data("basic_group")
    collect_data("basic_group_variant")