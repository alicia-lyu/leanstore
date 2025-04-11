import os, re

def collect_data(exec):
    p = f"./build/{exec}"
    maintenance_f = open(f"{p}/maintenance.csv", "w")
    # maintenance_f.write("scale,dram,method")
    query_f = open(f"{p}/query.csv", "w")
    # query_f.write("scale,dram,method")
    # all dirs in p
    for exp in os.listdir(p):
        pattern = r"([\d.]+)-in-([\d.]+)"
        m = re.match(pattern, exp)
        if not m:
            print(f"Skipping {exp}")
            continue
        scale = m.group(1)
        dram = m.group(2)
        for d in os.listdir(os.path.join(p, exp)):
            d_pattern = r"(maintain|query)-(\w+)\.csv"
            match = re.match(d_pattern, d)
            if not match:
                print(f"Skipping {d}")
                continue
            tx = match.group(1)
            method = match.group(2)
            if tx == "maintain":
                with open(os.path.join(p, exp, d), "r") as f:
                    if maintenance_f.tell() == 0:
                        maintenance_f.write("method,dram,scale,")
                        header = f.readline()
                        maintenance_f.write(header)
                    # add the last line
                    lines = f.readlines()
                    maintenance_f.write(f"{method},{dram},{scale}," + lines[-1])
            elif tx == "query":
                with open(os.path.join(p, exp, d), "r") as f:
                    if query_f.tell() == 0:
                        query_f.write("method,dram,scale,")
                        header = f.readline()
                        query_f.write(header)
                    # add the last line
                    lines = f.readlines()
                    query_f.write(f"{method},{dram},{scale}," + lines[-1])
    maintenance_f.close()
    query_f.close()
    
if __name__ == "__main__":
    collect_data("basic_join")
    collect_data("basic_group")
    collect_data("basic_group_variant")
                    
                    
                
                
            
        
            
        
    