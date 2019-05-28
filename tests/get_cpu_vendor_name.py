def get_cpu_vendor_name() -> str:
    """Returns the architecture name on the current host"""
    vendor_type = "Could not find vendor-type"
    with open("/proc/cpuinfo", "r") as f:
        cpuinfo = f.read()
        for line in cpuinfo.splitlines():
            if line.startswith("vendor_id"):
                vendor_type = line.split(":")[1].strip()
    return vendor_type


print(get_cpu_vendor_name())