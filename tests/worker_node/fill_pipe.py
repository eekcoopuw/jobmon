import sys
from time import sleep

ten_twenty_four = "a" * 2**10
for i in range(2**8):
    sys.stderr.write(ten_twenty_four)
    sys.stderr.flush()
    if i == (2**6 - 1):
        sys.stderr.write("\n")
        sleep(5)
    if i == (2**7 - 1):
        sys.stderr.write("\n")
        sleep(5)
