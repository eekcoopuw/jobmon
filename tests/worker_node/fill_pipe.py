import sys
from time import sleep

ten_twenty_four = "a" * 2**10
for i in range(2**8):
    sys.stderr.write(ten_twenty_four)
    sys.stderr.flush()
    sleep(0.1)

# force us to log an error which will trigger the assert
sys.exit(1)
