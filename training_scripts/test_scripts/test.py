import os
import argparse
import getpass
from typing import Dict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--args1',
                        required=False,
                        action='store',
                        help='args 1')
    parser.add_argument('-b', '--args2',
                        required=False,
                        action='store',
                        help='args 2')
    args = parser.parse_args()
    return args

def write_dummy_data(args: Dict) -> None:
    user = getpass.getuser()
    output_path = f'/ihme/scratch/users/{user}/jobmon_test/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    with open(f'{output_path}/test.txt', 'w') as f:
        f.write(f'args1: {args.args1}, args2: {args.args2}')

def main():
    args = get_args()
    write_dummy_data(args)

if __name__ == '__main__':
    main()