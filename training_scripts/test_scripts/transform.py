import os
import argparse
from typing import Dict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--location_id',
                        required=False,
                        action='store',
                        help='location id')
    parser.add_argument('-s', '--sex_id',
                        required=False,
                        action='store',
                        help='sex id')
    parser.add_argument('-o', '--output_file_path',
                        required=False,
                        action='store',
                        help='output file path')
    args = parser.parse_args()
    return args

def write_dummy_data(args: Dict) -> None:
    if not os.path.exists(args.output_file_path):
        os.makedirs(args.output_file_path)
    with open(f'{args.output_file_path}/transform_{args.location_id}_{args.sex_id}.txt', 'w') as f:
        f.write(f'location_id: {args.location_id}, sex_id: {args.sex_id}')

def main():
    args = get_args()
    write_dummy_data(args)

if __name__ == '__main__':
    main()