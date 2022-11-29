import os
import argparse
import getpass
from typing import Dict


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--location__id',
                        required=False,
                        action='store',
                        help='location_id')
    parser.add_argument('-v', '--log_level',
                        required=False,
                        action='store',
                        help='log_level')
    args = parser.parse_args()
    return args


def write_dummy_data(args: Dict) -> None:
    user = getpass.getuser()
    output_path = f'/ihme/scratch/users/{user}/jobmon_quickstart_example/'
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    location_ids = list(range(args.location_set_id))
    for loc_id in location_ids:
        with open(f'{output_path}/location_result_{loc_id}.txt', 'w') as f:
            f.write(f'location_id: {loc_id}, log_level: {args.log_level}')


def main():
    args = get_args()
    write_dummy_data(args)


if __name__ == '__main__':
    main()
