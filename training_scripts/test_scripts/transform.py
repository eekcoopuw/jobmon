import os
import argparse


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

def write_dummy_data(args):
    if not os.path.exists(args.output_file_path):
        os.makedirs(args.output_file_path)
    with open('{}/transform_{}_{}.txt'.format(args.output_file_path, args.location_id, args.sex_id), 'w') as f:
        f.write('location_id: {}, sex_id: {}'.format(args.location_id, args.sex_id))

def main():
    args = get_args()
    write_dummy_data(args)

if __name__ == '__main__':
    main()