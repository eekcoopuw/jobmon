import os
import argparse

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-l', '--location_hierarchy_id',
                        required=False,
                        action='store',
                        help='location hierarchy id')
    parser.add_argument('-o', '--output_file_path',
                        required=False,
                        action='store',
                        help='output file path')
    args = parser.parse_args()
    return args

def write_dummy_data(args):
    if not os.path.exists(args.output_file_path):
        os.makedirs(args.output_file_path)
    with open('{}/summarize_{}.txt'.format(args.output_file_path, args.location_hierarchy_id), 'w') as f:
        f.write('location_hierarchy_id: {}'.format(args.location_hierarchy_id))

def main():
    args = get_args()
    write_dummy_data(args)

if __name__ == '__main__':
    main()