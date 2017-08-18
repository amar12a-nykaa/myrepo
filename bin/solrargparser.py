import argparse
parser = argparse.ArgumentParser()
parser.add_argument('core', help='core name')
argv = vars(parser.parse_args())
print(argv)
core = argv['core']
assert core in ['autocomplete', 'live']


