import argparse

def read_config_as_first_arg():
  parser = argparse.ArgumentParser()
  parser.add_argument('config', help='config name')
  argv = vars(parser.parse_args())
  print(argv)
  return argv['config']
  


def read_collection_as_first_arg():
  parser = argparse.ArgumentParser()
  parser.add_argument('collection', help='collection name')
  argv = vars(parser.parse_args())
  print(argv)
  return argv['collection']

