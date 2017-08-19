import socket

hostname = socket.gethostname()

def get_zk_host():
  if hostname.startswith('api') or hostname.startswith('admin'):
    return "52.221.152.56:2181"
  return "localhost:9983"

def get_solr_host():
  if hostname.startswith('api') or hostname.startswith('admin'):
    return "52.221.153.113:8983"
  return "localhost:8983"
