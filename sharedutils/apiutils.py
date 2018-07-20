import sys
import json
import socket

class ApiUtils:
  def get_host():
    host = 'localhost'
    if socket.gethostname().startswith('admin'):
      host = 'priceapi.nyk00-int.network'
    return host

