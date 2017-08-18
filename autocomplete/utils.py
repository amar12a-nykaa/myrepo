import unicodedata
import re
import sys

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

def strip_accents(text):
    return ''.join(char for char in unicodedata.normalize('NFKD', text) if unicodedata.category(char) != 'Mn')

def createId(s):
  s = strip_accents(s)
  return re.sub('[^A-Za-z0-9]+', '_', s).lower()

