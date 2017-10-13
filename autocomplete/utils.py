import unicodedata
import re
import sys

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils

def strip_accents(text):
    return ''.join(char for char in unicodedata.normalize('NFKD', text) if unicodedata.category(char) != 'Mn')

def createId(s):
  s = strip_accents(s)
  s = re.sub('[^A-Za-z0-9 ]+', '', s).lower()
  s = re.sub(' +', '_', s).lower()
  return s

if __name__ == '__main__':
  terms = [
    "kiehl's",
    "M.A.C",
    "L'Oreal Paris",
    "Loreal     Paris",
    "Estée Lauder",
    "Estee Lauder",
  ]

  for term in terms:
    print(term, "->", createId(term))
