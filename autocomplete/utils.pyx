import unicodedata
import re
import sys

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

from nltk.stem import PorterStemmer
from nltk.tokenize import sent_tokenize, word_tokenize
ps = PorterStemmer()

def strip_accents(text):
    return ''.join(char for char in unicodedata.normalize('NFKD', text) if unicodedata.category(char) != 'Mn')

def createId(s):
  s = strip_accents(s)
  s = ps.stem(s)
  s = re.sub('[^A-Za-z0-9 _]+', '', s).lower()
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
