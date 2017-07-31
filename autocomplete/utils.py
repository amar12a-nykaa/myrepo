import re
import sys

sys.path.append("/nykaa/api")
from pas.v1.utils import Utils


def createId(s):
  return re.sub('[^A-Za-z0-9]+', '_', s).lower()


#class Brands():
#  def select_all():
#    2
