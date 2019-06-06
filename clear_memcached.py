import sys
sys.path.append('/var/www/discovery_api/')
from disc.v2.utils import MemcacheUtils
MemcacheUtils.flush_all() 

