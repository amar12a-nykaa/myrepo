"""This script is made for devops purpose to easily find details about currently live cluster to send as email header"""

import json
import sys
from IPython import embed

sys.path.append('/nykaa/scripts/sharedutils/')
from loopcounter import LoopCounter
from esutils import EsUtils

sys.path.append("/var/www/pds_api")
from pas.v2.utils import Utils as PasUtils
sys.path.append("/var/www/discovery_api")
from disc.v2.utils import Utils as DiscUtils

es = DiscUtils.esConn()
active_index = es.cat.aliases("livecore", format="json")[0]['index']
num_docs_active = es.count("livecore")['count']
resp = {
	"active_index": active_index,
	"number_docs_active_index": num_docs_active
}
print(json.dumps(resp))
