import sys
sys.path.append("/nykaa/scripts/feed_third_party/feed.py")
from feed import *
create_feed_index()
sys.path.append("/nykaa/scripts/feed_third_party/feed_update.py")
from feed import *
update_feed3()
sys.path.append("/nykaa/scripts/feed_third_party/feed_updateAttribute.py")
from feed import *
updateAtrribute_feed3()
sys.path.append("/nykaa/scripts/feed_third_party/feed_updateFields.py")
from feed import *
updateFields_feed3()
sys.path.append("/nykaa/scripts/feed_third_party/feed4_updateFields.py")
from feed import *
updateFields_feed4()
sys.path.append("/nykaa/scripts/feed_third_party/feed_new4_updateWishlist.py")
from feed import *
updatewislist_feed4()