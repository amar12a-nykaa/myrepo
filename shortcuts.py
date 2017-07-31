import IPython
import os
import os.path
shortcuts = [
  ("/nykaa/scripts/mysqlremote.py", "/usr/local/bin/mysqlremote")
]

def ensure_symlink(addr, ptr):
  for addr, ptr in shortcuts:
    to_create = False
    if os.path.exists(ptr):
      if not os.path.islink(ptr) or os.readlink(ptr) != addr:
        os.remove(ptr)
        to_create = True

    else:
      to_create = True
    
    if to_create:
      os.symlink(addr, ptr)
      print("Created a new symlink: %s -> %s" % (ptr, addr))
      
for addr, ptr in shortcuts:
  ensure_symlink(addr, ptr)
