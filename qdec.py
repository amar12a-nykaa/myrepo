import re
import traceback
import subprocess
import arrow
import os
import argparse
import datetime
def generate_gludo_orders():
  now = arrow.utcnow()

  DIR = "/tmp/error_logs_api_machines"
  machines = ['52.220.215.78' , '52.221.72.116', '52.221.34.173', '52.77.199.176']
  os.system("mkdir -p %s" % DIR)

  datestrs = [now.format('YYYY-MM-DD')]

  outfile = "/tmp/qty_decs.txt"
  os.system("rm -f %s" % outfile)
  for machine in machines:
    print(machine)
    dir1 = DIR + "/" + machine
    os.system("mkdir -p %s" % dir1)
    for datestr in datestrs:
      print("---")
      try:
        files = subprocess.check_output("ssh -i /root/.ssh/id_rsa ubuntu@%s 'ls /var/log/apache2/error.log'" % (machine,), shell=True)
        files = files.decode().split("\n")
        files = [f for f in files if f]
        print("files: %r" % files)
        for f in files:
          basename = os.path.basename(f) 
          localpath = DIR + "/%s/%s" % (machine, basename)
          cmd = "scp -i /root/.ssh/id_rsa ubuntu@%s:%s %s" % (machine, f, localpath)
          print(cmd)
          os.system(cmd)
          if re.search(".gz$",  localpath):
            os.system(localpath)

        print(cmd)
        os.system(cmd)
      except Exception as e:
        print("Exception .. ")
        if 'No such file or directory' in str(e):
          pass
        else:
          print(traceback.format_exc())
        pass
      localfiles = subprocess.check_output("ls -d -1 %s/%s/*.*" % (DIR, machine), shell=True).decode().split("\n")
      for localfile in localfiles:
        if not localfile:
          continue
        cmd = 'grep "Quantity decreased" %s >> %s' % (localfile, outfile)
        print(cmd)
        os.system(cmd)

  os.system("rm -f /nykaa/scripts/gludo_orders.csv")
  with open(outfile, 'r') as f:
    with open("/nykaa/scripts/gludo_orders.csv", 'w') as csv:
      csv.write("sku,quantity\n")
      for line in f:

        date_search_str = now.format("MMM DD HH")
#      date_search_str = d1.format("MMM DD ")
#      if hour is not None:
#        date_search_str += hour
        
        m = re.search(date_search_str + ".*Quantity decreased for sku ([^ ]+) by ([0-9]+)", line) 
        if not m:
          continue
        #print(m.group(0))
        sku = m.group(1)
        qty = m.group(2)
        csv.write("%s,%s\n" % (sku, qty))

