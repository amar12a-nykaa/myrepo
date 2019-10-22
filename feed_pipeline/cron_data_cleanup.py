import os

filepath = "/nykaa/product_metadata/data.csv"
#Write files to s3 Remove from machine
try:
  cmd = "/usr/local/bin/aws s3 cp "+filepath+" s3://nykaadp-feeds/product_metadata/"
  out = subprocess.check_output(cmd , shell=True).strip()
except:
  print("Error! Could not upload product_metadata to s3")
if os.path.exists(filepath):
  os.remove(filepath)
