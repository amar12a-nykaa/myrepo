import pprint
import argparse
import getpass

USER = getpass.getuser()

parser = argparse.ArgumentParser()
parser.add_argument("--user", "-u")
argv = vars(parser.parse_args())
if argv['user']:
  USER = argv['user']

filepath_github = "/nykaa/scripts/ssh_keys/public_keys.txt"

if USER == 'root':
  filepath_authorized_keys = "/root/.ssh/authorized_keys"
else:
  filepath_authorized_keys = "/home/%s/.ssh/authorized_keys" % USER

def get_contents(filepath):
  with open(filepath) as f:
    content = f.readlines()
  content = [x.strip() for x in content if x.strip()]
  return set(content)
  #pprint.pprint(content)

keys_github = get_contents(filepath_github)
keys_authorized_keys = get_contents(filepath_authorized_keys)

#print("keys_github: %s" % keys_github)
#print("keys_authorized_keys: %s" % keys_authorized_keys)

missing_keys = keys_github - keys_authorized_keys

print("missing_keys:")
pprint.pprint(missing_keys)

with open(filepath_authorized_keys, "a") as myfile:
  for key in missing_keys: 
    myfile.write(key + "\n")

