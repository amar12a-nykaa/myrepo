import sys
import requests
import json
import csv
import socket
import argparse

sys.path.append("/var/www/pds_api")
from pas.v3.utils import Utils as PasUtils

from utils.mailutils import Mail


parser = argparse.ArgumentParser()
parser.add_argument("-d", "--disabledswitch", default=0, help='switch for disabled products', type=int)
argv = vars(parser.parse_args())
disabled_switch = argv['disabledswitch']

CHUNK_SIZE = 100
diff_csv_headers = ["sku", "type", "backorders_v2", "backorders_v3", "msp_v2", "msp_v3", "expdt_v2",
           "expdt_v3", "mrp_v2", "mrp_v3", "is_in_stock_v2", "is_in_stock_v3", "quantity_v2", "quantity_v3", "sp_v2", "sp_v3", "jit_eretail_v2", "jit_eretail_v3"]
diff_csv_filename = "diff_v2_v3.csv"
fields_to_check = ["backorders", "msp", "expdt", "mrp", "is_in_stock", "sp", "jit_eretail"]
MAIL_RECIPIENTS = "gaurav.sharma@nykaa.com, charu.sharma@nykaa.com, sandeep@euler-systems.com, kangkan@gludo.com, raman@gludo.com, kedar.pagdhare@nykaa.com, rishi.kataria@nykaa.com, saurav.goyal@nykaa.com"
diff_exist = False
number_of_rows = 0
margin = 1
diff_map = {"backorders": 0, "msp": 0, "expdt": 0, "mrp": 0, "margin_in_stock": 0, "is_in_stock": 0, "sp": 0, "jit_eretail": 0 }
pas_url_v2 = "http://preprod-priceapi.nyk00-int.network/apis/v2/pas.get"
pas_url_v3 = "http://preprod-priceapi.nyk00-int.network/apis/v3/pas.get"
if socket.gethostname().startswith('admin'):
    pas_url_v2 = "http://prod-api.nyk00-int.network/apis/v2/pas.get"
    pas_url_v3 = "http://prod-api.nyk00-int.network/apis/v3/pas.get"


def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def compare_results(dict1={}, dict2={}):
    diff_list = []
    for key1, value1 in dict1.items():
        diff_dict = {}
        diff_dict["sku"] = key1
        diff_dict["type"] = value1.get("type")
        value2 = dict2.get(key1, {})
        diff_added = False

        for field in fields_to_check:
            if value1.get(field) != value2.get(field):
                diff_map[field] +=1
                diff_added = True
                diff_dict[field + '_v2'] = value1.get(field)
                diff_dict[field + '_v3'] = value2.get(field)
                if field == 'is_in_stock':
                    v2_quantity = int(value1.get('quantity'))
                    v3_quantity = int(value2.get('quantity'))
                    diff_dict['quantity_v2'] = v2_quantity
                    diff_dict['quantity_v3'] = v3_quantity
                    if abs(v2_quantity - v3_quantity) > margin:
                        diff_map['margin_in_stock'] += 1
        if diff_added:
            diff_list.append(diff_dict)
    return diff_list

def write_headers_to_csv_file():
    with open(diff_csv_filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=diff_csv_headers)
        writer.writeheader()

def write_to_csv_file(diff_list=[]):
    if diff_list:
        try:
            with open(diff_csv_filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=diff_csv_headers)
                for data in diff_list:
                    writer.writerow(data)
        except IOError:
            print("I/O error")

mysql_conn = PasUtils.mysqlConnection("r")
if disabled_switch == 1:
    query = "SELECT sku,type from products where disabled=1"
else:
    query = "SELECT sku,type from products where disabled=0"
results = PasUtils.fetchResults(mysql_conn, query)

chunks = chunkify(results, CHUNK_SIZE)

write_headers_to_csv_file()

chunks_completed = 0
for chunk in chunks:
    request_data = {}
    request_data["products"] = chunk
    result_v2 = {}
    result_v3 = {}

    response_v2 = requests.post(url=pas_url_v2, json=request_data, headers={'Content-Type': 'application/json'})
    if (response_v2.ok):
      result_v2 = json.loads(response_v2.content.decode('utf-8')).get('skus')
    else:
        print("Couldn't obtain response for v2")
        print(chunk)

    response_v3 = requests.post(url=pas_url_v3, json=request_data, headers={'Content-Type': 'application/json'})
    if (response_v3.ok):
        result_v3 = json.loads(response_v3.content.decode('utf-8')).get('skus')
    else:
        print("Couldn't obtain response for v3")
        print(chunk)

    diff_list = compare_results(result_v2, result_v3)

    #insert into csv file
    if diff_list:
        write_to_csv_file(diff_list)
    chunks_completed = chunks_completed + 1
    print("CHUNKS_COMPLETED %s" % chunks_completed)

with open(diff_csv_filename, 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for row in reader:
        diff_exist = True
        number_of_rows = number_of_rows+1

if diff_exist:
    if disabled_switch == 1:
        Mail.send(MAIL_RECIPIENTS, "noreply@nykaa.com", "GetPAS v2 vs v3 Mismatch(Disabled Products)", "GetPAS v2 and v3 comparison contains diff.\nNo. of rows %s, \n map : %s " % (number_of_rows, str(diff_map)), diff_csv_filename, diff_csv_filename)
    else:
        Mail.send(MAIL_RECIPIENTS, "noreply@nykaa.com", "GetPAS v2 vs v3 Mismatch(Enabled Products)", "GetPAS v2 and v3 comparison contains diff.\nNo. of rows %s, \n map : %s " % (number_of_rows, str(diff_map)), diff_csv_filename, diff_csv_filename)

else:
    if disabled_switch == 1:
        Mail.send(MAIL_RECIPIENTS, "noreply@nykaa.com", "GetPAS v2 vs v3 Mismatch(Disabled Products)", "GetPAS v2 and v3 comparison contains no diff.")
    else:
        Mail.send(MAIL_RECIPIENTS, "noreply@nykaa.com", "GetPAS v2 vs v3 Mismatch(Enabled Products)", "GetPAS v2 and v3 comparison contains no diff.")













