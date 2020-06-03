import sys
import requests
import json
import csv

sys.path.append("/var/www/pds_api")
from pas.v3.utils import Utils as PasUtils

from utils.mailutils import Mail


CHUNK_SIZE = 100
pas_url_v2 = "http://preprod-priceapi.nyk00-int.network/apis/v2/pas.get"
pas_url_v3 = "http://preprod-priceapi.nyk00-int.network/apis/v3/pas.get"
diff_csv_headers = ["sku", "quantity_v2", "quantity_v3", "backorders_v2", "backorders_v3", "msp_v2", "msp_v3", "expdt_v2",
           "expdt_v3", "mrp_v2", "mrp_v3", "is_in_stock_v2", "is_in_stock_v3", "sp_v2", "sp_v3"]
diff_csv_filename = "diff_v2_v3.csv"
MAIL_RECEIPIENTS = "discovery-tech@nykaa.com"
diff_exist = False
number_of_rows = 0

def chunkify(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]

def compare_results(dict1={}, dict2={}):
    diff_list = []
    for key1, value1 in dict1.items():
        diff_dict = {}
        value2 = dict2.get(key1, {})

        dict1_dict2 = {k+'_v2': v for k, v in value1.items() if value1[k] != value2.get(k)}
        dict2_dict1 = {k+'_v3': v for k, v in value2.items() if value2[k] != value1.get(k)}
        if dict1_dict2 or dict2_dict1:
            diff_dict["sku"] = key1
            diff_dict.update(dict1_dict2)
            diff_dict.update(dict2_dict1)
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
query = "SELECT sku,type from products"
results = PasUtils.fetchResults(mysql_conn, query)

chunks = chunkify(results, CHUNK_SIZE)

write_headers_to_csv_file()

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

with open(diff_csv_filename, 'r') as csvfile:
    reader = csv.reader(csvfile)
    next(reader)
    for row in reader:
        diff_exist = True
        number_of_rows = number_of_rows+1

if diff_exist:
    Mail.send(MAIL_RECEIPIENTS, "noreply@nykaa.com", "GetPAS v2 and v3 comparison", "GetPAS v2 and v3 comparison contains diff.\nNo. of rows %s" % number_of_rows, diff_csv_filename, diff_csv_filename)
else:
    Mail.send(MAIL_RECEIPIENTS, "noreply@nykaa.com", "GetPAS v2 and v3 comparison", "GetPAS v2 and v3 comparison contains no diff.")













