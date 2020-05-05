import sys
import csv
import datetime
import boto3
from dateutil import tz
sys.path.append('/var/www/pds_api/')
from pas.v2.utils import Utils as PasUtils
sys.path.append("/nykaa/scripts/feed_pipeline")
from pipelineUtils import PipelineUtils


class GenerateDailyDiscountCsv:

    mysql_conn = PasUtils.mysqlConnection()
    file_header = ['sku', 'type', 'current_discount', 'next_discount', 'next_discount_start', 'next_discount_end']
    bucket_name = PipelineUtils.getBucketNameForDailyDiscounts()

    def getProductDiscounts(self):
        query = "SELECT sku, discount as current_discount,scheduled_discount as next_discount,schedule_start as next_discount_start,schedule_end as next_discount_end, type FROM products"
        results = PasUtils.fetchResults(self.mysql_conn, query)
        with open('daily_discounts.csv', 'w') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=self.file_header)
            csv_writer.writeheader()
            for index, product in enumerate(results):
                product = self.processDiscountForSku(product)
                csv_writer.writerow(product)
            csvfile.close()

    def getBundleDiscounts(self):
        query = """SELECT sku,discount as current_discount,scheduled_discount as next_discount,schedule_start as next_discount_start,schedule_end as next_discount_end FROM bundles"""
        results = PasUtils.fetchResults(self.mysql_conn, query)
        with open('daily_discounts.csv', 'a+') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=self.file_header)
            for index, product in enumerate(results):
                product["type"] = 'bundle'
                product = self.processDiscountForSku(product)
                csv_writer.writerow(product)
        csvfile.close()

    def processDiscountForSku(self, product):
        if not product['next_discount']:
            product['next_discount'] = 0.0
        if not product['current_discount']:
            product['current_discount'] = 0.0
        scheduled_discount = product["next_discount"]
        schedule_start = product["next_discount_start"]
        schedule_end = product["next_discount_end"]
        discount = product["current_discount"]
        current_datetime = datetime.datetime.utcnow()
        from_zone = tz.gettz("UTC")
        to_zone = tz.gettz("Asia/Kolkata")
        current_datetime = current_datetime.replace(tzinfo=from_zone)
        current_datetime = current_datetime.astimezone(to_zone)
        if schedule_start and schedule_end:
            if current_datetime >= schedule_start.replace(
                tzinfo=to_zone
        ) and current_datetime < schedule_end.replace(tzinfo=to_zone):
                product["current_discount"] = scheduled_discount
                product["next_discount"] = discount
                product["next_discount_start"] = schedule_end
                product["next_discount_end"] = None
            if current_datetime >= schedule_end(tzinfo=to_zone):
                product["next_discount"] = None
                product["next_discount_start"] = None
                product["next_discount_end"] = None
        return product


    def uploadToS3(self):
        s3 = boto3.resource('s3')
        response = s3.meta.client.upload_file('daily_discounts.csv', self.bucket_name, 'daily_discounts_{}.csv'.format(datetime.date.today().strftime('%Y-%m-%d')))
        response = s3.meta.client.upload_file('daily_discounts.csv', self.bucket_name,
                                  'daily_discounts.csv')


if __name__ == "__main__":
    generate_daily_discounts = GenerateDailyDiscountCsv()
    generate_daily_discounts.getProductDiscounts()
    generate_daily_discounts.getBundleDiscounts()
    generate_daily_discounts.uploadToS3()