from contextlib import closing
import requests
import csv
import socket
import json
import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class ProductsMismatch():
    bundle_mismatched_skus = []
    configurable_mismatched_skus = []

    def __init__(self, url, mail_list, price_api_url):
        self.mail_list = mail_list
        self.url = url
        self.price_api_url = price_api_url

    def getMismatchedProducts(self):
        with closing(requests.get(self.url, stream=True)) as r:
            f = (line.decode('utf-8') for line in r.iter_lines())
            reader = csv.DictReader(f, delimiter=',', quotechar='"')
            count = 0
            products = []
            for row in reader:
                if row['type_id'] == 'bundle':
                    product_skus = (row['parent_sku'] or "").split('|')    if row['parent_sku'] else []
                    sku_id = row['sku'] if row['sku'] else ''
                    if sku_id:
                        self.findGludoBundleProducts(sku_id, product_skus)

                if row['type_id'] == 'configurable':
                    count+=1
                    sku_id = row['sku'] if row['sku'] else ''
                    if sku_id:
                        products.append({'sku': sku_id, 'type':'configurable'})
                if count == 100:
                    self.findGludoConfigurableProducts(products)
                    products = []
                    count =0
        self.sendMail()

    def findGludoConfigurableProducts(self,products):
        response = requests.post(self.price_api_url,data=json.dumps({"products":products}))
        pas_object = response.json()
        pas_object = pas_object.get('skus')
        for sku in pas_object:
            sku_obj = pas_object.get(sku,{})
            if sku_obj.get('mrp',0) == 0 or sku_obj.get('sp',0) == 0:
                self.configurable_mismatched_skus.append(sku)

    def findGludoBundleProducts(self, sku_id, product_skus):
        request_url = "{}?type=bundle&sku={}".format(self.price_api_url, sku_id)
        response = requests.get(request_url)
        pas_object = response.json()
        pas_object = pas_object.get('skus')
        if pas_object.get(sku_id):
            pas = pas_object.get(sku_id)
            bundle_products = pas.get('products', {})
            bundle_products_skus = set(bundle_products.keys())
            product_skus = set(product_skus)
            if bundle_products_skus - product_skus or product_skus - bundle_products_skus:
                self.bundle_mismatched_skus.append({'sku': sku_id, 'gludo_products': ",".join(bundle_products_skus),
                                                    'magento_products': ",".join(product_skus)})

    def sendMail(self):
        files_to_send = []
        if self.bundle_mismatched_skus:
            with open('mismatched_bundle_products.csv', 'w') as mismatched_bundle_products:
                csv_writer = csv.DictWriter(mismatched_bundle_products, fieldnames=['sku','gludo_products','magento_products'])
                csv_writer.writeheader()
                for index, product in enumerate(self.bundle_mismatched_skus):
                    csv_writer.writerow(product)
            files_to_send.append('mismatched_bundle_products.csv')
        if self.configurable_mismatched_skus:
            with open('mismatched_configurable_products.csv', 'w') as mismatched_bundle_products:
                csv_writer = csv.DictWriter(mismatched_bundle_products, fieldnames=['sku'])
                csv_writer.writeheader()
                for index, product in enumerate(self.configurable_mismatched_skus):
                    csv_writer.writerow(product)
            files_to_send.append('mismatched_configurable_products.csv')
        if files_to_send:
            Mail.send(self.mail_list, "noreply@nykaa.com", "Product Mismatch in Gludo and Magento", '',
                      files_to_send,files_to_send)


class Mail(object):

    HOST = "smtp.mandrillapp.com:587"
    USER_NAME = "sumit.bisai@nykaa.com"
    PASSWORD = "Q9bieZgKxOBYwkw5OUEavQ"

    @classmethod
    def send(cls, to, author, subject, message, filename=None, fileToSend=None):
        msg = MIMEMultipart()
        msg["From"] = author
        msg["To"] = to
        msg["Subject"] = subject

        if fileToSend is not None:
            for f in fileToSend:
                ctype, encoding = mimetypes.guess_type(f)
                if ctype is None or encoding is not None:
                    ctype = "application/octet-stream"
                maintype, subtype = ctype.split("/", 1)
                fp = open(f, "rb")
                attachment = MIMEBase(maintype, subtype)
                attachment.set_payload(fp.read())
                fp.close()
                encoders.encode_base64(attachment)
                attachment.add_header("Content-Disposition", "attachment", filename=f)
                msg.attach(attachment)
        msg.attach(MIMEText(message, "plain"))
        server = smtplib.SMTP(cls.HOST)
        server.starttls()
        server.login(cls.USER_NAME, cls.PASSWORD)
        server.sendmail(author, to.split(","), msg.as_string())
        server.quit()


if __name__ == "__main__":
    if socket.gethostname().startswith('admin'):
        product_mismatch = ProductsMismatch(url = "http://adminpanel.nyk00-int.network/media/feed/master_feed_gludo.csv",
                                            mail_list="cataloging@nykaa.com,rahil.khan@nykaa.com,discovery-tech@nykaa.com",
                                            price_api_url='http://priceapi.nyk00-int.network/apis/v2/pas.get')
    else:
        product_mismatch = ProductsMismatch(url='http://172.26.17.227:8080/media/feed/master_feed_gludo.csv',
                                            mail_list="rishi.kataria@nykaa.com,honey.lakhani@nykaa.com",
                                            price_api_url='http://preprod-priceapi.nyk00-int.network/apis/v2/pas.get')
    product_mismatch.getMismatchedProducts()
