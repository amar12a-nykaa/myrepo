import sys
import ftplib

sys.path.append("/home/ubuntu/nykaa_scripts/utils")
sys.path.append("/home/hadoop/nykaa_scripts/utils")
from s3utils import S3Utils

class FTPUtils:

    def get_handler():
        ftp = ftplib.FTP('52.220.4.21')
        ftp.login('omniture', 'C9PEy2H8TEC2')
        ftp.set_pasv(True)
        return ftp

    def sync_ftp_data(files_prefix, bucket_name, key_dir, other_files=[]):
        ftp = FTPUtils.get_handler()
        files = other_files

        def fill_files(f):
            if f.startswith(files_prefix):
                files.append(f)

        ftp.retrlines('NLST', fill_files)
        csvs = list(map(lambda f: f.replace('zip', 'csv'), files))

        s3_csvs = S3Utils.ls_file_paths(bucket_name, key_dir)

        s3_csvs = [s[s.rfind("/") + 1:] for s in s3_csvs]
        csvs_needed_to_be_pushed = list(set(csvs) - set(s3_csvs))

        S3Utils.transfer_ftp_2_s3(ftp, list(map(lambda f: f.replace('csv', 'zip'), csvs_needed_to_be_pushed)), bucket_name, key_dir)

    def get_handler_search_ftp():
        ftp = ftplib.FTP('52.221.96.159')
        ftp.login('ftpuser', 'ftp4nykaa')
        ftp.set_pasv(True)
        return ftp

    def sync_ftp_data_from_search(files_prefix, bucket_name, key_dir, other_files=[]):
        ftp = FTPUtils.get_handler_search_ftp()
        files = other_files

        ftp.cwd(key_dir)

        def fill_files(f):
            #TODO: remove in prod
            if f.startswith(files_prefix) and "2019" in f:
                files.append(f)

        ftp.retrlines('NLST', fill_files)
        csvs = list(map(lambda f: f.replace('zip', 'csv'), files))

        s3_csvs = S3Utils.ls_file_paths(bucket_name, key_dir)

        s3_csvs = [s[s.rfind("/") + 1:] for s in s3_csvs]
        csvs_needed_to_be_pushed = list(set(csvs) - set(s3_csvs))

        S3Utils.transfer_ftp_2_s3(ftp, list(map(lambda f: f.replace('csv', 'zip'), csvs_needed_to_be_pushed)), bucket_name, key_dir)

