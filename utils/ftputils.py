import ftplib

class FTPUtils:

    def get_handler():
        ftp = ftplib.FTP('52.220.4.21')
        ftp.login('omniture', 'C9PEy2H8TEC2')
        ftp.set_pasv(True)
        return ftp
