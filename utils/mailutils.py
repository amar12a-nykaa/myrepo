import mimetypes
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


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
            ctype, encoding = mimetypes.guess_type(fileToSend)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"

            maintype, subtype = ctype.split("/", 1)

            if maintype == "text":
                fp = open(fileToSend)
                attachment = MIMEText(fp.read(), _subtype=subtype)
                fp.close()
            elif maintype == "image":
                fp = open(fileToSend, "rb")
                attachment = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
            else:
                fp = open(fileToSend, "rb")
                attachment = MIMEBase(maintype, subtype)
                attachment.set_payload(fp.read())
                fp.close()
                encoders.encode_base64(attachment)
            attachment.add_header("Content-Disposition", "attachment", filename=filename)
            msg.attach(attachment)
        msg.attach(MIMEText(message, "plain"))
        server = smtplib.SMTP(cls.HOST)
        server.starttls()
        server.login(cls.USER_NAME, cls.PASSWORD)
        server.sendmail(author, to.split(","), msg.as_string())
        server.quit()
