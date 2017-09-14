from marrow.mailer import Mailer, Message

mailer = Mailer(dict(
        transport = dict(
                use = 'smtp',
                host = 'localhost')))
mailer.start()

message = Message(author="user@example.com", to="mayank@gludo.com")
message.subject = "Testing Marrow Mailer"
message.plain = "This is a test."
mailer.send(message)

mailer.stop()

