#!/usr/bin/python

import sys, smtplib
from email.MIMEText import MIMEText

smtp_server=sys.argv[1]
smtp_port=sys.argv[2]
sender=sys.argv[3]
recipients=sys.argv[4] # ','-separated
subject=sys.argv[5]
content=sys.argv[6]

message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
""" % (sender, recipients.replace(",", ", "), subject, content)

print message

try:
	server = smtplib.SMTP(smtp_server, smtp_port)
	server.ehlo()
	server.sendmail(sender, recipients.split(','), message)
	server.close()
	print "successfully sent the mail"
except (RuntimeError):
	print "failed to send mail: %s" % RuntimeError

