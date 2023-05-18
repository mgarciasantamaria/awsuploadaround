#!/usr/bin/env python
#_*_ codig: utf8 _*_
import smtplib, threading, sys, os
from email.message import EmailMessage 
from Modules.Constants import *

def Send_Mail(text, Subject):
    msg = EmailMessage()
    msg.set_content(text)
    msg['Subject'] = Subject
    msg['From'] = 'alarmas-aws@vcmedios.com.co'
    msg['To'] = ['ingenieriavod@vcmedios.com.co']
    conexion = smtplib.SMTP(host='10.10.130.217', port=25)
    conexion.ehlo()
    conexion.send_message(msg)
    conexion.quit()

#********** Clase para observar el porcentaje de Upload ****************#*
#***********************************************************************#*
class ProgressPercentage(object):                                       #*                                                                        
    def __init__(self, filename):                                       #*
        self._filename = filename                                       #*
        self._size = float(os.path.getsize(filename))                   #*
        self._seen_so_far = 0                                           #*
        self._lock = threading.Lock()                                   #*
                                                                        #*
    def __call__(self, bytes_amount):                                   #*
        # To simplify, assume this is hooked up to a single filename    #*
        with self._lock:                                                #*
            self._seen_so_far += bytes_amount                           #*
            percentage = (self._seen_so_far / self._size) * 100         #*
            sys.stdout.write(                                           #*
                "\r%s  %s / %s  (%.2f%%)" % (                           #*
                    self._filename, self._seen_so_far, self._size,      #*
                    percentage))                                        #*
            sys.stdout.flush()                                          #*
#***********************************************************************#*