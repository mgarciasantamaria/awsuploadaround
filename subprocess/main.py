#!/usr/bin/env python
#_*_ codig: utf8 _*_
import smtplib, json
from email.message import EmailMessage 

if __name__ == '__main__':
    with open('../json/summary.json', 'r') as json_file:
        json_data=json.load(json_file)
    dict_summary_srt=json.dumps(json_data, sort_keys=True, indent=4) #Se transforma el diccionario a formato texto.
    msg = EmailMessage()
    msg.set_content(dict_summary_srt)
    msg['Subject'] = '"Sumary upload Around"'
    msg['From'] = 'alarmas-aws@vcmedios.com.co'
    msg['To'] = ['ingenieriavod@vcmedios.com.co']
    conexion = smtplib.SMTP(host='10.10.122.17', port=25)
    conexion.ehlo()
    conexion.send_message(msg)
    conexion.quit()
    json_data={}
    with open('../json/summary.json', 'w') as json_file:
        json.dump(json_data, json_file)