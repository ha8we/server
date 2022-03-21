import paho.mqtt.client as mqtt
import mysql.connector
from time import time

MQTT_HOST = 'ha8we.hu'
MQTT_PORT = 1883
MQTT_CLIENT_ID = 'server'
MQTT_USER = ''
MQTT_PASSWORD = ''
TOPIC = '/charger/powermeter/'

mydb = mysql.connector.connect(
    host = "ha8we.hu",
    user = "feri",
    password = "mJtWrHciNkwwWZRS",
    database ="Charger"
)

print(mydb)


def on_connect(mqtt_client, user_data, flags, conn_result):
    mqtt_client.subscribe(TOPIC)


def on_message(mqtt_client, user_data, message):
    cursor = mydb.cursor()

    payload = message.payload.decode('utf-8')
    print(payload)
    x = payload.split(";")
    for i in x:
        print(i)
    query = "INSERT INTO `PowerMeter` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'"+x[0]+"','"+x[1]+"', '"+x[2]+"', '"+x[3]+"', '"+x[4]+"', '"+x[5]+"', '"+x[6]+"', '"+x[7]+"', '"+x[8]+"', '"+x[9]+"', '"+x[10]+"', '"+x[11]+"', '"+x[12]+"', '"+x[13]+"', '"+x[14]+"', '"+x[15]+"')"

    cursor.execute(query)
    mydb.commit()

    print(query)


def main():




    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)


    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_HOST, MQTT_PORT)
    mqtt_client.loop_forever()


main()
