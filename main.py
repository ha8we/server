from paho.mqtt import client as mqtt_client
import mysql.connector
from time import time
import random

MQTT_HOST = 'ha8we.hu'
MQTT_PORT = 1883
client_id = f'python-mqtt-{random.randint(0, 100)}'
MQTT_USER = ''
MQTT_PASSWORD = ''
TOPIC = '/charger/powermeter/'
TOPIC1 = '/charger/Control/'
mydb = mysql.connector.connect(
    host="ha8we.hu",
    user="feri",
    password="mJtWrHciNkwwWZRS",
    database="Charger"
)

MacPowermeter = "ffeeddccbbaa"  # fogymérő MAC ADRESS

print(mydb)



def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(client_id)
    client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connect(MQTT_HOST, MQTT_PORT)
    return client


"""
def on_message(mqtt_client, user_data, message):
    cursor = mydb.cursor()

    payload = message.payload.decode('utf-8')
    print(payload)
    x = payload.split(";")

  #  query = "INSERT INTO `PowerMeter` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'"+x[0]+"','"+x[1]+"', '"+x[2]+"', '"+x[3]+"', '"+x[4]+"', '"+x[5]+"', '"+x[6]+"', '"+x[7]+"', '"+x[8]+"', '"+x[9]+"', '"+x[10]+"', '"+x[11]+"', '"+x[12]+"', '"+x[13]+"', '"+x[14]+"', '"+x[15]+"')"
    try:
   #     cursor.execute(query)
        mydb.commit()
    except:
        print("nem megfelelo uzenet")


    print(query)
"""
def getdevice(x):
    cursor = mydb.cursor()
    cursor.execute("SELECT * FROM Device")
    result = cursor.fetchall()
    return result



def savesqlpwrmeter(x):
    cursor = mydb.cursor()
    if (len(x) == 17):
        try:
            query = "INSERT INTO `PowerMeter` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'" + \
                    x[0] + "','" + x[1] + "', '" + x[2] + "', '" + x[3] + "', '" + x[4] + "', '" + x[5] + "', '" + x[
                        6] + "', '" + x[7] + "', '" + x[8] + "', '" + x[9] + "', '" + x[10] + "', '" + x[11] + "', '" + \
                    x[12] + "', '" + x[13] + "', '" + x[14] + "', '" + x[15] + "')"
            cursor.execute(query)
            mydb.commit()
            print(query)
        except:
            print("nem megfelelo uzenet")

        try:
            query = "UPDATE `System` SET `I1` = "+ x[4] +" ,`I2` = "+ x[5]+" ,`I3` = "+ x[6] +" WHERE 1"
            cursor.execute(query)
            mydb.commit()
            print(x[0] + " PWR ref")
        except:
            print("HIBA " + query)

def savesqlclientpwr(x,y):
    cursor = mydb.cursor()

    if (len(x) == 17):
        try:
            query = "INSERT INTO `"+y+"` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'" + \
                    x[0] + "','" + x[1] + "', '" + x[2] + "', '" + x[3] + "', '" + x[4] + "', '" + x[5] + "', '" + x[
                        6] + "', '" + x[7] + "', '" + x[8] + "', '" + x[9] + "', '" + x[10] + "', '" + x[11] + "', '" + \
                    x[12] + "', '" + x[13] + "', '" + x[14] + "', '" + x[15] + "')"
            cursor.execute(query)
            mydb.commit()
            print(query)
        except:
            print("nem megfelelo uzenet"+query)

def updateclient(x):
    cursor = mydb.cursor()

    if (len(x) == 17):
        try:
            query ="UPDATE `Device` SET `I1` = "+ x[4] +" ,`I2` = "+ x[5]+" ,`I3` = "+ x[6] +" WHERE  `MAC`=" + x[0]
            cursor.execute(query)
            mydb.commit()
            print(query)
        except:
            print("nem megfelelo uzenet" + query)

def Readyclient(x,y):
    cursor = mydb.cursor()
    try:
        query ="UPDATE `Device` SET `Ready` = "+ str(y) +" WHERE  `MAC`=" + x[0]
        cursor.execute(query)
        mydb.commit()
        print(x[0]+" Töltésre kész! állapota")
    except:
        print("HIBA " +query)

def PWRcontrol():
    cursor = mydb.cursor()
    try:
        query = "UPDATE `Device` SET `Ready` = " + str(y) + " WHERE  `MAC`=" + x[0]
        cursor.execute(query)
        mydb.commit()
        print(x[0] + " Töltésre kész! állapota")
    except:
        print("HIBA " + query)
    print(x)


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        payload = msg.payload.decode()
        x = payload.split(";")
        if x[0] == MacPowermeter:
          #  PWRcontrol(x)
            savesqlpwrmeter(x)
        else:

            if x[1] == "START" : #itt mindenkit meg kell kérni egy státuszra/vagy leszajuk és az utolsó adatok szerint járunk el
                print(x[1])
            elif x[1] == "READY":
                Readyclient(x, 1)
                print("READy"+x[1])
            elif x[1] == "STOP":
                Readyclient(x, 1)
                print("STOP" + x[1])
            else:
                device=getdevice(x)  #EZ MAJD külső szál lesz
                                    #töltés
                print(len(device))
                for y in range(0, len(device)):    #UPDATE `Device` SET `I1max` = 15 WHERE  `MAC`=112233445566
                    mac=device[y]
                    if mac[0]==x[0]:
                        savesqlclientpwr(x,mac[0])
                        updateclient(x)
                        print(mac)
                        print(x[0])


    client.subscribe(TOPIC)
    client.subscribe(TOPIC1)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()


if __name__ == '__main__':
    run()