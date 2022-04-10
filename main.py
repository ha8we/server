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
    cursor.execute("SELECT `MAC`,`VL1`,`VL2`,`VL3` FROM `Device` WHERE 1")
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

def savesqlclientpwr(x,mac):
    cursor = mydb.cursor()

    if (len(x) == 17):
        try:
            I1 = 0
            I2 = 0
            I3 = 0
            if (mac[1] == 1):
                I1 = x[4]
            elif (mac[1] == 2):
                I2 = x[4]
            elif (mac[1] == 3):
                I3 = x[4]

            if (mac[2] == 1):
                I1 = x[5]
            elif (mac[2] == 2):
                I2 = x[5]
            elif (mac[2] == 3):
                I3 = x[5]

            if (mac[3] == 1):
                I1 = x[6]
            elif (mac[3] == 2):
                I2 = x[6]
            elif (mac[3] == 3):
                I3 = x[6]
            query = "INSERT INTO `"+mac[0]+"` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'" + \
                    x[0] + "','" + x[1] + "', '" + x[2] + "', '" + x[3] + "', '" + str(I1) + "', '" + str(I2) + "', '" + str(I3)+ "', '" + x[7] + "', '" + x[8] + "', '" + x[9] + "', '" + x[10] + "', '" + x[11] + "', '" + \
                    x[12] + "', '" + x[13] + "', '" + x[14] + "', '" + x[15] + "')"
            cursor.execute(query)
            mydb.commit()
            print(query)
        except:
            print("nem megfelelo uzenet"+query)

def updateclient(x,mac):
    cursor = mydb.cursor()
    I1=0
    I2 = 0
    I3 = 0
    if (mac[1]==1):
        I1=x[4]
    elif (mac[1]==2):
        I2 = x[4]
    elif (mac[1] == 3):
        I3 = x[4]

    if (mac[2]==1):
        I1=x[5]
    elif (mac[2]==2):
        I2 = x[5]
    elif (mac[2] == 3):
        I3 = x[5]

    if (mac[3]==1):
        I1=x[6]
    elif (mac[3]==2):
        I2 = x[6]
    elif (mac[3] == 3):
        I3 = x[6]

    if (len(x) == 17):
        try:
            query ="UPDATE `Device` SET `I1` = '" + str(I1) +"' ,`I2` = '" + str(I2)+"' ,`I3` = '" + str(I3) +"' WHERE  `MAC`='" + str(x[0])+"'"
            cursor.execute(query)
            mydb.commit()

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
def getcharge(s):
    cursor = mydb.cursor()
    cursor.execute("SELECT `MAC`,`"+str(s) +"`,`Priority` FROM `Device` WHERE `Charging`=1")
    result = cursor.fetchall()
    return result

def PWRcontrol():
    cursor = mydb.cursor()
    try:
        query = "SELECT `Max I1`,`Max I2`,`Max I3`,`I1`,`I2`,`I3`,`LastData`, `Tartalek`  FROM `System` WHERE 1"
        cursor.execute(query)
        result = cursor.fetchall()
        mydb.commit()
        tmp=result[0]
        I1max=tmp[0]
        I2max = tmp[1]
        I3max = tmp[2]
        I1 = tmp[3]
        I2 = tmp[4]
        I3 = tmp[5]
        tart=tmp[7]
        tart = 0.9
        print("A maximális áramok I1:" +str(tmp[0])+ " I2: " + str(tmp[1]) +" I3: " + str(tmp[2]))
        print("Aktuális áramok I1:" + str(I1) + " I2: " + str(I2) + " I3: " + str(I3))
        TI1=0
        TI2=0
        TI3=0
        dI1=0
        dI2=0
        dI3=0
        szummprio=0
        if I1>(I1max *tart):    #be kell avatkozni
            aktolt=getcharge("I1")
            db= len(aktolt)
            for y in range(0,db):
                tmp=aktolt[db-1]
                TI1=TI1+int(tmp[1]) #töltők áramösszege
                szummprio= szummprio+int(tmp[2])
                print(int(TI1))
            dI1=  I1-TI1
            dI1= (I1max*tart)-dI1-3 #kicsit lőjünk alá
                   #szabad szétosztható áramok
            print(TI1,  dI1, szummprio, db)
            tmp1=dI1/szummprio
            if tmp1 >= 6:
                print("jeeeee")
                for y in range(0,db):
                    tmp=aktolt[db-1]
                    print(tmp[0])
                    client.publish("/charger/control", str(tmp[0])+";"+ str(int(tmp[2]*tmp1)))
                for y in range(0, db): #database max update
                    tmp = aktolt[db - 1]
                    print(tmp[0])
                    imaxsqlupdate(str("I1max"),str(int(tmp[2]*tmp1)) , str(tmp[0]))
                   

    except:
        print("HIBA+++++++++++ " + query)

def imaxsqlupdate(L,I,MAC):
    cursor = mydb.cursor()
    try:
        query ="UPDATE `Device` SET `"+L+"`= "+I + " WHERE `MAC`= '" + str(MAC)+ "'"
        cursor.execute(query)
        mydb.commit()
        print("SIKERES I FRISSITÉS"+query)
    except:
        print("HIBA " + query)

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        payload = msg.payload.decode()
        x = payload.split(";")
        if x[0] == MacPowermeter:
            savesqlpwrmeter(x)
            PWRcontrol()
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
                        savesqlclientpwr(x,mac)
                        updateclient(x,mac)
                        print(mac)
                        print(x[0])


    client.subscribe(TOPIC)
    client.subscribe(TOPIC1)
    client.on_message = on_message

client = connect_mqtt()
def run():

    subscribe(client)
    client.publish("house/light", "ON")
    client.loop_forever()


if __name__ == '__main__':
    run()
