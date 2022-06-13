from paho.mqtt import client as mqtt_client
import mysql.connector
from time import time
import random
from datetime import datetime
import time
import logging
import sys

logging.basicConfig(filename='app.log',
                    level=logging.ERROR,
                    format = '%(levelname)s:%(asctime)s: %(message)s')


logging.info('start log')

logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


MQTT_HOST = '192.168.1.111'
#MQTT_HOST = 'ha8we.hu'
MQTT_PORT = 1883
client_id = f'python-mqtt-{random.randint(0, 100)}'
MQTT_USER = ''
MQTT_PASSWORD = ''
TOPIC = '/charger/powermeter/'
TOPIC1 = '/charger/Control/'

pauseguard=180

def Lastopt(x):
    global Lstopt
    Lstopt = x
def globChange(x):
    global Change
    Change = x

def Lastctrl(x):
    global Lstctrl
    Lstctrl = x
def Lastpwr(x):
    global Lstpwr
    Lstpwr = x

def mydbc():
    global mydb
    mydb = mysql.connector.connect(
     #   host="127.0.0.1",
        host="ha8we.hu",
        user="feri",
        password="mJtWrHciNkwwWZRS",
        database="Charger",
    )

def connectsql():
    conn = 0
    while conn == 0:
        try:
            mydbc()
            logging.info("Connected SQL")
            conn = 1
        except:
            time.sleep(2)
            logging.critical("Not connected SQL")
            conn = 0

def mysqlcheck():
    x = mydb.is_connected()
    while x!=True:

        connectsql()
        x = mydb.is_connected()


connectsql()
globChange(0)
Lastpwr(0)
Lastctrl(0)
Lastopt(0)
MacPowermeter = "ffeeddccbbaa"  # fogymĂ©rĹ‘ MAC ADRESS e8eb1be06e39;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;
#MacPowermeter = "e8eb1be06e39"




def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
        else:
            logging.critical("Failed to connect, return code %d\n", rc)

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
            logging.info(query)
        except:
            logging.error(query)
            mysqlcheck()

        try:
            query = "UPDATE `System` SET `I1` = "+ x[4] +" ,`I2` = "+ x[5]+" ,`I3` = "+ x[6] +" WHERE 1"
            cursor.execute(query)
            mydb.commit()
            #    print(x[0] + " PWR ref")
            logging.info(query)
        except:
            logging.error(query)
            mysqlcheck()

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
            query = "INSERT INTO `"+"chargerdata"+"` (`LastData`, `MAC`, `U12`, `U23`, `U31`, `I1`, `I2`, `I3`, `INe`, `Freq`, `PF`, `P`, `Q`, `S`, `Ph`, `Qh`, `Sh`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP),'" + \
                    x[0] + "','" + x[1] + "', '" + x[2] + "', '" + x[3] + "', '" + str(I1) + "', '" + str(I2) + "', '" + str(I3)+ "', '" + x[7] + "', '" + x[8] + "', '" + x[9] + "', '" + x[10] + "', '" + x[11] + "', '" + \
                    x[12] + "', '" + x[13] + "', '" + x[14] + "', '" + x[15] + "')"
            cursor.execute(query)
            mydb.commit()
            logging.info(query)
        except:

            logging.error(query)
            mysqlcheck()

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
            logging.info(query)
        except:
            logging.error(query)
            mysqlcheck()

def Readyclient(x,y):
    cursor = mydb.cursor()
    try:
        query ="UPDATE `Device` SET `Ready` = "+ str(y) +" WHERE  `MAC`='" + x+"'"
        cursor.execute(query)
        mydb.commit()
        logging.info(x+" Ready charging!r")
    except:
        logging.error(query)
        mysqlcheck()

def StartInvoice(mac):
    logging.error(" Invoice start:  "+str(mac))
    cursor = mydb.cursor()
    #utolsó pwr lekérdezése
    lastpower=0

    try:
        query = "SELECT `LastData`, `P`  FROM `chargerdata` where `MAC`='"+str(mac)+"' ORDER BY `LastData` DESC"
        cursor.execute(query)
        result = cursor.fetchall()
        mydb.commit()

        tmp = result[0]
        lastpower=tmp[1]

    except:
        lastpower=0


##kezelni kell majd a táblatörlés miatti adatvesztést


    try:


        query = "INSERT INTO `Invoice` (`Startime`, `MAC`, `Stoptime`, `Pstart`, `Pstop`, `AVGI`) VALUES (TIMESTAMP(CURRENT_TIMESTAMP), '"+str(mac)+"', NULL, '"+str(lastpower)+"', '0', '0')"
        cursor.execute(query)
        mydb.commit()
        logging.info(query)
    except:
        logging.error(query)
        mysqlcheck()


def StopInvoice(mac):
    logging.error(" Invoice STOP: "+str(mac))
    cursor = mydb.cursor()
    avgi=0
    starttime=0
    stoptime =0
    try:
        query = "SELECT `LastData`, `P`  FROM `chargerdata` where `MAC`='"+str(mac)+"' ORDER BY `LastData` DESC"
        cursor.execute(query)
        result = cursor.fetchall()
        mydb.commit()
        tmp = result[0]
        lastpower=tmp[1]
    except:
        lastpower=0
        mysqlcheck()

    try:
        query = "SELECT `Startime` FROM `Invoice` WHERE `MAC`='"+str(mac)+"' ORDER BY `Startime` DESC"
        cursor.execute(query)
        result = cursor.fetchall()
        mydb.commit()
        tmp = result[0]
        starttime=tmp[0]
    except:
        starttime=0
        logging.error(query)
        mysqlcheck()
    try:
        query = "UPDATE `Invoice` SET `Stoptime`= CURRENT_TIMESTAMP,`Pstop`='"+str(lastpower)+"',`AVGI`='0' WHERE `Startime`='"+str(starttime)+"' AND `MAC`='"+str(mac)+"'"
        cursor.execute(query)
        mydb.commit()
        logging.info(query)
    except:
        logging.error(query)
        mysqlcheck()

        #avgi kalkuláció
    """
   try:
       query = "SELECT `Startime`,`Stoptime` FROM `Invoice` WHERE `MAC`='"+str(mac)+"' ORDER BY `Startime` DESC"
       cursor.execute(query)
       result = cursor.fetchall()
       mydb.commit()
       tmp = result[0]
       starttime=tmp[0]
       stoptime=tmp[1]


   except:
       starttime=0
       print("bakker" + query)
   try:
       query = "UPDATE `Invoice` SET `Stoptime`= CURRENT_TIMESTAMP,`Pstop`='"+str(lastpower)+"',`AVGI`='0' WHERE `Startime`='"+str(starttime)+"' AND `MAC`='"+str(mac)+"'"
       cursor.execute(query)
       mydb.commit()
       print("je " + query)
   except:
       print("HIBA " + query)
    """

def Startclient(x,y):
    cursor = mydb.cursor()
    try:
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        dt_object = datetime.fromtimestamp(timestamp + 1800)
        if y == 1:
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            dt_object = datetime.fromtimestamp(timestamp + 20)
            query = "UPDATE `Device` SET `Charging` = " + str(y) + ",`chargestart` = CURRENT_TIMESTAMP ,`Pause` = 0, `Guardtime` = '"+str(dt_object)+"' WHERE  `MAC`='" + x + "'"
            cursor.execute(query)
            globChange(1)


        else:
            query = "UPDATE `Device` SET `I1` = '0',`Ready` = 0, `Charging` = " + str(y) + ",`chargestart` = NULL , `Pause` = 0 ,`Guardtime` = NULL WHERE  `MAC`='" + x + "'"
            cursor.execute(query)
            StopInvoice(x)
        mydb.commit()
        logging.info(x+" Ready flag = " +str(y))

    #    print(timestamp)                ###itt kell egy pwr management
    except:
        mysqlcheck()
        logging.error(query)


def deadcharger():
    cursor = mydb.cursor()
    try:
        query = "  SELECT `MAC`, `I1`, `Ready`, `Charging` FROM `Device` WHERE 1"
        cursor.execute(query)
        result = cursor.fetchall()
        mydb.commit()
        db = len(result)
       # print(db)
        for y in range(0, db):
            tmp = result[y]

            if((tmp[1]>0 ) and tmp[2]==0 and tmp[3]==0):


                Startclient(tmp[0], 0)
    except:

        mysqlcheck()

def getcharge(s):
    cursor = mydb.cursor()
    cursor.execute("SELECT `MAC`,`"+str(s) +"`, `Priority`FROM `Device` WHERE `Charging`=1 ORDER BY `chargestart`")
    result = cursor.fetchall()
    return result


def getpause(s):
    cursor = mydb.cursor()
    cursor.execute("SELECT `MAC`,`" + str(s) + "`,`Priority`, `Guardtime` FROM `Device` WHERE `Pause` = 1 ORDER BY `Guardtime`")
    result = cursor.fetchall()
    return result

def pause(tmp): #stop kĂĽldĂ©s mac /sql pause bejegyez(guardal i=0,charge=0)/
    client.publish("/charger/control/",tmp + ";Pause")
    cursor = mydb.cursor()
    try:
        now = datetime.now()
        timestamp = datetime.timestamp(now)
        dt_object = datetime.fromtimestamp(timestamp + pauseguard)
        query = "UPDATE `Device` SET `Charging` = 0" + ",`I1` = 0 ,`I2` = 0, `I3` = 0, `Pause` = 1, `Guardtime` = '"+str(dt_object)+"' WHERE  `MAC`='" + tmp + "'" #i1re figyelni

        cursor.execute(query)
        mydb.commit()
        logging.info(query)
    except:
        logging.error(query)
        mysqlcheck()

def PWRcontrol():
    connectsql()
    cursor = mydb.cursor()

    try:
        deadcharger()
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
        tart = 1
        logging.info("I1 limit: " +str(tmp[0])+ " I2: " + str(tmp[1]) +" I3: " + str(tmp[2]))
        logging.info("I1 actual:" + str(I1) + " I2: " + str(I2) + " I3: " + str(I3))
        TI1=0
        TI2=0
        TI3=0
        dI1=0
        dI2=0
        dI3=0
        szummprio=0
        pausech = getpause("I1")
        pdb = len(pausech)

        if (I1>((I1max *tart) -0)) or (Change ==1):    #be kell avatkozni
            globChange(0)
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            Lastctrl(timestamp + 3)
            aktolt=getcharge("I1")
            db= len(aktolt)

            for y in range(0,db):
                tmp=aktolt[y]
                TI1=TI1+int(tmp[1]) #tĂ¶ltĹ‘k ĂˇramĂ¶sszege
                szummprio= szummprio+int(tmp[2])


            dI1=  I1-TI1
            dI1= (I1max*tart)-dI1-0 #Szétoszthtó áramok
           # logging.info(str(TI1)+  str(dI1), str(szummprio), str(db))
            tmp1=dI1/szummprio    #elvben számolt szétosztható

            if tmp1 > 32:
                tmp1 = 32
            print("NEM TUDOM TEMP +++++++++++++++++++"+str(tmp1))


            if tmp1 >= 6:

                for y in range(db):
                    tmp=aktolt[y]
                    client.publish("/charger/control/", str(tmp[0])+";"+ str(int(tmp[2]*tmp1))+";")
                for y in range(0, db): #database max update
                    tmp = aktolt[y]
                    imaxsqlupdate(str("I1max"),str(int(tmp[2]*tmp1)) , str(tmp[0]))

                # elif I1>I1max:##itt kell kilĹ‘ni tĂ¶ltĂ¶t starttime alapjĂˇn SELECT `MAC`,`Guardtime` FROM `Device`  WHERE `Charging`=1 ORDER BY `chargestart`
            else:
                 #tĂ¶ltĹ‘Ăˇram kiszĂˇmolĂˇsa
                ndb=int((dI1/6))
                ndb=db-ndb
                for y in range(ndb):
                    tmp = aktolt[y]
                    logging.error("Paused charger: "+str(tmp[0]))

                    pause(str(tmp[0]))
                    globChange(1)
                #6 amperkikĂĽld
                    aktolt=getcharge("I1")
                    db = len(aktolt)
                    for y in range(db):
                        tmp = aktolt[y]

                        client.publish("/charger/control/", str(tmp[0]) + ";6")

                logging.info("Stop device number "+str(ndb))

        #elif ((I1)<((I1max *tart)-1) and  (I1)>((I1max *tart)-6)) :
        #else:
        elif(pdb>0):
            pausech = getpause("I1")
            pdb = len(pausech)

            if(pdb==0):     #mindenki tĂ¶lt

                now = datetime.now()
                timestamp = datetime.timestamp(now)
                if (Lstopt + 5) < timestamp:
                    Lastopt(timestamp)
                    globChange(1)
            else:

                aktolt = getcharge("I1")
                db = len(aktolt)
                if db > 0:
                    for y in range(0, db):
                        tmp = aktolt[y]
                        TI1 = TI1 + int(tmp[1])  # tĂ¶ltĹ‘k ĂˇramĂ¶sszege
                        szummprio = szummprio + int(tmp[2])

                    dI1 = I1 - TI1

                    dI1 = (I1max * tart) - dI1 - 0  # kicsit lĹ‘jĂĽnk alĂˇ                    # szabad szĂ©toszthatĂł Ăˇramok

                  #  logging.info(TI1, dI1, szummprio, db)
                    tmp1 = int(dI1 / 6)

                else:
                    tmp1=int(((I1max*tart)-I1)/6)

                if tmp1>pdb:
                    tmp1=pdb
                    logging.error("Paused charger nummber: LOG:  " + str(pdb))
                    logging.info("Resume charger number: " + str(tmp1))
                    print("NEM TUDOM TEMP +++++++++++++++++++" + str(tmp1))
                    pausech = getpause("I1")

                #visszatesszĂĽk a tĂ¶ltĹ‘ket

                for y in range(tmp1):   ######talán itt a hiba    tmp1 hordozza a visszatehető töltők számát
                    x=pausech[y]
                    now = datetime.now()
                    timestamp = datetime.timestamp(now)
                    guardtimestamp = datetime.timestamp(x[3])
                    if(timestamp>guardtimestamp):
                        Startclient(x[0], 1)
                        logging.error("Resume charger: "+ str(x))
                        tmp1=tmp1-1  #visszarakható töltők csökkentése
                        #feriketeszte
                        client.publish("/charger/control/", str(x[0]) + ";6;")
                        globChange(1)
                        ######Ha még védett a töltő de van visszakapcsolható akkor kezelni kell
                    ##itt kell szabad kapacitás feltét
                #if ((I1)<((I1max *tart)-5) and tmp1>0):

                # #idemég kell feltéttt
                print(int(dI1-(db*6)))
                if ( 5 < (dI1-(db*6)) and tmp1 > 0):
                    for y in range(tmp1):
                        x = pausech[y]
                        Startclient(x[0], 1)
                        globChange(1)
                        logging.error("FORCESART: " + str(x))

                    print("MARAD SZABAD KAPACITÁS    "+str(tmp1))
        elif (I1)<=((I1max *tart)-1) and pdb==0:
            globChange(1)
    except:

        mysqlcheck()

def imaxsqlupdate(L,I,MAC):
    cursor = mydb.cursor()
    try:
        query ="UPDATE `Device` SET `"+L+"`= "+I + " WHERE `MAC`= '" + str(MAC)+ "'"
        cursor.execute(query)
        mydb.commit()
        logging.info("Imax UPDATE")
    except:
        logging.error(query)
        mysqlcheck()

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        logging.info(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        payload = msg.payload.decode()
        x = payload.split(";")




        if x[0] == MacPowermeter:
            now = datetime.now()
            timestamp = datetime.timestamp(now)
            if (Lstpwr + 0) < timestamp:
                Lastpwr(timestamp)
                savesqlpwrmeter(x)

            if (Lstctrl) < timestamp:

                PWRcontrol()

        else:

            if x[1] == "START" : #itt mindenkit meg kell kĂ©rni egy stĂˇtuszra/vagy leszajuk Ă©s az utolsĂł adatok szerint jĂˇrunk el
                print(x[1])

            elif x[1] == "StartRequest":
                Readyclient(x[0], 1)
                Startclient(x[0], 1)
                StartInvoice(x[0])
                logging.warning("StartRequest charger:----------"+x[1])
            elif x[1] == "Stop":
                Readyclient(x[0], 0)
                Startclient(x[0], 0)
                logging.warning("Stop charger:----------" + x[0])
            else:
                device=getdevice(x)  #EZ MAJD kĂĽlsĹ‘ szĂˇl lesz
                                    #tĂ¶ltĂ©s


                try:
                    for y in range(0, len(device)):    #UPDATE `Device` SET `I1max` = 15 WHERE  `MAC`=112233445566
                        mac=device[y]

                        if mac[0]==x[0]:
                            savesqlclientpwr(x,mac)
                            updateclient(x,mac)

                except:
                    logging.info("itt crash")
                mysqlcheck()


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