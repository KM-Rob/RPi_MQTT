import paho.mqtt.client as mqtt
import MySQLdb as sql
from time import strftime

#Dane do polaczenia z baza danych
conn = sql.connect(host="localhost",
                   user="****",
                   passwd="****",
                   db="****")
global clients = ["ESP32_1", "ESP32_2", ""]
#Informacja o polaczeniu MQTT
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Polaczono z Brokerem")
    else:
        print("Nieudane polaczenie z Brokerem")

#Obsluga odebranych wiadomosci MQTT
def on_message(client, userdata, message):
    print("Odebrano: {}, temat: {}".format(str(message.payload), str(message.topic)))

    x = conn.cursor()   #Odniesienie do bazy danych

    #Zapytanie o aktualny czas
    if(message.topic == "GetTime"):
        #Ustalenie aktualnego czasu
        godzina = strftime('%H')
        minuty = strftime('%M')
        sekundy = strftime('%S')
        czas = ('{},{},{}').format(godzina, minuty, sekundy)
        #Wyslanie informacji o aktualnym czasie
        client.publish("SendTime", czas)
        print("Wyslano czas: {}".format(czas))
    else:
        if "kontraktron" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO Kontraktron (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))
        if "czujnikSwiatla" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO CzujnikSwiatla (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))
        if "MCP9808" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO MCP9808 (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))
        if "BME280" and "temperature" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO BME280_temperature (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))
        if "BME280" and "humidity" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO BME280_Humidity (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))
        if "BME280" and "presure" in message.topic():
            value, ID = [str(h) for h in message.payload.split(";")]
            insert_ = "INSERT INTO MCP9808 (ID_Czujnika,Wartosc) values (%s,%s)"
            print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            try:
                x.execute(insert_, (ID, value))
            except sql.Error as e:
                print("Error: {}".format(e))


def main():

    #Polaczenie MQTT
    Broker = "localhost"           #Ip Brokera
    client = mqtt.Client()         #Utworzenie clienta
    client.on_connect = on_connect #Informacja o polaczeniu MQTT
    client.connect(Broker)         #Polaczenie clienta z brokerem MQTT
    #Subskrypcja tematu MQTT
    for c in clients:
         client.subscribe('{}/services/kontraktron1'.format(c))
         client.subscribe('{}/services/kontraktron2'.format(c))
         client.subscribe('{}/services/kontraktron3'.format(c))
         client.subscribe('{}/services/czujnikSwiatla1'.format(c))
         client.subscribe('{}/services/czujnikSwiatla2'.format(c))
         client.subscribe('{}/services/czujnikSwiatla3'.format(c))
         client.subscribe('{}/services/czujnikSwiatla4'.format(c))
         client.subscribe('{}/services/MCP9808_1/temperature'.format(c))
         client.subscribe('{}/services/BME280_1/temperature'.format(c))
         client.subscribe('{]/services/BME280_1/humidity'.format(c))
         client.subscribe('{}/services/BME280_1/presure'.format(c))


    client.subscribe("BME280")
    client.subscribe("Temperatura")
    client.subscribe("Oswietlenie")
    client.subscribe("Glosnosc")
    client.subscribe("Akumulator")
    client.subscribe("GetTime")

    client.on_message = on_message #Odebranie wiadomosci i jej przetworzenie

    client.loop_forever() # Zapetlenie programu

main()
