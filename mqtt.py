import paho.mqtt.client as mqtt
import MySQLdb as sql
from time import strftime

#Dane do polaczenia z baza danych
conn = sql.connect(host="localhost", 
                   user="***",
                   passwd="***",
                   db="***")
                       
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
        #Wpisanie informacji do bazy danych dla BME680    
        if(message.topic == "BME680"):
            temp,wilg,cisn,gaz,ID = [str(h) for h in message.payload.decode("utf-8").split(',')]
            
            insert_temp = "INSERT INTO Temperatura (ID_Czujnika,Wartosc) values (%s,%s)"
            insert_wilg = "INSERT INTO Wilgotnosc (ID_Czujnika,Wartosc) values (%s,%s)"
            insert_cisn = "INSERT INTO Cisnienie (ID_Czujnika,Wartosc) values (%s,%s)"
            insert_gaz = "INSERT INTO Gaz (ID_Czujnika,Wartosc) values (%s,%s)"
            
            #Wykonanie polecen SQL
            try:
                x.execute(insert_temp,(ID,temp))
                x.execute(insert_wilg,(ID,wilg))
                x.execute(insert_cisn,(ID,cisn))
                x.execute(insert_gaz,(ID,gaz))
                conn.commit()  #Zatwierdzenie dokonanych zmian 
                print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            except sql.Error as e:
                print("Error: {}".format(e))
                
        #Wpisanie informacji do bazy danych w przypadku braku BME680
        else:
            wiadomosc,ID = [str(h) for h in message.payload.decode("utf-8").split(',')]
        
            if(message.topic == "Temperatura"):
                insert_ = "INSERT INTO Temperatura (ID_Czujnika,Wartosc) values (%s,%s)"
            if(message.topic == "Oswietlenie"):
                insert_ = "INSERT INTO Oswietlenie (ID_Czujnika,Wartosc) values (%s,%s)"
            if(message.topic == "Kontaktron"):
                insert_ = "INSERT INTO Kontaktron (ID_Czujnika,Wartosc) values (%s,%s)"
            
            #Wykonanie polecen SQL
            try:
                x.execute(insert_,(ID,wiadomosc)) 
                conn.commit()  #Zatwierdzenie dokonanych zmian 
                print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            except sql.Error as e:
                print("Error: {}".format(e))

def main():
    #Polaczenie MQTT
    Broker = "localhost"           #Ip Brokera
    client = mqtt.Client()         #Utworzenie clienta
    client.on_connect = on_connect #Informacja o polaczeniu MQTT
    client.connect(Broker)         #Polaczenie clienta z brokerem MQTT
 
    #Subskrypcja tematu MQTT
    client.subscribe("BME680")
    client.subscribe("Temperatura")
    client.subscribe("Oswietlenie")
    client.subscribe("Kontaktron")
    client.subscribe("GetTime")
    
    client.on_message = on_message #Odebranie wiadomosci i jej przetworzenie
    
    client.loop_forever() # Zapetlenie programu

main()
