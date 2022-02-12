import paho.mqtt.client as mqtt
import MySQLdb as sql
from time import strftime

#Dane do polaczenia z baza danych
conn = sql.connect(host="localhost", 
                   user="****",
                   passwd="****",
                   db="****")
                       
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
        #Wpisanie informacji do bazy danych dla BME280    
        if(message.topic == "BME280"):
            temp,wilg,cisn,ID = [str(h) for h in message.payload.split(',')]
            
            insert_temp = "INSERT INTO Temperatura (ID_Czujnika,Wartosc) values (%s,%s)"
            insert_wilg = "INSERT INTO Wilgotnosc (ID_Czujnika,Wartosc) values (%s,%s)"
            insert_cisn = "INSERT INTO Cisnienie (ID_Czujnika,Wartosc) values (%s,%s)"
            
            #Wykonanie polecen SQL
            try:
                x.execute(insert_temp,(ID,temp))
                x.execute(insert_wilg,(ID,wilg))
                x.execute(insert_cisn,(ID,cisn))
                conn.commit()  #Zatwierdzenie dokonanych zmian 
                print("Wpisano do bazy danych: {}, temat: {}".format(str(message.payload), str(message.topic)))
            except sql.Error as e:
                print("Error: {}".format(e))
                
        #Wpisanie informacji do bazy danych w przypadku braku BME280
        else:
            wiadomosc,ID = [str(h) for h in message.payload.split(',')]
        
            if(message.topic == "Temperatura"):
                insert_ = "INSERT INTO Temperatura (ID_Czujnika,Wartosc) values (%s,%s)"
            if(message.topic == "Oswietlenie"):
                insert_ = "INSERT INTO Oswietlenie (ID_Czujnika,Wartosc) values (%s,%s)"
            if(message.topic == "Glosnosc"):
                insert_ = "INSERT INTO Glosnosc (ID_Czujnika,Wartosc) values (%s,%s)"
            
            #Wykonanie polecen SQL
            try:
                if(message.topic == "Akumulator"):
                    update_ = "UPDATE Akumulatory SET Status = %s WHERE ID_Urzadzenia = %s"
                    x.execute(update_,(wiadomosc,ID))
                else:
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
    client.subscribe("BME280")
    client.subscribe("Temperatura")
    client.subscribe("Oswietlenie")
    client.subscribe("Glosnosc")
    client.subscribe("Akumulator")
    client.subscribe("GetTime")
    
    client.on_message = on_message #Odebranie wiadomosci i jej przetworzenie
    
    client.loop_forever() # Zapetlenie programu

main()
