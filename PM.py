import serial
from time import sleep
from sds011 import *
import MySQLdb as sql

# Do projektu wykorzystano biblioteke https://github.com/ikalchev/py-sds011
# Aby ja zainstalowac nalezy wpisac w terminalu ponizsze komendy w przestawionej kolejnosci:
#1. git clone https://github.com/ikalchev/py-sds011.git
#2. cd py-sds011
#3. sudo python setup.py install
# W razie pojawienia się problemow z utworzeniem komunikacjia za pomoca biblioteki serial,
# należy odinstalować biblioteki za pomoca komedny "sudo pip3 uninstall serial" oraz
# "sudo pip3 uninstall pyserial".
# Po odinstalowaniu bibliotek zainstalowac starsza biblioteke pyserial za pomoca komendy
# "pip3 install pyserial==3.3"
# Jesli pojawi sie probelm z zamknietym portem to nalezy go otworzyc
# "menu aplikacji" malnika w lewym gornym rogu, nastepnie wjesc w "preferencje",
# "konfiguracja Raspberry Pi", "interfejs" i dać na ON "gniazdo szeregowe"


# Utworzenie obiektu odpowiadajacego za komunikacje z czujnikiem oraz ustawienie czujnika,
# aby wystalal dane tylko kiedy zostanie oto poproszony. Wpisanie "False"powoduje wysylanie
# odczytu wartosci zanieczyszczenia w trybie ciaglym
sensor = SDS011("/dev/ttyUSB0",use_query_mode=False)

#Zadeklarowanie ID Czujnika
ID = 0;

#Dane do polaczenia z baza danych
conn = sql.connect(host="localhost", 
                   user="***",
                   passwd="***",
                   db="***")

while True:
	sensor.sleep(sleep=False) #wylaczenie trybu uspienia
	sleep(60) #czas na rozruch 
	pmt_2_5, pmt_10 = sensor.query() #odczyt wartosci zanieczyszczenia
	
	#wpisywanie do bazy danych
	try:
		y = conn.cursor()
		insert2_5 = "INSERT INTO ZanieczyszczeniePM2 (ID_Czujnika,Wartosc) values (%s,%s)"
		insert10 = "INSERT INTO ZanieczyszczeniePM10 (ID_Czujnika,Wartosc) values (%s,%s)"
		y.execute(insert2_5,(ID,pmt_2_5))
		conn.commit()
		print("Wpisano PM2.5 = " + str(pmt_2_5) + " do bazy danych")
		y.execute(insert10,(ID,pmt_10))
		conn.commit()
		print("Wpisano PM10 = " + str(pmt_10) + " do bazy danych")
	except sql.Error as e:
		print("Error: {}".format(e))
	
	# wlaczenie trybu uspienia. Tryb uspienia powoduje wylaczenie wentylatora oraz lasera
	sensor.sleep(sleep=True)
	sleep(9*60)
