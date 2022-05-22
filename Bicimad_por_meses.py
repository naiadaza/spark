
############################################################################################
#                                 bicimad 
#se devuelve un fichero donde se indican los recorridos más repetidos en función del mes 
#y el dı́a en los que se han realizado.
#
############################################################################################

from pyspark.sql import SparkSession
from collections import Counter
import json
from datetime import datetime
import sys

############################################################################################
#                              FUNCION DATOS

#En esta funcion obtenemos los datos del archivo json de manera: ((semana, hora, mes), trayecto) 

#
############################################################################################

def datos(line): 
    data = json.loads(line)
    #tuplas ordenadas lexicograficamente
    estacion_de_origen = data["idunplug_station"]
    estacion_de_destino = data["idplug_station"]
    if estacion_de_origen < estacion_de_destino:
        estaciones = (estacion_de_origen,estacion_de_destino)
    else:
        estaciones = (estacion_de_destino,estacion_de_origen)
        
    fecha = datetime.strptime(data['unplug_hourTime']['$date'], "%Y-%m-%dT%H:%M:%S.%f%z")
    hora = fecha.hour
    mes = fecha.month

    numero = fecha.weekday()
    if numero == 0:
        dia = 'Lunes'
    elif numero == 1:
        dia = 'Martes'
    elif numero == 2:
        dia = 'Miercoles'
    elif numero == 3:
        dia = 'Jueves'
    elif numero == 4:
        dia = 'Viernes'
    elif numero == 5:
        dia = 'Sabado'
    elif numero == 6:
        dia = 'Domingo'
        
    return ((dia,hora,mes),estaciones)

############################################################################################
#                              FUNCION REPETICIONES

#Cuenta el número de veces que se ha repetido el trayecto a lo largo del mes y 
#devuelve el número de veces que se ha repetido cada trayecto.

#
############################################################################################

def repeticiones(lista):
    return Counter(lista).most_common() 

############################################################################################
#                              FUNCION ORDENAR

#Función que ordena donde el parámetro de ordenación es la hora en la que se ha 
#realizado el trayecto.

#
############################################################################################

def ordenar(lista):
    return sorted(lista,key = lambda x: x[0])

############################################################################################
#                              FUNCION PASAR A STRING

#Escribe cada tupla como un string.

#
############################################################################################  
def pasar_a_string(lista):
    texto = ""
    for x in lista:
        texto = texto + "Hora: "+str(x[0])+"  recorrido:  "+str(x[1][0])+" se realiza " +str(x[1][1])+ " veces.\n"
    return(texto)
############################################################################################
#                              FUNCION PASAR MES

#Se obtiene el nombre de cada uno de los meses en función de la posición que ocupan en el calendario.

#
############################################################################################

def meses(numero):
    if numero == 1:
        mes = 'Enero'
    elif numero == 2:
        mes = 'Febrero'
    elif numero == 3:
        mes = 'Marzo'
    elif numero == 4:
        mes = 'Abril'
    elif numero == 5:
        mes = 'Mayo'
    elif numero == 6:
        mes = 'Junio'
    elif numero == 7:
        mes = 'Julio'
    elif numero == 8:
        mes = 'Agosto'
    elif numero == 9:
        mes = 'Septiembre'
    elif numero == 10:
        mes = 'Octubre'
    elif numero == 11:
        mes = 'Noviembre'
    else:
        mes = 'Diciembre'
    return mes

def main(sc,files):
    text = sc.parallelize([])
    for file_name in files:        
        file_text = sc.textFile(file_name)
        text = text.union(file_text)        
    tuplas =text.map(datos).groupByKey() #Obtener lista de tuplas ((semana, hora),estaciones) y agrupar por semana y hora
    reps = tuplas.mapValues(repeticiones) #Obtener las repeticiones de cada recorrido para cada tipo de día y hora
    mas_concurrido = reps.mapValues(lambda x: x[0]) #Cogemos el más concurrido
    datos1 = mas_concurrido.map(lambda x: ((x[0][2],x[0][0]),(x[0][1],x[1]))).groupByKey().sortByKey() #Ordenamos para agrupar por tipo de día y hora
    horas = datos1.mapValues(list).mapValues(ordenar)#Ordenamos por horas
    lista_final = horas.mapValues(pasar_a_string).collect()
    z = ['Lunes','Martes','Miercoles','Jueves','Viernes','Sabado','Domingo','Fin']
    t = [] 
    for elem in lista_final:
        mes = meses(elem[0][0])
        if mes not in t:
            t.append(mes)
    i=0
    j=0
    u=0
    booleano = True
    while booleano and j<len(t):
        for elem in lista_final:
          mes = meses(elem[0][0])
          semana = elem[0][1]
          if i<len(z) and j<len(t):
              if semana == z[i] and mes==t[j]:
                 print("-----------------------------------------------------")
                 print("                   MES: "+mes+"\n")
                 print("-----------------------------------------------------")
                 print("")
                 print("                    " +semana+"\n")
                 print(elem[1])
                 u+=1
                 if z[i]=='Domingo'and j==len(t)-1:
                     booleano = False
                 i+=1 
                 if u==7 and j<len(t):
                    j+=1
                    u=0
                    i=0
             
    
         
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        l = []
        for i in range(1,len(sys.argv)):
            l.append(sys.argv[i])

        spark = SparkSession.builder.getOrCreate()
        sc = spark.sparkContext
        main(sc,l)
