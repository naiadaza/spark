###############################################################################
#                                 BICIMAD_VARIOS
#        Se accede a varios ficheros y se obtiene la ruta más repetida 
#        en función del día de la semana en el que se ha realizado sin tener
#        en cuenta cada mes.   
###############################################################################


from pyspark.sql import SparkSession
from collections import Counter
import json
from pprint import pprint
from datetime import datetime
import sys

############################################################################################
#                              FUNCION DATOS

#En esta funcion obtenemos los datos del archivo json de manera: ((semana, hora), trayecto) 

#
############################################################################################

def datos(line): 
    data = json.loads(line)
    
    dia=''
    estacion_de_origen = data["idunplug_station"]
    estacion_de_destino = data["idplug_station"]
    #Ordenamos los recorridos lexicográficamente
    if estacion_de_origen < estacion_de_destino:
        estaciones = (estacion_de_origen,estacion_de_destino)
    else:
        estaciones = (estacion_de_destino,estacion_de_origen)
    fecha = datetime.strptime(data['unplug_hourTime']['$date'], "%Y-%m-%dT%H:%M:%S.%f%z")
    hora=fecha.hour
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


    return ((dia,hora),estaciones)


############################################################################################
#                              FUNCION REPETICIONES

#   
#   Para cada semana y hora ordena por los recorridos más usados y las veces que se repiten
#
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

#Escribe cada tupla como un string para luego copiarlo en el fichero de salida.

#
############################################################################################  

def pasar_a_string(lista):
    texto = ""
    for x in lista:
        texto = texto + "Hora: "+str(x[0])+ "  recorrido:  "+str(x[1][0])+" se realiza "+str(x[1][1])+ " veces.\n"
    return(texto)
   
    
    
def main(sc,files):
    rdd = sc.parallelize([])
    for file_name in files:        
        file_rdd = sc.textFile(file_name)
        rdd = rdd.union(file_rdd)        
    agrupados = rdd.map(datos).groupByKey() #Obtener lista de tuplas ((semana, hora),estaciones) y agrupar por semana y hora
    datos1 = agrupados.mapValues(repeticiones) #Obtener las repeticiones de cada recorrido para cada tipo de día y hora
    datos2 = datos1.mapValues(lambda x: x[0]) #Cogemos el más concurridoa
    #Agrupamos por días de la semana
    agrupa_dias = datos2.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().sortByKey().mapValues(list) #Ordenamos para agrupar por tipo de día y hora
    ordenado = agrupa_dias.mapValues(ordenar).mapValues(pasar_a_string).collect()
    z = ['Lunes','Martes','Miercoles','Jueves','Viernes','Sabado','Domingo','Fin']
    i=0
    booleano = True
    while booleano:
        for x in ordenado:
          semana = x[0]
          if semana == z[i]:
             print("-----------------------------------------------------")
             print( "                     " + semana + "                ")
             print("-----------------------------------------------------")
             print("")
             print(x[1])
             if z[i] == 'Domingo':
                 booleano = False
             i+=1
    
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