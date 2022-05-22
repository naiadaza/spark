
###############################################################################
#                                 bicimad 
#hemos realizado un analisis de los 5 viajes o rutas más frecuentes y los 5 
#viajes menos frecuentes de un mes en función de los días de lunes a viernes o 
#de sábado a domingo, es decir nos devolverá: top 5 viajes más frecuentes en un
#mes de lunes a viernes, top 5 viajes menos frecuentes en un mes de lunes a 
#viernes, top 5 viajes más frecuentes en un mes en s bado o domingo y top 5 
#viajes menos frecuentes en un mes en sábado o domingo.

###############################################################################

from pyspark.sql import SparkSession
from collections import Counter
import json
from pprint import pprint
from datetime import datetime

import sys

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


def datos(linea): 
    data = json.loads(linea)
    estacion_de_origen = data["idunplug_station"]
    estacion_de_destino = data["idplug_station"]
    if estacion_de_origen < estacion_de_destino:
        estaciones = (estacion_de_origen,estacion_de_destino)
    else:
        estaciones = (estacion_de_destino,estacion_de_origen)
        
    fecha = datetime.strptime(data['unplug_hourTime']['$date'], "%Y-%m-%dT%H:%M:%S.%f%z")
    dia = fecha.weekday()
    
    return (dia,estaciones)
    #Escribimos cada tupla como un string para luego copiarlo en el fichero de salida

def fin_de_semana(dia):
    if dia <= 4:
        semana = False
    else:
        semana = True
    return semana


def main(sc,filename):
    text = sc.textFile(filename).map(datos) #Obtener lista de tuplas ((semana, hora),estaciones)
    viajes = text.map(lambda x: ((x[1][0],x[1][1]),fin_de_semana(x[0]))).groupByKey().mapValues(Counter)
    viajes_entre_semana_top5 = viajes.map(lambda x: (x[0], x[1][False])).takeOrdered(5, lambda x: -x[1])
    viajes_fiesta_top5 = viajes.map(lambda x: (x[0], x[1][True])).takeOrdered(5, lambda x: -x[1])
    print("------------------------------------------------- ")
    print("TOP 5 TRAYECTOS MÁS FRECUENTES DE LUNES A VIERNES ")
    print("------------------------------------------------- ")
    for i in range(5):
        print("Se realiza el recorrido: ",viajes_entre_semana_top5[i][0], "veces: ", viajes_entre_semana_top5[i][1])
        print("")
    print("")
    print("-------------------------------------------------- ")
    print("TOP 5 TRAYECTOS MÁS FRECUENTES DE SÁBADO A DOMINGO ")
    print("-------------------------------------------------- ")
    for i in range(5):
        print("Se realiza el recorrido: ",viajes_fiesta_top5[i][0], "veces: ", viajes_fiesta_top5[i][1])
        print("")
    viajes_entre_semana_least5 = viajes.map(lambda x: (x[0], x[1][False])).takeOrdered(5, lambda x: x[1])
    viajes_fiesta_least5 = viajes.map(lambda x: (x[0], x[1][True])).takeOrdered(5, lambda x: x[1])
    print("--------------------------------------------------- ")
    print("TOP 5 TRAYECTOS MENOS FRECUENTES DE LUNES A VIERNES ")
    print("--------------------------------------------------- ")
    for i in range(5):
        print("Se realiza el recorrido: ",viajes_entre_semana_least5[i][0], "veces: ", viajes_entre_semana_least5[i][1])
        print("")
    print("")
    print("---------------------------------------------------- ")
    print("TOP 5 TRAYECTOS MENOS FRECUENTES DE SÁBADO A DOMINGO ")
    print("---------------------------------------------------- ")
    for i in range(5):
        print("Se realiza el recorrido: ", viajes_fiesta_least5[i][0], "veces: ", viajes_fiesta_least5[i][1])
        print("")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sc,sys.argv[1])
