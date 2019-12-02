
# coding: utf-8

# ### EXAMEN ETL
# 
# ##### Carlota Echevarría

# #### Primera pregunta: Describe brevemente que diferencia el persists, cache y collect en spark. Explica brevemente casos en los que es interesante su aplicación

# - Persists (persist() ): metodo para gaurdar resultados intermedios, se pueden usar varios niveles de almacenamiento.
# - Cache (cache() ): es un método para guardar resultados intermedios en la memoria.
# - Collect ( collect() ): se trata de una accion primitiva de Spark, vuelca la salida de RDD

# #### Segunda pregunta: Explica brevemente los pasos a seguir para realizar la carga de un conjunto de datos (pasos que se siguieron en la práctica con datos de logs)

# En primer lugar se deben cargar los datos en un contexto Spark ( from pyspark import SparkContext ), aignamos a una variable la IP y el proyecto que ejecuta la variable (ejemplo sc = SparkContext("local", "Proyecto").
# 
# En segundo lugar iniciamos la conexión con la IP, pero en nuestro caso nos encontramos en local, por lo que introduciremos la ruta del cojunto de datos, y establecemos la conexión en el contexto sc (ejemplo: datos= "./basededatos.gz" ; lectura = sc.textFile(datos) )

# #### Tercera Pregunta: Índica un tipo de problema que puede empeorar los datos. (pe. Que no exista un representante del CDO en todas las áreas de negocio), pon algún ejemplo específico (pe. Datos duplicados) y cómo lo tratarías con técnicas de data cleaning.

# Es muy común que haya problemas en la base de datos, por ejemplo:
#     
# - Celdas vacias: podemos reemplazar valores vacios de una columna por ejemplo con la siguiente funcion: transformer.replace_col
# - Valores duplicados: transformer.remove_duplicates
# 

# #### Cuarta tarea: Inicializar spark context y cargar los datos desde el fichero.

# In[1]:

from pyspark import SparkContext
sc = SparkContext("local", "First App")


# In[2]:

data_file = "./partidosLigaNBA.csv"
raw_data = sc.textFile(data_file)


# In[3]:

raw_data.take(5)


# In[4]:

header1 = raw_data.filter(lambda x: 'PTS' in x) #asiganmos el indice de las variables


# In[5]:

datos = raw_data.subtract(header1) #substraemos el header


# In[6]:

datos.take(5)


# In[7]:

from pprint import pprint
data_separar = datos.map(lambda x: x.split(":")) #realizamos la separación de las variables


# In[8]:

data_separar.take(5) #comprobamos


# #### Quinta tarea: Media de la diferencia de puntos por año

# In[9]:

#En la posición 4 están los puntos de local, #los puntos eran strings, al realizar el mapeo, los he convertido a númericos.
datos_puntos_local= data_separar.map(lambda x: int(x[6]))
datos_puntos_visit = data_separar.map(lambda x: int(x[4]))


# In[10]:

#Queremos realizar la media de la resta de ambas

datos_puntos_visit.take(10)



# In[49]:

datos_puntos_local.take(5)


# In[53]:

suma_visit = datos_puntos_visit.reduce(lambda a,b: a+b)
print(suma_visit)


# In[ ]:




# In[ ]:

#Se debe convertir la fecha, para poder disponer de los datos anuales. 


# #### Sexta tarea: ¿Han judado todos los equipos el mismo número de partidos? ¿ Si es qué no a que puede deberse?
# 
# No,no han jugado el mismo número de partidos, y además no han sido jugados el mismo número de partidos en campo local, o campo de visita.

# ### Preguntar: como utiñozar el reduce by Key

# In[19]:

datos_partidos1 = data_separar.map(lambda x: x[3], 1).reduceByKey
datos_partidos2 = data_separar.map(lambda x:x[5], 1)


# In[20]:

datos_partidos1.countByKey().items()


# In[14]:

datos_partidos2.countByKey().items()


# #### Séptima pregunta: ¿Cuantos partidos ha ganado en Enero Clevelant?
# 
# En local ha ganado 41 partidos.
# En visitante ha ganado 44 partidos.
# 
# En total ha ganado 85 partidos

# In[15]:

clevelant = data_separar.filter(lambda x:x[3] == 'Cleveland Cavaliers').filter(lambda x:'Jan' in x[0]).filter(lambda x: int(x[4])>int(x[6]))
clevelant.count()


# In[16]:

clevelant = data_separar.filter(lambda x:x[3] == 'Cleveland Cavaliers').filter(lambda x:'Jan' in x[0]).filter(lambda x: int(x[6])>int(x[4]))
clevelant.count()


# #### Octava pregunta: ¿Los Warrios son mejores fuera de casa o en casa?
# 
# Han ganado más partidos fuera de casa, en total 215 ganados en casa, y 225 fuera de casa.

# In[17]:

warriors_encasa = data_separar.filter(lambda x:x[3] == 'Golden State Warriors').filter(lambda x: int(x[4])>int(x[6]))
warriors_encasa.count()


# In[18]:

warriors_fueracasa = data_separar.filter(lambda x:x[3] == 'Golden State Warriors').filter(lambda x: int(x[6])>int(x[4]))
warriors_fueracasa.count()


# #### Novena pregunta: Equipo que ha quedado primerio en victorias más temporadas. (si es que hay alguno que más)

# ####  Décima pregunta: Escribe la expresión regular correcta que sólo macheen los teléfonos y el correo del siguiente texto.
# 
# Si eres cliente y necesitas información sobre tus posiciones, productos o realizar operaciones: Desde España. Desde el extranjero. Banca telefónica en castellano. Bandera castellano. 902 13 23 13. Banca telefónica en catalán. Bandera catalana. 902 88 30 08. Banca telefónica en inglés. Bandera inglesa. 902 88 88 35. O por correo electrónico a atencioncliente@bankinter.com

# In[ ]:

#Parseado del telefono: '^[(\d{3}) (\d{2}) (\d{2}) (\d{2})]' 
#Parseado del correo electronico : '^(\S+) (\S+)'


# In[23]:

sc.stop()

