
# coding: utf-8

# ### Resolución en SQL LogNasa
# ###### Carlota Echevarría

# #### 1. Parsear el dataset para su tratamiento

# Cargamos los datos en RDD: raw_Data

# In[1]:

from pyspark import SparkContext
sc = SparkContext("local", "First App")


# In[4]:

data_file = "./apache.access.log_small"
raw_data = sc.textFile(data_file)


# Comprobamos que los datos han sido correctamente cargados.

# In[5]:

raw_data.take(5)


# A continuación se realizara el parseado, para dar formato a nuestra estructura.

# In[6]:

import re # libreria que sirve pra aplicar expresiones regulares 
# search: funcion del paquete re. 
def parse_log1(line):
    match = re.search('^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*(\S+)\s*(\S+)\s*([\w\.\s*]+)?\s*"*(\d{3}) (\S+)', line)
    if match is None:
        return 0
    else:
        return 1
n_logs = raw_data.count()


# In[7]:

example_rd = raw_data.take(4)


# In[8]:

example_rd


# Las expresiones regulares se inician con comillas simples,
#  - ^ la estructura que tiene que cumplirse desde el inicio. 
#  - \S+ cadena de texto que no sea espacio.
#  - [] indica que de aparecer algo, aparecería alguno de lo elementos dentro del corchete. 
#  - \d{4} significa que aparecen exactamente cuatro dígitos.
#  - Los parantesis para lo único que sirve es para definir los bloques que luego utiliza para realizar grupos.
#  - \s* signifca que puede haber o no espacio.
#  - \w incluye solo las letras.
#  -\d{3} coge los tres numeros.
#  - D, todo lo que no es digito.
#  -s es espacio.
#  - (*) puede haber caracter y puede aparecer varias veces.

# In[13]:

rexp = '^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*(\S+)\s*(\S+)\s*([/\w\.\s*]+)?\s*"* (\d{3}) (\S+)'
example_rd[0]


match = re.search(rexp, example_rd[0])


# In[14]:

match


# In[15]:

match.groups()


# In[16]:

def parse_log2(line):
    match = re.search('^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*(\S+)\s*(\S+)\s*([/\w\.\s*]+)?\s*"* (\d{3}) (\S+)',line)
    if match is None:
        match = re.search('^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*([/\w\.]+)>*([\w/\s\.]+)\s*(\S+)\s*(\d{3})\s*(\S+)',line)
    if match is None:
        return (line, 0)
    else:
        return (line, 1)


# In[17]:

def map_log(line):
    match = re.search('^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*(\S+)\s*(\S+)\s*([/\w\.\s*]+)?\s*"* (\d{3}) (\S+)',line)
    if match is None:
        match = re.search('^(\S+) (\S+) (\S+) \[(\S+) [-](\d{4})\] "(\S+)\s*([/\w\.]+)>*([\w/\s\.]+)\s*(\S+)\s*(\d{3})\s*(\S+)',line)
    return(match.groups()) # match.groups() es una lista de python
parsed_rdd = raw_data.map(lambda line: parse_log2(line)).filter(lambda line: line[1] == 1).map(lambda line : line[0])
parsed_def = parsed_rdd.map(lambda line: map_log(line))


# In[74]:

def convert_long(x):
    x = re.sub('[^0-9]',"",x) 
    if x =="":
        return 0
    else:
        return int(x)
parsed_def.map(lambda line: convert_long(line[-1])).stats()


# In[19]:

raw_data.map(lambda line: parse_log2(line)).take(3)


# A continuación calculamos el número de hosts distintos:

# In[21]:

parsed_def.map(lambda line: line[0]).distinct().count()


# #### 2. Transformar en un sqldataframe

# In[75]:

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)


# In[76]:

from pyspark.sql import Row
from datetime import datetime



# In[77]:

sql_data = parsed_def.map(lambda p: (Row(Host = p[0], 
                            Date = datetime.strptime(p[3][:11], "%d/%b/%Y"),
                            Endpoint = p[6], Code = p[-2],
                            Size = p[-1]))) #de nuestra lista al primer elemento lo vamos a llamar host
#esto es un RDD con formato Row


# In[78]:

sql_data.take(2)


# In[79]:

lognasa_df = sqlContext.createDataFrame(sql_data)
lognasa_df.registerTempTable("datos")


# In[80]:

df = sqlContext.sql(""" SELECT * FROM datos LIMIT 30""")
df.show()


# #### 3. Responder las preguntas usando lenguaje sql sobre spark

# 3.1 Minimo, Máximo y Media del tamaño de las peticiones(size)

# In[81]:

metrica = sqlContext.sql(""" 
    SELECT MIN(Size) AS Minimo, MAX(Size) AS Maximo, ROUND(AVG(Size),2) AS Media 
    FROM datos 
    """)
metrica.show()


# 3.2 Número de peticiones de cada código de respuesta(response_code)

# In[103]:

npeticiones = sqlContext.sql(""" 
    SELECT COUNT(Size) AS Numero, Code AS Codigo
    FROM datos GROUP BY Code""")
npeticiones.show()


# 3.3 Mostrar 20 hosts que han sido visitados más de 10 veces

# In[149]:

hostvisitados = sqlContext.sql("""
   SELECT COUNT(Host) AS Visitas, Host
   FROM datos
   GROUP BY Host
   ORDER BY COUNT (Host) DESC
   LIMIT 20
""")
hostvisitados.show()


# 3.4 Mostrar los 10 endpoints más visitados

# In[142]:

endmasvisitados = sqlContext.sql("""
SELECT COUNT(Endpoint) AS Visitas, Endpoint
FROM datos
GROUP BY Endpoint
ORDER BY COUNT(Endpoint)DESC
LIMIT 10""")

endmasvisitados.show()


# 3.5 Mostrar los 10 endpoints más visitados que no tienen código de respuesta = 200

# In[160]:

resultado = sqlContext.sql("""SELECT COUNT(Endpoint) AS Visitas, Endpoint
FROM datos
WHERE Code != 200
GROUP BY Endpoint
ORDER BY COUNT(Endpoint)DESC
LIMIT 10""")

resultado.show()


# 3.6 Calcular el número de hosts distintos

# In[134]:

hostdistintos=sqlContext.sql("""SELECT COUNT(DISTINCT Host) AS Distintos_Host
FROM datos""")

hostdistintos.show()


# 3.7 Contar el número de hosts únicos cada día

# In[ ]:

#La base de datos solo dispone de los datos de un día, por lo que el código es el mismo que el anterior
hostdistintos=sqlContext.sql("""SELECT COUNT(DISTINCT Host) AS Distintos_Host
FROM datos""")

hostdistintos.show()


# 3.8 Calcular la media de peticiones diarias por host

# In[168]:

peticioneshost= sqlContext.sql("""
    SELECT AVG(Size) AS Peticiones, Host, Date
    FROM datos
    GROUP BY Date,Host
""")

peticioneshost.show()


# 3.9 Mostrar una lista de 40 endpoints distintos que generan código de respuesta = 404

# In[172]:

endpoints40= sqlContext.sql("""
    SELECT DISTINCT Endpoint AS Endpoints, Code AS Codigo
    FROM datos
    WHERE Code = 404
    LIMIT 40
""")
endpoints40.show()


# 3.10 Mostrar el top 25 de endpoints que más códigos de respuesta 404 generan

# In[180]:

top25endpoints= sqlContext.sql("""
    SELECT endpoint, COUNT(Code)
    FROM datos
    WHERE code = 404
    GROUP BY Endpoint
    ORDER BY COUNT(Code)DESC""")

top25endpoints.show()


# 3.11 El top 5 de días que se generaron código de respuestas 404

# In[184]:

top5codigo= sqlContext.sql(""" 
    SELECT COUNT(Code), Date
    FROM datos
    WHERE Code = 404
    GROUP BY Date
    ORDER BY COUNT(Code)DESC
    LIMIT 5
    """)
top5codigo.show()


# El resultado es debido a que estamos utilizando la base de datos con un solo dia. Por lo tanto se generaron 22 códigos de respuesta 404

# In[185]:

sc.stop()

