'''
Cuando se realiza una simulación con funciones aleatorias, los resultados
seran únicos para esa simulación con lo cual hay que pensar en algún metodo
para replicar los resultados de la simulación en diversas instancias sin
utilizar una semilla al inicio, ya que esto generaría un patrón predecible
en la simulación.

Para esto se construyó este codigo a modo ejemplo donde se comienza por
almacenar el estado inicial de aleatoriedad en una variable utilizando la
función get_state() y posteriormente posteriormente restaurando el estado
inicial con la función set_state().
'''

# Importo numpy para generar las series aleatorias
import numpy as np

# Primero guardo el estado de aleatoriedad de numpy
estado_inicial = np.random.get_state()

# Genero la Serie1, una serie aleatoria de distribución normal como ejemplo.
Serie1 = np.random.normal(1000, 100, 5)
print(Serie1)

# Si genero la Serie2 sin recuperar el estado inicial de aleatoriedad
# la Serie2 sera distinta a la Serie1.
Serie2 = np.random.normal(1000, 100, 5)
print(Serie2)

# Con np.random.set_state puedo recuperar el estado inicial de aleatoriedad
np.random.set_state(estado_inicial)

# Genero una serie aleatoria de distribución normal y el resultado es igual
# a la serie aleatoria inicial
Serie3 = np.random.normal(1000, 100, 5)
print(Serie3)

print("Hello World!")