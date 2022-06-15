import numpy as np
import pandas as pd
from tqdm import tqdm

df = pd.read_excel("Triangulo.xlsx")

fechas = pd.to_datetime(df['Fecha']).dt.date.drop_duplicates().values

nodos = np.arange(len(fechas)-1)*30+30

Productos = df['Producto'].unique()

Desarrollos = {}

for producto in Productos:
    
    Montos = df.loc[df['Producto'] == producto].values[:,-1:]
    
    array = np.zeros(shape=(len(fechas),len(fechas)))
    
    for i in tqdm(range(len(fechas))):
        for j in range(len(fechas)):
            if i + j >= len(fechas):
                array[i,j] = np.nan
            else:
                array[i,j] = df["Montos"].values[j+i] / df["Montos"].values[i] - 1
        
    percentil = []
    for column in range(len(array)-1):
        percentil.append(np.nanpercentile(array[:,column+1],0.5))
    
    Desarrollos[producto] = percentil