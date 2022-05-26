# Importo librerias 
import pandas as pd
import numpy as np

def AnualADiario(Sim):
    SimTasasDiarias = []
    for i in range(len(Sim)):
        SimTasasDiarias.append((1 + Sim[i]) ** (1 / 360) - 1)
        if (5 - (i % 5) == 1):
            SimTasasDiarias.append(SimTasasDiarias[-1])
            SimTasasDiarias.append(SimTasasDiarias[-1])
    return SimTasasDiarias

def AcumulaTasas(Sim):
    SimTasasAcumuladas = [Sim[0]]
    for i in range(1,len(Sim)):
        SimTasasAcumuladas.append(((1+SimTasasAcumuladas[i-1]) * (1+Sim[i]))-1)
    return SimTasasAcumuladas

TasasSimuladas = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/TasasSimuladas.xlsx")
TasasSim = np.transpose(TasasSimuladas.values)
del TasasSimuladas

TasasDiarias = []
for Simulacion in TasasSim:
    TasasDiarias.append(AnualADiario(Simulacion))
del Simulacion
TasasDiarias = np.array(TasasDiarias)

TasasAcum = []
for Simulacion in TasasDiarias:
    TasasAcum.append(AcumulaTasas(Simulacion))
del Simulacion
TasasAcum = np.array(TasasAcum)