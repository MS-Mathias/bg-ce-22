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

def TratamientoTasas(TasasSim):
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
    return TasasAcum

TasasSimuladas = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/TasasSimuladas.xlsx")
TasasSim = np.transpose(TasasSimuladas.values)
del TasasSimuladas
TasasSim = TratamientoTasas(TasasSim)
TasasSim = np.transpose(TasasSim)
TasasSimDF = pd.DataFrame(TasasSim)
TasasSimDF["Dias"] = np.arange(1,len(TasasSimDF)+1)

Activos = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Activos.xlsx")
Activos["Dias"] = (Activos["Vencimiento"] * len(TasasSimDF)) // 1
Activos.drop(['Vencimiento','Identificador'],axis=1,inplace=True)

ActivosSim = pd.merge(Activos.groupby(["Dias"]).sum(),
                       TasasSimDF,
                       how="left",
                       on="Dias")

for i in range(25):
    ActivosSim.iloc[:,i+2]=ActivosSim["Monto"]/(1+ActivosSim.iloc[:,i+2])












