#%% Import de librerias relevantes
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import make_interp_spline
#%% Funcion flip string
def flipArray(array):
    return array[::-1]
#%% Funcion que genera df de diferencia de tasas para todos los nodos
def DiferenciaNodos(df):
    df1 = df.copy()
    for nodo in df1:   
        if nodo == "Date":
            continue
        else:
            n = int(nodo[0:len(nodo)-1])
            df1[nodo] = df1[nodo].diff(n)
    return df1
#%% Funcion que genera los shocks independientes
def ShockIndependiente(df):
    VectorShock = []
    for nodo in df:
        if nodo == "Date":
            continue
        else:
            percentil = df[nodo].quantile(np.random.rand())
            average = df[nodo].mean()
            std = df[nodo].std()
            shock = (percentil - average) / std
        VectorShock.append(shock)
    return VectorShock
#%% Funcion que genera los shocks dependientes
def shockCorrelacionado(shockIndependiente, cholesky):
    shockCorrelacionado = []
    for nodo in cholesky:
        shockCorrelacionado.append(np.dot(shockIndependiente,nodo))
    return shockCorrelacionado
#%% Funcion que simula m veces la siguiente curva
def simulacionCurva(dfDiferencia, M = 1000):  
    arraySimulaciones = np.zeros(shape=(M,len(ultimaCurva)))
    for i in range(M):
        shockIndependiente = np.array(ShockIndependiente(dfDiferencia))
        shockCorr = np.array(shockCorrelacionado(shockIndependiente, cholesky))
        curvaSimulada = np.add(ultimaCurva,shockCorr)
        arraySimulaciones[i] = flipArray(curvaSimulada)
    return arraySimulaciones
#%% Funcion que transforma las simulaciones para graficar smooth lines
def smoothLine(arraySimulaciones):
    smoothArray = np.zeros((len(arraySimulaciones),500))
    t = np.arange(12)*30+30
    Xline = np.linspace(t.min(), t.max(), 500)
    for i in range(len(arraySimulaciones)):
        XY_Spline = make_interp_spline(t, arraySimulaciones[i])
        Yline = XY_Spline(Xline)
        smoothArray[i] = Yline
    smoothArray = smoothArray.transpose()
    return Xline, smoothArray
#%% Input del modelo
dfInput = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/TasaFTP.xlsx")
ultimaCurva = dfInput.iloc[-1,1:].values
#%% Genero el un df con las diferencias de tasas
dfDiferencia = DiferenciaNodos(dfInput)
#%% Genera la matriz covarianza y de Cholesky
dfCovarianza = dfDiferencia.cov()
cholesky = np.linalg.cholesky(dfCovarianza).transpose()
#%% Simulo M veces la siguiente curva y generlo un array con todos los resultados
arraySimulaciones = simulacionCurva(dfDiferencia,25)
#%% Grafico para ver nodos simulados
Xline, smoothArray = smoothLine(arraySimulaciones)
plt.plot(Xline,smoothArray)
plt.xlim(30, 360)
plt.xticks(np.arange(30, 361, 30))
plt.ylabel("Tasa FTP")
plt.xlabel("Tiempo (días)")
plt.title("Simulación de Tasas de Interés")
plt.show()
