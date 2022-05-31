import pandas as pd
import numpy as np
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
        arraySimulaciones[i] = (curvaSimulada)
    return arraySimulaciones
#%% Input del modelo
dfInput = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/TasaFTP.xlsx")
ultimaCurva = dfInput.iloc[-1,1:].values
#%% Genero el un df con las diferencias de tasas
dfDiferencia = DiferenciaNodos(dfInput)
#%% Genera la matriz covarianza y de Cholesky
dfCovarianza = dfDiferencia.cov()
matrizCovI = dfCovarianza.values
cholesky = np.linalg.cholesky(dfCovarianza)
cholesky = cholesky.transpose()
#%% Simulo M veces la siguiente curva y generlo un array con todos los resultados
arraySimulaciones = simulacionCurva(dfDiferencia)
