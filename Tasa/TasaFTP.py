#%% Import de librerias relevantes
import time
start = time.time()
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import make_interp_spline

#%% Funcion que extrae los dias de los nodos
def extractoNodos(N = 10):
    n = np.arange(120) * 30 + 30
    return n
#%% Funcion que genera df de diferencia de tasas para todos los nodos
def DiferenciaNodos(df):
    df1 = df.copy()
    for nodo in df1:   
        if nodo == "Date":
            continue
        else:
            n = int(nodo[0:len(nodo)-1])
            df1[nodo] = df1[nodo].diff(90)
    return df1

#%% Funcion que genera los shocks independientes
def shockIndependiente(df):
    VectorShock = []
    average = {}
    std = {}
    for nodo in df:
        if nodo == "Date":
            continue
        else:
            average[nodo] = df[nodo].mean()
            std[nodo] = df[nodo].std()
    for nodo in df:
        if nodo == "Date":
            continue
        else:
            percentil = df[nodo].quantile(np.random.rand())
            shock = (percentil - average[nodo]) / std[nodo]
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
        shockIndep = np.array(shockIndependiente(dfDiferencia))
        shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky))
        curvaSimulada = np.add(ultimaCurva,shockCorr)
        arraySimulaciones[i] = curvaSimulada
    return arraySimulaciones

#%% Funcion que transforma las simulaciones para graficar smooth lines
def smoothLine(arraySimulaciones,N=500):
    smoothArray = np.zeros((len(arraySimulaciones),N))
    t = np.arange(12)*30+30
    Xline = np.linspace(t.min(), t.max(), N)
    for i in range(len(arraySimulaciones)):
        XY_Spline = make_interp_spline(t, arraySimulaciones[i])
        Yline = XY_Spline(Xline)
        smoothArray[i] = Yline
    smoothArray = smoothArray.transpose()
    return Xline, smoothArray

#%% Funcion que genera tasas para todos los nodos
def interpolaTasas(dfSimulaciones,nodosTasas):
    Tasas = pd.DataFrame(columns=nodosTasas)
    for nodo in nodosTasas:
        if nodo in dfSimulaciones:
            Tasas[nodo] = dfSimulaciones[nodo]
    del nodo
    
    for i in range(len(nodosTasas)):
        if pd.isnull(Tasas[nodosTasas[i]])[0]:
            for j in range(i+1,len(nodosTasas)):
                if not pd.isnull(Tasas[nodosTasas[j]])[0]:
                    difTasas = Tasas[nodosTasas[j]] - Tasas[nodosTasas[i-1]]
                    difFechas = nodosTasas[j] - nodosTasas[i]
                    Tasas[nodosTasas[i]] = Tasas[nodosTasas[i-1]] + difTasas * (nodosTasas[i]-nodosTasas[i-1]) / difFechas
                    break
        else:
            continue
    return Tasas

#%% Input del modelo
dfInput = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/TasaFTP.xlsx")
ultimaCurva = dfInput.iloc[-1,1:].values
nodosTasas = extractoNodos(dfInput)
Caidas = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Caidas tasa.xlsx")

#%% Genero el un df con las diferencias de tasas
dfDiferencia = DiferenciaNodos(dfInput)

#%% Genera la matriz covarianza y de Cholesky
dfCovarianza = dfDiferencia.cov()
cholesky = np.linalg.cholesky(dfCovarianza)

#%% Simulo M veces la siguiente curva y generlo un array con todos los resultados
M = 1000
arraySimulaciones = simulacionCurva(dfDiferencia,M)
dfSimulaciones = pd.DataFrame(arraySimulaciones, columns=[30,90,180,360,540,720,1080,1800,2160,3600,1,2])

#%% Interpola tasas para tener la tasa de todas las fechas
Tasas = interpolaTasas(dfSimulaciones, nodosTasas)

#%% Grafico para ver nodos simulados
Xline, smoothArray = smoothLine(arraySimulaciones)
plt.plot(nodosTasas,Tasas.values.transpose())
plt.xlim(30, 3600)
plt.xticks(np.arange(0, 3601, 360))
plt.ylabel("Tasa FTP")
plt.xlabel("Tiempo (días)")
plt.title("Simulación de Tasas de Interés 360 días")
plt.show()

#%% 
CaidasPesos = Caidas[Caidas["Moneda"] == "ARS"]
CaidasDolar = Caidas[Caidas["Moneda"] == "USD"]
CaidaTest = CaidasPesos.loc[6]
for nodo in Tasas:
    Tasas[nodo] = CaidaTest[nodo] / (1 + Tasas[nodo]) ** (nodo / 360)

#%% Time report
end = time.time()
print(f'el codigo tarda {end - start} segundos en correr {M} simulaciones')
