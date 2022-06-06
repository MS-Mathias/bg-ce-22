# %% Import de librerias relevantes


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
start = time.time()

# %% Funciones para las Simulacion de Tasas de Interes


def DiferenciaNodos(tasasInput):
    df1 = tasasInput.copy()
    for nodo in df1:
        if nodo == "Date":
            continue
        else:
            df1[nodo] = df1[nodo].diff(90)
    return df1

def parametrosSimulaciones(tasasInput, nodosTasas):
    dfDiferencia = DiferenciaNodos(tasasInput)
    
    dfCovarianza = dfDiferencia.cov()
    cholesky = np.linalg.cholesky(dfCovarianza)
    
    return dfDiferencia, dfCovarianza, cholesky

# %% Funciones que simulan las curvas y las interpola


def simulacionCurva(dfDiferencia, M=1000):
    arraySimulaciones = np.zeros(shape=(M, len(ultimaCurva)))
    for i in range(M):
        shockIndep = np.array(shockIndependiente(dfDiferencia))
        shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky))
        curvaSimulada = np.add(ultimaCurva, shockCorr)
        arraySimulaciones[i] = curvaSimulada
    return arraySimulaciones

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

def shockCorrelacionado(shockIndependiente, cholesky):
    shockCorrelacionado = []
    for nodo in cholesky:
        shockCorrelacionado.append(np.dot(shockIndependiente, nodo))
    return shockCorrelacionado

def interpolaTasas(dfSimulaciones, nodosTasas):
    Tasas = pd.DataFrame(columns=nodosTasas)
    for nodo in nodosTasas:
        if nodo in dfSimulaciones:
            Tasas[nodo] = dfSimulaciones[nodo]
    del nodo

    for i in range(len(nodosTasas)):
        if pd.isnull(Tasas[nodosTasas[i]])[0]:
            shift = 0
            for j in range(i+shift+1, len(nodosTasas)):
                if not pd.isnull(Tasas[nodosTasas[j]])[0]:
                    a = nodosTasas[i] - nodosTasas[i-1]
                    b = nodosTasas[j] - nodosTasas[i]
                    f = a / (a + b)
                    x = (Tasas[nodosTasas[j]] ** f) * \
                        (Tasas[nodosTasas[i-1]] ** (1 - f))
                    Tasas[nodosTasas[i]] = x
                    break
        else:
            shift = 0
            continue
    return Tasas

# %% Funciones que actualizan las caidas por sus respectivas tasas.


def CapitalEconomico(Caidas, Tasas, Moneda, M = 1000):
    A, P = separaCaidas(Caidas, Moneda)
    
    ActivosActualizados = actualizaSimulaciones(A, Tasas.values, M)
    PasivosActualizados = actualizaSimulaciones(P, Tasas.values, M)
    
    Resultados = netoSimulaciones(ActivosActualizados, PasivosActualizados, M)
    
    CE = np.percentile(Resultados,99.9)
    return CE

def separaCaidas(Caidas, moneda):
    Activos = Caidas.fillna(0)[(Caidas["Moneda"] == moneda) & (
        Caidas["LugarBalance"] == "Activo")].values[:, 4:]
    Pasivos = Caidas.fillna(0)[(Caidas["Moneda"] == moneda) & (
        Caidas["LugarBalance"] == "Pasivo")].values[:, 4:]
    return Activos, Pasivos

def actualizaSimulaciones(arrayCaidas, arrayTasas, M=1000):
    Resultados = []
    for i in range(M):
        Resultados.append(actualizaCartera(arrayCaidas, arrayTasas[i]))
    return Resultados

def actualizaCartera(arrayCaidas, Tasas):
    ValorCartera = 0
    for Caida in arrayCaidas:
        ValorCartera += actualizaCaida(Caida, Tasas)
    return ValorCartera

def actualizaCaida(Caida, Tasas):
    ValorActual = 0
    for i in range(len(Tasas)):
        ValorActual += Caida[i] / (1+Tasas[i]) ** ((i*30+30) / 360)
    return ValorActual

def netoSimulaciones(Activo, Pasivo, M=1000):
    ResultadoNeto = []
    for i in range(M):
        ResultadoNeto.append(Activo[i] - Pasivo[i])
    return ResultadoNeto

# %% Inputs del modelo


Caidas = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Caidas tasa.xlsx")
tasasInput = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/ActivosFTP.xlsx")
ultimaCurva = tasasInput.iloc[-1, 1:].values
nodosTasas = np.arange(120) * 30 + 30

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados


dfDiferencias, dfCovarianza, cholesky = parametrosSimulaciones(tasasInput,nodosTasas)

M = 100
dfSimulaciones = pd.DataFrame(simulacionCurva(dfDiferencias, M), columns=[
                              30, 90, 180, 360, 540, 720, 1080, 1800, 2160, 3600])

Tasas = interpolaTasas(dfSimulaciones, nodosTasas)

# %% Grafico para ver nodos simulados

plt.plot(nodosTasas, Tasas.values.transpose())
plt.xlim(30, 3600)
plt.xticks(np.arange(0, 3601, 360))
plt.ylabel("Tasa FTP")
plt.xlabel("Tiempo (días)")
plt.title("Simulación de Tasas de Interés 360 días")
plt.show()

# %% Actualizo la cartera


capitalEconomicoPesos = CapitalEconomico(Caidas, Tasas, "ARS", M)


# capitalEconomicoDolares = CapitalEconomico(Caidas, Tasas, "USD", M)

# %% Time report

end = time.time()
print(f'el codigo tarda {end - start} segundos en correr {M} simulaciones')
