# %% Import de librerias relevantes


import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import time
start = time.time()

# %% Funciones para las Simulacion de Tasas de Interes


def parametrosSimulaciones(tasasInput,LugarBalance,Moneda):
    dfDiferencia = DiferenciaNodos(tasasInput.loc[(tasasInput["LugarBalance"]==LugarBalance) & 
                                                  (tasasInput["Moneda"]==Moneda),
                                                  tasasInput.columns.values.tolist()[1:11]])
    
    dfCovarianza = dfDiferencia.cov()
    cholesky = np.linalg.cholesky(dfCovarianza)
    
    return dfDiferencia, dfCovarianza, cholesky

def DiferenciaNodos(tasasInput):
    df1 = tasasInput.copy()
    for nodo in df1:
        if nodo == "Date":
            continue
        else:
            df1[nodo] = df1[nodo].diff(90)
    return df1

# %% Funciones que simulan las curvas y las interpola


def simulacionCurva(dfDiferencia, ultimaCurva, cholesky, M=1000):
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
                    a = Tasas[nodosTasas[j]] - Tasas[nodosTasas[i-1]]
                    b = nodosTasas[i] - nodosTasas[i-1]
                    c = nodosTasas[j] - nodosTasas[i-1]
                    x = Tasas[nodosTasas[i-1]] + (a * b / c)
                    Tasas[nodosTasas[i]] = x
                    break
        else:
            shift = 0
            continue
    return Tasas

# %% Funciones que actualizan las caidas por sus respectivas tasas.


def ValorActualiza(Caidas, Tasas, LugarBalance, Moneda, M = 1000):
    A= separaCaidas(Caidas, LugarBalance, Moneda)
    
    Actualizados = actualizaSimulaciones(A, Tasas.values, M)
    
    return Actualizados

def separaCaidas(Caidas, LugarBalance, Moneda):
    CaidasSep = Caidas.fillna(0)[(Caidas["Moneda"] == Moneda) & (
        Caidas["LugarBalance"] == LugarBalance)].values[:, 4:]
    return CaidasSep

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

# %% Funcion que actualiza las distintas aperturas de caidas

def loopActualiza(Caidas,tasasInput,M):
    ValorActual = {}
    start = time.time()
    for LugarBalance in LB:
        ValorActual_assist = {}
        for Moneda in TS:
            dfDiferencias, dfCovarianza, cholesky = parametrosSimulaciones(tasasInput,LugarBalance,Moneda)

            dfSimulaciones = pd.DataFrame(simulacionCurva(dfDiferencias,
                                                          tasasInput.loc[(tasasInput["LugarBalance"]==LugarBalance) & 
                                                                         (tasasInput["Moneda"]==Moneda),
                                                                         tasasInput.columns.values.tolist()[1:11]].iloc[-1], 
                                                          cholesky, 
                                                          M), 
                                          columns=[30, 90, 180, 360, 540, 720, 1080, 1800, 2160, 3600])

            Tasas = interpolaTasas(dfSimulaciones, nodosTasas)
            
            end = time.time()
            print(f'el codigo tarda {end - start} segundos en realizar las {M} simulaciones para la tasa {LugarBalance} {Moneda} ')
            start = time.time()
            
            ValorActual_assist[Moneda] = ValorActualiza(Caidas, Tasas, LugarBalance, Moneda, M)
            
            end = time.time()
            print(f'el codigo tarda {end - start} segundos en actualizar los activos de {LugarBalance} {Moneda} ')
            start = time.time()
            
        ValorActual[LugarBalance] = ValorActual_assist
    return ValorActual

def calculaCE(ValoresActuales,cotizaUSD):
    CE = {}
    for i in TS:
        Activo = ValoresActuales["Activo"][i]
        Pasivo = ValoresActuales["Pasivo"][i]
        diferencia = [e1 - e2 for e1, e2 in zip(Activo,Pasivo)]
        CE[i] = np.percentile(diferencia, 99.9)
    return CE

# %% Inputs del modelo
end = time.time()
print(f'el codigo tarda {end - start} segundos en definir las funciones')
start = time.time()

Caidas = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Caidas tasa.xlsx")
tasaAFTP = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/ActivosFTP.xlsx")
tasaAUSD = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/ActivosUSD.xlsx")
tasaACER = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/ActivosCER.xlsx")
tasaPFTP = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/PasivosFTP.xlsx")
tasaPUSD = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/PasivosUSD.xlsx")
tasaPCER = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/PasivosCER.xlsx")

tasaAFTP["LugarBalance"],tasaAFTP["Moneda"] = "Activo", "ARS"
tasaAUSD["LugarBalance"],tasaAUSD["Moneda"] = "Activo", "USD"
tasaACER["LugarBalance"],tasaACER["Moneda"] = "Activo", "CER"
tasaPFTP["LugarBalance"],tasaPFTP["Moneda"] = "Pasivo", "ARS"
tasaPUSD["LugarBalance"],tasaPUSD["Moneda"] = "Pasivo", "USD"
tasaPCER["LugarBalance"],tasaPCER["Moneda"] = "Pasivo", "CER"
 
frames = [tasaAFTP, tasaAUSD, tasaACER, tasaPFTP, tasaPUSD, tasaPCER]
    
tasasInput = pd.concat(frames)


end = time.time()
print(f'el codigo tarda {end - start} segundos en realizar el input de tasas y caidas')
start = time.time()

nodosTasas = np.arange(120) * 30 + 30

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados

M = 10000

LB = ["Activo","Pasivo"]
TS = ["ARS","USD","CER"]

ValoresActuales = loopActualiza(Caidas,tasasInput,M)

cotizaUSD = 200

CapitalEconomico = calculaCE(ValoresActuales, cotizaUSD)


# %% Time report

end = time.time()
print(f'el codigo tarda {end - start} segundos en correr {M} simulaciones para todas las tasas')
