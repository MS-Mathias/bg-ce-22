# %% Import de librerias relevantes


import numpy as np 
import pandas as pd 
import time

start = time.time()

# %% Funciones para las Simulacion de Tasas de Interes


def parametrosSimulaciones(tasasInput, LugarBalance, Moneda):
    """Genera tres DataFrames con los parametros necesarios para realizar las simulaciones.
    Calcula la matriz de diferencias de tasas, la matriz de covarianza y la matriz de Cholesky.
    
    Parametros
        ----------
        tasasInput : DataFrame
            Serie historica de las curvas de tasas con todos sus nodos.
        LugarBalance : String
            Nombre del lugar del balance al que pertenecen las tasas que se van a utilizar.
            Puede ser Activo o Pasivo.
        Moneda : String
            Codigo de la tasa que se va a utilizar.
            Puede ser ARS, USD o CER.
    """
    
    dfDiferencia = DiferenciaNodos(tasasInput.loc[(tasasInput["LugarBalance"] == LugarBalance) &
                                                  (tasasInput["Moneda"] == Moneda),
                                                  tasasInput.columns.values.tolist()[1:len(tasasInput.columns)-2]])

    dfCovarianza = dfDiferencia.cov()
    cholesky = np.linalg.cholesky(dfCovarianza) 

    return dfDiferencia, dfCovarianza, cholesky


def DiferenciaNodos(tasasInput):
    """Genera un DataFrame con las diferencias entre las tasas de cada periodo y las de 90 dias antes.
    
    Parametros
        ----------
        tasasInput : DataFrame
            Serie historica de las curvas de tasas con todos sus nodos.
    """
    
    df1 = tasasInput.copy() 
    for nodo in df1:
        if nodo == "Date":
            continue
        else:
            df1[nodo] = df1[nodo].diff(90)
    return df1

# %% Funciones que simulan las curvas y las interpola


def simulacionCurva(dfDiferencia, ultimaCurva, cholesky, M=1000):
    """Genera array bidimencional de numpy con las M simulaciones de las curvas de tasas.
    
    Parametros
        ----------
        dfDiferencia : DataFrame
            Serie historica de variaciones de tasas
        ultimaCurva : Series
            Es una serie que contiene las tasas de todos los nodos para la ultima fecha disponible
        cholesky : Array of float64
            Es la matriz de cholesky calculada a partir de la matriz de covarianzas del dfDiferencia
        M : Interger
            Es el numero de simulaciones definido por default como 1.000 
    """
    
    arraySimulaciones = np.zeros(shape=(M, len(ultimaCurva)))
    for i in range(M):
        shockIndep = np.array(shockIndependiente(dfDiferencia))
        shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky))
        curvaSimulada = np.add(ultimaCurva, shockCorr)
        arraySimulaciones[i] = curvaSimulada
    return arraySimulaciones

def shockIndependiente(dfDiferencia):
    """Genera a partir del DataFrame de diferencias de tasas,
    una lista de shocks intependientes para cada nodo de la curva de tasas.
    
    Parametros
        ----------
        dfDiferencia : DataFrame
            Serie historica de variaciones de tasas
    """
    
    VectorShock = []
    average = {}
    std = {}
    for nodo in dfDiferencia:
        if nodo == "Date":
            continue
        else:
            average[nodo] = dfDiferencia[nodo].mean()
            std[nodo] = dfDiferencia[nodo].std()
    for nodo in dfDiferencia:
        if nodo == "Date":
            continue
        else:
            percentil = dfDiferencia[nodo].quantile(np.random.rand())
            shock = (percentil - average[nodo]) / std[nodo]
        VectorShock.append(shock)
    return VectorShock


def shockCorrelacionado(shockIndependiente, cholesky):
    """Genera a partir de los shocks independientes y la matriz de cholesky,
    una lista de shocks correlacionados entre si.
    
    Parametros
        ----------
        shockIndependiente : List
            Lista de shocks aleatorios para cada nodo de la curva.
        cholesky : Array of float64
            Es la matriz de cholesky calculada a partir de la matriz de covarianzas del dfDiferencia.
    """
    
    shockCorrelacionado = []
    for nodo in cholesky:
        shock = np.dot(shockIndependiente, nodo)
        if shock < 0:
            shock = 0
        shockCorrelacionado.append(shock)
    return shockCorrelacionado


def interpolaTasas(dfSimulaciones, nodosTasas):
    """Interpola el DataFrame de simulaciones para tener las tasas de todos los meses hasta los 10 a??os.
    
    Parametros
        ----------
        dfSimulaciones : DataFrame
            Es el DataFrame que contiene todas las curvas de tasas simuladas en las M simulaciones.
        nodosTasas : array of int32
            Es un vector con todos los nodos de la curva de tasas
    """
    
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


def ValorActualiza(Caidas, Tasas, LugarBalance, Moneda, M=1000):

    Actualizados = actualizaSimulaciones(Caidas.fillna(0)[(Caidas["Moneda"] == Moneda) & 
                                                          (Caidas["LugarBalance"] == LugarBalance)].values[:, 4:], 
                                         Tasas.values, M)

    return Actualizados


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

# %% Funcion que actualiza las distintas aperturas de caidas


def loopActualiza(Caidas, tasasInput, nodosTasas, M):

    ValorActual = {}
    start = time.time()
    
    for LugarBalance in LB:

        ValorActual_assist = {}
        for Moneda in TS:

            dfDiferencias, dfCovarianza, cholesky = parametrosSimulaciones(
                tasasInput, LugarBalance, Moneda)

            if dfDiferencias.iloc[90:].isnull().any().any():
                print("El DataFrame de diferencias de tasas contiene valores nulos \n")
                raise SystemExit()

            if dfCovarianza.isnull().any().any():
                print("La matriz de covarianza tiene valores nulos")
                raise SystemExit()
            
            dfSimulaciones = pd.DataFrame(simulacionCurva(dfDiferencias,
                                                          tasasInput.loc[(tasasInput["LugarBalance"] == LugarBalance) &
                                                                         (tasasInput["Moneda"] == Moneda),
                                                                         tasasInput.columns.values.tolist()[1:len(tasasInput.columns)-2]].iloc[-1],
                                                          cholesky,
                                                          M),
                                          columns=[30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900, 1080, 1260, 1440, 1620, 1800, 2160, 3600])

            if dfSimulaciones.isnull().any().any():
                print("El DataFrame de Simulaciones de tasas contiene valores nulos")
                raise SystemExit()

            Tasas = interpolaTasas(dfSimulaciones, nodosTasas)

            if Tasas.isnull().any().any():
                print("El DataFrame de tasas interpoladas contiene valores nulos")
                raise SystemExit()

            end = time.time()
            print(
                f'el codigo tarda {end - start:.2f} segundos en realizar las {M} simulaciones para la tasa {LugarBalance} {Moneda} ')
            start = time.time()

            ValorActual_assist[Moneda] = ValorActualiza(
                Caidas, Tasas, LugarBalance, Moneda, M)

            end = time.time()
            print(
                f'el codigo tarda {end - start:.2f} segundos en actualizar los activos de {LugarBalance} {Moneda} ')
            start = time.time()

        ValorActual[LugarBalance] = ValorActual_assist
    return ValorActual


def calculaCE(ValoresActuales):
    CE = {}
    for i in TS:
        Activo = ValoresActuales["Activo"][i]
        Pasivo = ValoresActuales["Pasivo"][i]
        diferencia = [e1 - e2 for e1, e2 in zip(Activo, Pasivo)]
        CE[i] = (np.mean(diferencia) - np.percentile(diferencia, 0.1))
    return CE


# %% Inputs del modelo


end = time.time()
print(f'el codigo tarda {end - start:.2f} segundos en definir las funciones')
start = time.time()

Caidas = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Caidas tasa.xlsx")
tasaAFTP = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Activa Pesos Fija")
tasaAUSD = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Activa Dolares Fija")
tasaACER = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Activa Pesos Fija")
tasaPFTP = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Pasiva Pesos Fija")
tasaPUSD = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Pasiva Dolares Fija")
tasaPCER = pd.read_excel(
    "C:/Users/mathias.ezequiel.va1/Desktop/Banco Galicia - Capital Economico/Fase2/Tasa/InputsFalsos/Tasas Pasivas y Activas Diarias.xls", "Pasiva Pesos Fija")

tasaAFTP["LugarBalance"], tasaAFTP["Moneda"] = "Activo", "ARS"
tasaAUSD["LugarBalance"], tasaAUSD["Moneda"] = "Activo", "USD"
tasaACER["LugarBalance"], tasaACER["Moneda"] = "Activo", "CER"
tasaPFTP["LugarBalance"], tasaPFTP["Moneda"] = "Pasivo", "ARS"
tasaPUSD["LugarBalance"], tasaPUSD["Moneda"] = "Pasivo", "USD"
tasaPCER["LugarBalance"], tasaPCER["Moneda"] = "Pasivo", "CER"

frames = [tasaAFTP, tasaAUSD, tasaACER, tasaPFTP, tasaPUSD, tasaPCER]

tasasInput = pd.concat(frames)

tasasInput.columns = ["Fecha", 30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900,
                      1080, 1260, 1440, 1620, 1800, 2160, 2520, 2880, 3240, 3600, "LugarBalance", "Moneda"]

tasasInput.drop([2520, 2880, 3240], axis=1, inplace=True)

for nodo in tasasInput:
    if nodo == "Fecha" or nodo == "LugarBalance" or nodo == "Moneda":
        continue
    else:
        tasasInput[nodo] = tasasInput[nodo] / 100


end = time.time()
print(
    f'el codigo tarda {end - start:.2f} segundos en realizar el input de tasas y caidas')
start = time.time()

nodosTasas = np.arange(120) * 30 + 30

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados

M = 10000

LB = ["Activo", "Pasivo"]
TS = ["ARS", "USD", "CER"]

ValoresActuales = loopActualiza(Caidas, tasasInput, nodosTasas, M)

CapitalEconomico = calculaCE(ValoresActuales)


# %% Time report

end = time.time()
print(
    f'el codigo tarda {end - start:.2f} segundos en correr {M} simulaciones para todas las tasas')
del start, end, M, frames, nodosTasas
