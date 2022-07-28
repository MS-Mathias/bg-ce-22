# %% Import de librerias relevantes


import numpy as np 
import pandas as pd 
import time
from tqdm import tqdm

start = time.time()

# %% Funciones para definir parametros de las Simulacion de Tasas de Interes

def parametrosSimulaciones(dfDiferencia):
    """Genera tres DataFrames con los parametros necesarios para realizar las simulaciones.
    Calcula la matriz de diferencias de tasas, la matriz de covarianza y la matriz de Cholesky.
    
    Parametros
        ----------
        dfDiferencia : DataFrame
            Serie historica de las variaciones curvas de tasas.
    """
    try:
        dfCovarianza = dfDiferencia.cov()
        cholesky = np.linalg.cholesky(dfCovarianza)
    except:
        dfDiferencia.drop(dfDiferencia.columns[-2],axis=1,inplace=True)
        dfCovarianza, cholesky = parametrosSimulaciones(dfDiferencia)

    return dfCovarianza, cholesky


def DiferenciaNodos(df0):
    """Genera un DataFrame con las diferencias entre las tasas de cada periodo y las de 90 dias antes.
    
    Parametros
        ----------
        tasasInput : DataFrame
            Serie historica de las curvas de tasas con todos sus nodos.
    """
    
    df1 = df0.copy() 
    for nodo in df1:
        if type(nodo) != int:
            continue
        else:
            df1[nodo] = df1[nodo].diff(30)
    return df1

# %% Funciones que simulan las curvas y las interpola


def simulacionCurva(dfDiferencia, ultimaCurva, cholesky, M=1000, arrayShockIndependiente="PRIMERO"):
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
    
    avg = {}
    std = {}
    nodosAsist = ultimaCurva.index
    nodosindex = []
    for i in range(len(nodosAsist)):
        nodosindex.append(nodosAsist[i]//30-1)
    
    for nodo in dfDiferencia:                                                   ### ciclo todos los nodos
        avg[nodo] = dfDiferencia[nodo].mean()                                   ### calculo el promedio del nodo
        std[nodo] = dfDiferencia[nodo].std()                                    ### calculo el desvio del nodo
        
    if type(arrayShockIndependiente) == str:

        arraySimulaciones = np.zeros(shape=(M, len(ultimaCurva)))               ### Genero Array para almacenar las simulaciones

        arrayRandom = np.random.rand(M,120)                                     ### Genero array de valores aleatorios
        
        dfRandom = pd.DataFrame(arrayRandom)                                    ### paso el array a un df de pandas para operar por nodos
        
        dfRandom = dfRandom.drop(columns=[col for col in dfRandom if col not in nodosindex])
        dfRandom.columns = nodosAsist
        
        for nodo in dfRandom:                                                   ### ciclo todos los nodos
            dfRandom[nodo] = ((dfDiferencia[nodo].quantile(
                dfRandom[nodo]) - avg[nodo]) / std[nodo]).values                ### tomo el percentil y lo estandarizo

        arrayShockIndependiente = dfRandom.values                               ### Guardo el array de shocks independientes

        for i in tqdm(range(M)):                                                ### ciclo a travez de todas las simulaciones
            shockIndep = arrayShockIndependiente[i]                             ### tomo el shock aleatorio independiente de la simulacion
            shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky))     ### uso la funcion shockCorrelacionado para correlacionar los shocks
            curvaSimulada = np.add(ultimaCurva, shockCorr)                      ### sumo la curva de shocks correlacionados a la ultima curva
            arraySimulaciones[i] = curvaSimulada                                ### guardo la curva simulada en el array de simulaciones

    else:                                                                       ### aca se realiza el mismo proceso de antes, tomando los shocks aleatorios de una tasa ya simulada y correlacionada
        arraySimulaciones = np.zeros(shape=(M, len(ultimaCurva)))
        arrayRandom = arrayShockIndependiente                                  ### Genero array de valores aleatorios
        
        dfRandom = pd.DataFrame(arrayRandom)                                    ### paso el array a un df de pandas para operar por nodos
        
        dfRandom = dfRandom.drop(columns=[col for col in dfRandom if col not in nodosindex])
        dfRandom.columns = nodosAsist
        
        for nodo in dfRandom:                                                   ### ciclo todos los nodos
            dfRandom[nodo] = ((dfDiferencia[nodo].quantile(
                dfRandom[nodo]) - avg[nodo]) / std[nodo]).values                ### tomo el percentil y lo estandarizo

        arrayShockIndependiente = dfRandom.values   
        for i in tqdm(range(M)):
            shockIndep = arrayShockIndependiente[i]
            shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky))
            curvaSimulada = np.add(ultimaCurva, shockCorr)
            arraySimulaciones[i] = curvaSimulada
            arrayShockIndependiente = arrayShockIndependiente
            
    return arraySimulaciones, arrayRandom


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
    """Interpola el DataFrame de simulaciones para tener las tasas de todos los meses hasta los 10 años.
    
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

    for i in tqdm(range(len(nodosTasas))):
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


def ValorActualiza(df, Tasas, M=1000):
    """Actualiza las caidas utilizando las M cuva de tasas simuladas.
    
    Parametros
        ----------
        Caidas : DataFrame
            Contiene  todas las caidas de activos y pasivos con intereses ya calculados.
        Tasas : DataFrame
            Contiene todas las curvas de tasas simuladas para la tasa que esta siendo operada.
        M : Integer
            Es el numero de simulaciones que se estan ejecutando.
    """
    
    Actualizados = []
    
    for i in tqdm(range(M)):
        Actualizados.append(actualizaCaida(df.sum(axis=0), Tasas.values[i]))

    return Actualizados


def actualizaCaida(df1, Tasas):
    """Actualiza las caidas utilizando las M cuvas de tasas simuladas.
    
    Parametros
        ----------
        Caidas : DataFrame
            Contiene  todas las caidas ya filtradas de activos y pasivos 
            con intereses ya calculados.
        Tasas : DataFrame
            Contiene una curva simulada de la tasa que esta siendo operada.
    """
    ValorActual = 0
    for i in range(len(Tasas)):
        if df1[i] != 0:
            ValorActual += df1[i] / (1+Tasas[i]) ** ((i*30+30) / 360)
    return ValorActual

# %% Funcion que actualiza las distintas aperturas de caidas


def loopActualiza(Caidas, tasasInput, nodosTasas, correlaciones, M = 1000):
    """Actualiza las caidas utilizando las M cuva de tasas simuladas.
    
    Parametros
        ----------
        Caidas : DataFrame
            Contiene todas las caidas de activos y pasivos con intereses ya calculados.
        tasasInput : DataFrame
            Contiene todas las curvas de todas las tasas.
        nodosTasas : Array of int32
            Es la secuencia que contiene todos los nodos de tasas (cada 30 días por 10 años).
        correlaciones : DataFrame
            Es la tabla que informa que tasas estan correlacionadas.
        M : Integer
            Es el numero de simulaciones que se van a ejecutar.
            Esta seteada por default en 1.000 simulaciones
    """
    
    ValorActual = {}
    dicSimulacionesCorr = {}
    sims = {}
    for LugarBalance in LB:
        sims_assist = {}
        ValorActual_assist = {}
        for Moneda in TS:
            print(f"""==============================
Comienza proceso para la tasa {LugarBalance} {Moneda}""")
            
            
            dfDiferencia = DiferenciaNodos(tasasInput.loc[(tasasInput['Lugar del balance'] == LugarBalance) &
                                                          (tasasInput["Moneda"] == Moneda),
                                                          tasasInput.columns.values.tolist()[1:len(tasasInput.columns)-2]])
            
            if (LugarBalance== "Activo") & (Moneda == "CER"):
                dfDiferencia.drop([60,90,120],axis=1,inplace=True)
            
            dfCovarianza, cholesky = parametrosSimulaciones(dfDiferencia)
            
            
            if dfDiferencia.iloc[360:].isnull().any().any():
                print("El DataFrame de diferencias de tasas contiene valores nulos \n")
                raise SystemExit()
                
            if dfCovarianza.isnull().any().any():
                print("La matriz de covarianza tiene valores nulos")
                raise SystemExit()
                
            Grupo = str(correlaciones.loc[(correlaciones['Lugar del balance'] == LugarBalance) & 
                                          (correlaciones["Moneda"] == Moneda)].values[:,-1][0])
            
            ultimaCurva = tasasInput.loc[(tasasInput['Lugar del balance'] == LugarBalance) &
                                         (tasasInput["Moneda"] == Moneda),
                                         dfDiferencia.columns.values.tolist()].iloc[-1]
            print("Simulo Tasa")
            
            if not(Grupo in dicSimulacionesCorr):
                arrayTasasSimuladas, arrayShockInependiente = simulacionCurva(dfDiferencia,
                                                                              ultimaCurva,
                                                                              cholesky,
                                                                              M)
                dicSimulacionesCorr[Grupo] = arrayShockInependiente
            
            else:
                arrayTasasSimuladas, arrayShockInependiente = simulacionCurva(dfDiferencia,
                                                                              ultimaCurva,
                                                                              cholesky,
                                                                              M,
                                                                              dicSimulacionesCorr[Grupo])
                
            dfSimulaciones = pd.DataFrame(arrayTasasSimuladas,
                                              columns=dfDiferencia.columns)
            
            if dfSimulaciones.isnull().any().any():
                print("El DataFrame de Simulaciones de tasas contiene valores nulos")
                raise SystemExit()
            
            print("Interpolo Tasas")
            
            Tasas = interpolaTasas(dfSimulaciones, nodosTasas)
            sims_assist[Moneda] = Tasas
            
            if Tasas.isnull().any().any():
                print("El DataFrame de tasas interpoladas contiene valores nulos")
                raise SystemExit()
            
            print("Actualizo Caidas")
            ValorActual_assist[Moneda] = ValorActualiza(Caidas.fillna(0).loc[(Caidas["Moneda"] == Moneda) & 
                                                                         (Caidas['Lugar del balance'] == LugarBalance)].values[:,4:], 
                                                        Tasas,
                                                        M)
            
        ValorActual[LugarBalance] = ValorActual_assist
        sims[LugarBalance] = sims_assist
    return ValorActual, dicSimulacionesCorr, sims


def neteoAP(ValoresActuales,TS, cotizaUSD):
    CE = pd.DataFrame(columns=TS)
    for i in TS:
        if i == "USD":
            Activo = (np.array(ValoresActuales["Activo"][i]) * cotizaUSD).tolist()
            Pasivo = (np.array(ValoresActuales["Pasivo"][i]) * cotizaUSD).tolist()
        else:
            Activo = ValoresActuales["Activo"][i]
            Pasivo = ValoresActuales["Pasivo"][i]
        
        Neto = [e1 - e2 for e1, e2 in zip(Activo, Pasivo)]
        CE.assign(i = Neto)
        CE[i] = Neto
    return CE


def Capitales(netos, TS):
    
    netos["Total"] = 0
    
    for i in TS:
        netos["Total"] += netos[i]
    
    Media = np.average(netos["Total"])
    Mediana = np.quantile(netos["Total"],0.5)  
    
    Percentil999 = np.quantile(netos["Total"],0.001)
    Percentil995 = np.quantile(netos["Total"],0.005)
    Percentil99 = np.quantile(netos["Total"],0.01)
    
    netos["Media"] = Media
    netos["Mediana"] = Mediana
    netos["99.9%"] = Percentil999
    netos["99.5%"] = Percentil995
    netos["99.0%"] = Percentil99
    
    
    
    CapitalEconomico999 = Media - Percentil999
    CapitalEconomico995 = Media - Percentil995
    CapitalEconomico99 = Media - Percentil99
    
    CapitalEconomico = {"CE 99.9" : CapitalEconomico999*1000,
                        "CE 99.5" : CapitalEconomico995*1000,
                        "CE 99.0" : CapitalEconomico99*1000}
    
    return CapitalEconomico


# %% Inputs del modelo


end = time.time()
print(f'el codigo tarda {end - start:.2f} segundos en definir las funciones')
start = time.time()

Caidas = pd.read_excel("Caidas V3.xlsx")

tasasInput = pd.read_excel('Curva Tasas Historicas.xlsx')

tasasInput.Fecha = tasasInput.Fecha.apply(lambda x: x.date())

tasasInput.columns = ["Fecha", 30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900,
                      1080, 1260, 1440, 1620, 1800, 2160, 2520, 2880, 3240, 3600, "Lugar del balance", "Moneda"]


for nodo in tasasInput:
    if nodo == "Fecha" or nodo == "Lugar del balance" or nodo == "Moneda":
        continue
    else:
        tasasInput[nodo] = tasasInput[nodo] / 100

end = time.time()

print(
    f'el codigo tarda {end - start:.2f} segundos en realizar el input de tasas y caidas')
start = time.time()

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados

nodosTasas = np.arange(120) * 30 + 30
M = 1000
LB = pd.unique(Caidas["Lugar del balance"])
TS = pd.unique(Caidas["Moneda"])
d = {"Lugar del balance":["Activo","Activo","Activo","Pasivo","Pasivo","Pasivo"],
     "Moneda":["ARS","USD","CER","ARS","USD","CER"],
     "Grupo":["A","B","C","A","D","C"]}
correlaciones = pd.DataFrame(d)

ValoresActuales,ShocksAleatorios,SimulacionesTasas = loopActualiza(Caidas, tasasInput, nodosTasas, correlaciones, M)
Caidas[30].max()
print()
print("==============================")

cotizaUSD = 110

neto = neteoAP(ValoresActuales, TS, cotizaUSD)

CapitalEcon = Capitales(neto,TS)

    
# %% Time report

end = time.time()
print(
    f'el codigo tarda {(end - start)/60:.2f} minutos en correr {M} simulaciones para todas las tasas')
