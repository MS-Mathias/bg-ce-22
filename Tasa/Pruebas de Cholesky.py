import numpy as np 
import pandas as pd
from tqdm import tqdm

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


tasasInput = pd.read_excel('Curva Tasas Historicas.xlsx')

tasasInput.columns = ["Fecha", 30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900,
                      1080, 1260, 1440, 1620, 1800, 2160, 2520, 2880, 3240, 3600, "Lugar del balance", "Moneda"]


for nodo in tasasInput:
    if nodo == "Fecha" or nodo == "Lugar del balance" or nodo == "Moneda":
        continue
    else:
        tasasInput[nodo] = tasasInput[nodo] / 100
        
ActivoOPasivo = pd.unique(tasasInput["Lugar del balance"])
MonedasTasas = pd.unique(tasasInput["Moneda"])

Tasas = []

for LugarBalance in ActivoOPasivo:
    for Moneda in MonedasTasas:
        Tasas.append(tasasInput.loc[(tasasInput['Lugar del balance'] == LugarBalance) & (tasasInput["Moneda"] == Moneda),
                       tasasInput.columns.values.tolist()[1:len(tasasInput.columns)-2]])

TasasDiff = []

for i in Tasas:
    TasasDiff.append(DiferenciaNodos(i).iloc[30:])

NodosCorr = []

for i in TasasDiff:
    NodosCorr.append(i.corr())

TasasDiff[0].drop([450,540,1620,2880,3240],axis=1,inplace=True)
TasasDiff[1].drop([450,900,1260,1620,2880],axis=1,inplace=True)
TasasDiff[2].drop([2160,2520,2880,3240],axis=1,inplace=True)
TasasDiff[3].drop([2160,2520,2880,3240],axis=1,inplace=True)
TasasDiff[5].drop([60,90,120,2160,2520,2880,3240],axis=1,inplace=True)

NodosCov = []

for i in TasasDiff:
    NodosCov.append(i.cov())

cholesky = []

for i in NodosCov:
    try:
        cholesky.append(np.linalg.cholesky(i))
    except:
        cholesky.append(np.linalg.eigvals(i))
