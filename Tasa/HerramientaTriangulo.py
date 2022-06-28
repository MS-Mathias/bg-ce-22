import numpy as np
import pandas as pd
from tqdm import tqdm

def importaTriangulo():

    Cuadro1 = pd.read_excel("TrianguloDatos.xlsx","Cuadro1")
    Cuadro2 = pd.read_excel("TrianguloDatos.xlsx","Cuadro2")
    Cuadro3 = pd.read_excel("TrianguloDatos.xlsx","Cuadro3")
    Cuadro4 = pd.read_excel("TrianguloDatos.xlsx","Cuadro4")
    IPC_Tabla = pd.read_excel("TrianguloDatos.xlsx","IPC")
    
    
    post2019 = pd.concat([Cuadro1,Cuadro2])
    pre2019 = pd.concat([Cuadro3,Cuadro4])
    
    
    pre2019["fecha"] = pre2019["anio"].map(str) + pre2019["mes"].map(str)
    
    
    post2019.fecha = post2019.fecha.apply(lambda x: x.date())
    post2019['anio'] = pd.DatetimeIndex(post2019['fecha']).year
    post2019['mes'] = pd.DatetimeIndex(post2019['fecha']).month
    post2019["fecha"] = post2019["anio"].map(str) + post2019["mes"].map(str)
    
    post2019 = post2019[post2019.anio != 2019]
    
    IPC_Tabla.indicador_macro_fc = IPC_Tabla.indicador_macro_fc.apply(lambda x: x.date())
    IPC_Tabla['anio'] = pd.DatetimeIndex(IPC_Tabla['indicador_macro_fc']).year
    IPC_Tabla['mes'] = pd.DatetimeIndex(IPC_Tabla['indicador_macro_fc']).month
    IPC_Tabla["fecha"] = IPC_Tabla["anio"].map(str) + IPC_Tabla["mes"].map(str)
    
    consolidado = pd.concat([pre2019,post2019])
    
    actualizado = pd.merge(consolidado, 
                          IPC_Tabla, 
                          on ='fecha', 
                          how ='inner')
    
    
    actualizado["ipc_actual"] = max(actualizado.loc[actualizado["fecha"] == max(actualizado["fecha"]),"indicador_macro_vl"])
    
    actualizado["MontoActualizado"] = actualizado["saldo"] * actualizado["indicador_macro_vl"] / actualizado["ipc_actual"]
    
    final = actualizado[["indicador_macro_fc","segmento","MontoActualizado"]].copy()
    final.columns = ["Fecha","Producto","Montos"]
    productoacuenta = {"CC mino":"Cuentas Vista",
                       "CC mayo":"Cuentas Corrientes",
                       "CA no Mesa":"Cajas de Ahorro Mesa",
                       "CA trans":"Cajas de Ahorro Resto",
                       "CA no trans":"Cajas de Ahorro Resto Nueva Estructura"}
    
    for key in productoacuenta:
        final.loc[final["Producto"] == key,["Producto"]] = productoacuenta[key]
        
    return final

def triangulo(df,Productos,fechas,nodos):
    
    Desarrollos = {}
    
    for producto in tqdm(Productos):
        
        array = np.zeros(shape=(len(fechas),len(fechas)))
        df1 = df[df["Producto"]==producto].copy()
        for i in range(len(fechas)):
            for j in range(len(fechas)):
                if i + j >= len(fechas):
                    array[i,j] = np.nan
                else:
                    array[i,j] = df1["Montos"].values[j+i] / df1["Montos"].values[i] - 1
            
        percentil = []
        for column in range(len(array)):
            percentil.append(np.nanpercentile(array[:,column],0.5))
        
        Desarrollos[producto] = percentil
        
    arrayProductos = {}
    for producto in Desarrollos.keys():
        
        array = np.array(Desarrollos[producto][1:])
        
        for i in range(len(array)-1):
            if i == 0:
                array[i] = -array[i]
            else:
                array[i] = -array[i] - array[0:i].sum()
        
        for i in range(len(array)-1):
            if array[i] <= 0:
                array[i] = 0
        arrayProductos[producto] = array
        
        Output = pd.DataFrame.from_dict(arrayProductos)
        Output.index = nodos
        Output = Output.truncate(after = 1800)

    for producto in Output:
        total = 0
        finished = False
        for index in Output[producto].index:
            if (total + Output[producto][index] < 1) & (finished == False):
                total += Output[producto][index]
            else:
                Output[producto][index] = 0
                finished = True
            
    return Output


def distribuyeCaida(caidas,Desarrollos,vida_promedio):
    
    nodos = np.arange(60)*30+30

    for producto in Resultado:
        CaidaAssist = caidas[(caidas["Cuenta"] == producto) & (caidas["Moneda"] == "ARS")]
        ResultadoAssist = Resultado[producto]
        Monto = CaidaAssist[30].values[0]
        CaidaVP = Monto * 0.5
        CaidaDesarrollos = Monto * 0.5
        for nodo in nodos:
            CaidaAssist.loc[CaidaAssist.index[0],nodo] = CaidaDesarrollos * ResultadoAssist[nodo]
            if nodo >= vida_promedio:
                CaidaAssist.loc[CaidaAssist.index[0],nodo] = CaidaAssist.loc[CaidaAssist.index[0],nodo] + CaidaVP
                vida_promedio = 3600
        caidas.loc[CaidaAssist.index[0],:] = CaidaAssist.loc[CaidaAssist.index[0],:]


def ejecutoTriangulo(dfCaidas):

    df = importaTriangulo()
    
    fechas = pd.to_datetime(df['Fecha']).dt.date.drop_duplicates().values
    
    nodos = np.arange(len(fechas)-1)*30+30
    
    Productos = df['Producto'].unique()
    
    Resultado = triangulo(df, Productos, fechas, nodos)
    
    return Resultado

Caidas = pd.DataFrame()

Resultado = ejecutoTriangulo(Caidas)

vida_promedio = 362.5 #CalcVidaPromedio(df,FiltroMoneda=moneda,FiltroBalance=balance)

distribuyeCaida(Caidas,Resultado,vida_promedio)