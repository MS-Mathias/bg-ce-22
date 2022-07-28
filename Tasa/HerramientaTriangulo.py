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
    
    
    actualizado["ipc_base"] = 100
    
    actualizado["MontoActualizado"] = actualizado["saldo"] / (actualizado["indicador_macro_vl"] / actualizado["ipc_base"])
    
    final = actualizado[["indicador_macro_fc","segmento","MontoActualizado"]].copy()
    final.columns = ["Fecha","Producto","Montos"]
    productoacuenta = {"CC mino":"CC mino",
                       "CC mayo":"CC mayo",
                       "CA no Mesa":"CA no Mesa",
                       "CA trans":"CA trans",
                       "CA no trans":"CA no trans"}
        
    Ponderando = {}
    for producto in productoacuenta.values():
        Ponderando[producto] = final.loc[(final["Producto"] == producto) & (final["Fecha"] == max(final["Fecha"])),"Montos"].values[0]
    
    saldoCC = Ponderando["CC mino"] + Ponderando["CC mayo"]
    saldoCA = Ponderando["CA no Mesa"] + Ponderando["CA trans"] + Ponderando["CA no trans"]
    
    for producto in Ponderando.keys():
        if producto[0:2] == "CA":
            Ponderando[producto] /= saldoCA
        else:
            Ponderando[producto] /= saldoCC
    
    return final,Ponderando

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
        if producto in ("Cuentas Corrientes Mino","Cajas de Ahorro Resto No Trans"):
            for index in Output[producto].index:
                if (total + Output[producto][index] < 0.5) & (finished == False):
                    total += Output[producto][index]
                elif finished == False:
                    Output[producto][index] = 1-total
                    finished = True
                else:
                    Output[producto][index] = 0
        else:
            for index in Output[producto].index:
                if (total + Output[producto][index] < 1) & (finished == False):
                    total += Output[producto][index]
                elif finished == False:
                    Output[producto][index] = 1-total
                    finished = True
                else:
                    Output[producto][index] = 0
        
    return Output


def distribuyeCaida(caidas,Desarrollos,vida_promedio, ponderacion):
    
    nodos = np.arange(60)*30+30

    for producto in ["Cuentas Corrientes","Cajas de Ahorro Resto"]:
        CaidaAssist = caidas[(caidas["Cuenta"] == producto) & (caidas["Moneda"] == "ARS")]
        if producto == "Cuentas Corrientes":
            ResultadoAssist = [Desarrollos["Cuentas Corrientes Mino"],
                               Desarrollos["Cuentas Corrientes Mayo"]]
        else:
            ResultadoAssist = [Desarrollos["Cajas de Ahorro Resto No Mesa"],
                               Desarrollos["Cajas de Ahorro Resto Trans"],
                               Desarrollos["Cajas de Ahorro Resto No Trans"]]
        
        Monto = CaidaAssist[30].values[0]
        CaidaAssist[30].values[0] = 0
        
        for desarrollo in ResultadoAssist:
            if desarrollo.name in ("Cuentas Corrientes Mino","Cajas de Ahorro Resto No Trans"):
                for nodo in nodos:
                    CaidaAssist.loc[CaidaAssist.index[0],nodo] += Monto * ponderacion[desarrollo.name] * desarrollo[nodo]
                for nodo in nodos:
                    if nodo >= vida_promedio:
                        CaidaAssist.loc[CaidaAssist.index[0],nodo] += Monto * 0.5 * ponderacion[desarrollo.name]
                        break
            else:
                for nodo in nodos:
                    CaidaAssist.loc[CaidaAssist.index[0],nodo] += Monto * ponderacion[desarrollo.name] * desarrollo[nodo]
        caidas.loc[CaidaAssist.index[0],:] = CaidaAssist.loc[CaidaAssist.index[0],:]


def ejecutoTriangulo(dfCaidas):

    df,ponderacion = importaTriangulo()
    
    fechas = pd.to_datetime(df['Fecha']).dt.date.drop_duplicates().values
    
    nodos = np.arange(len(fechas)-1)*30+30
    
    Productos = df['Producto'].unique()
    
    Resultado = triangulo(df, Productos, fechas, nodos)
    
    return Resultado, ponderacion

Caidas = pd.read_excel("Caidas V3.xlsx")

Resultado, ponderacion = ejecutoTriangulo(Caidas)

vida_promedio = 362.5 #CalcVidaPromedio(df,FiltroMoneda=moneda,FiltroBalance=balance)

distribuyeCaida(Caidas,Resultado,vida_promedio,ponderacion)