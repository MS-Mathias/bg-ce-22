import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import matplotlib.pyplot as plt
import os
import pyodbc
import time
import datetime
import calendar
from decimal import Decimal
start = time.time()

conn = pyodbc.connect("""Driver=Teradata; DBCName=DATA1N1; 
                      MechanismName=LDAP; UID=l0516053;
                      PWD=Capitaleconomico.1""")

conn.setencoding(encoding='latin1')
conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding = 'latin1')



def Fecha(df):
    
    lista_mes=[]
    lista_año=[]
    
    for i in range(len(df.columns)):
        if isinstance(df.columns[i], datetime.datetime)==True:
            #if df.columns[i].month-(i-1)<0:
            meses=0
            while ((df.columns[i].month-(i-1))+meses*(12))<0:
                meses+=1
            else:
                mes= df.columns[i].month-(i-1) +12*meses
                año=df.columns[i].year - meses 
                lista_mes.append(mes)
                lista_año.append(año)
        else:
            continue
    if len(set(lista_mes))==1 and len(set(lista_año))==1:
        return lista_mes[0],lista_año[0]
    else:
        print(f'El arhcivo {df} no pasa el control de contener las mismas fechas')
        raise SystemExit()
        
 

# =============================================================================
# 
# =============================================================================


                                         
def RenombraColumnas (df,cols=['Cuenta','Total','Moneda','Lugar del balance']):
    """Renombra las columnas para que tengan las caidas cada 30 dias.
        Formato:
    
    Parametro
        ----------
        df : DataFrame
            Nombre de los 4 archivos input de caidas
        cols : lista
            Por default son las columnas que no se van a modificar sus valores
            (no representan buckets)
    """
    
    
    df=df.rename(columns={df.columns[0]: 'Cuenta'}) ##renombra la priemr columna        
    df['Cuenta']=df['Cuenta'].str.strip()               ##le quita los espacios
    df2 = df[df.columns[~df.columns.isin(cols)]]    ##filtra el df
    df2.columns = [30*(i+1) for i in range(len(df2.columns))] ###genera bucktes cada 30 dias
    df=df[cols].join(df2)       ###genera el merge entre ambos dataframes
    
    return df

def ImportaArchivos (años_caidas=10):
    """Importa los archivos, genera las columnas Moneda y Lugar Balance
        segun el nombre con el que viene y se les realiza las siguientes transformaciones:
            renombro las columnas
            calculo los interes para cada cuenta
            realizo el filtro runooff
            
    
    Parametro
        ----------
        Archivos : list
            Nombre de los 4 archivos input de caidas
    """
        
    Lista_caidas_capital = []
    Lista_caidas_intereses = []
    Lista_caidas_capital_e_intereses = []
    Lista_df=[]
    ListaMes=[]
    ListaAño=[]
    parametros_caidas = pd.read_excel('Parametros Caidas (1).xlsx')
    Archivos = ['Activo $','Activo U$S','Pasivo $','Pasivo U$S']
    #Archivos = ['Pasivo $']
    Lista_descarte_filas=['Total Assets','Total Liab & Equity','Difference',np.NaN,
                          'WART','Maturity Timing','Repricing Spread','Period Cap',
                          'Period Floor','Lifetime Cap','Lifetime Floor',
                          'Beg Book Balance', 'Beg Book Rate', 'Runoff Balance Book',
                          'Runoff Yield Book','Repricing Balance','Repricing Rate']
    
    for archivo in tqdm(Archivos):  ##itero por cada nombre de archivo de la lista
        
        df = pd.read_excel('{}.xls'.format(archivo))   ##cargo el archivo
        
        """Calculo la fecha de calculo en base al arhcivo input y lo
            condenso en una lista
        """              
        mes,año=Fecha(df) ## filtro la fecha de corrida
        ListaMes.append(mes)
        ListaAño.append(año)
                        
        """Les inserto las columnas Moneda y lugar de balance segun el tipo de archivo"""
        
        if archivo[0:6] =='Activo' and ('U' not in archivo):            
            df.insert(loc=1, column='Moneda',value='ARS')
            df.insert(loc=2, column='Lugar del balance',value='Activo')  
            
        elif archivo[0:6] =='Activo' and ('U' in archivo):
            df.insert(loc=1, column='Moneda',value='USD')
            df.insert(loc=2, column='Lugar del balance',value='Activo')
            
        elif archivo[0:6] =='Pasivo' and ('U' not in archivo):
            df.insert(loc=1, column='Moneda',value='ARS')
            df.insert(loc=2, column='Lugar del balance',value='Pasivo')
                        
        else:
            df.insert(loc=1, column='Moneda',value='USD')
            df.insert(loc=2, column='Lugar del balance',value='Pasivo')
            
        """Llamo a la funcion RenmbraColumnas para modificar el df"""    
            
        df=RenombraColumnas(df)
        
        """Acumulo la sumatoria de las caidas posteriores al maximo de bucket
            a considerar dentro del modelo"""
       
        df.drop('Total',axis=1,inplace=True)

        """Calculo un nuevo df donde cada fila es una cuenta unica del archivo input"""
        
        df2=df[~df['Cuenta'].isin(Lista_descarte_filas)]
        
        """Realizo una union entre el dataframe con las cuentas unicas y el archivo input
            de los parametros, para de esta forma obtener los parametros de cada cuenta"""
                
        df3 = pd.merge(df2.reset_index(), parametros_caidas,  how='left', on=['Cuenta','Moneda','Lugar del balance']).set_index('index')
        
        """Reordeno el orden de las columnas para que aquellas que contengan informacion
        de los parametros queden al inicio"""
        
        Flag_cuenta=df3.pop('Flag cuenta')
        
        Capitalizacion_tasa=df3.pop('Capitalizacion tasa')
        df3.insert(3,'Capitalizacion_tasa',Capitalizacion_tasa)
        df3.insert(3,'Flag_cuenta',Flag_cuenta)
        #df3.insert(3,'Indice',Indice)
        
        """Aplico un control tal que detecte que haya cuentas nuevas que no esten
        contemplados dentro del archivo input de Parametros caidas"""

        nuevas_cuentas=df3[df3['Capitalizacion_tasa'].isna() | 
                           df3['Capitalizacion_tasa'].isna() | 
                           #df3['Indice'].isna() | 
                           df3['Flag_cuenta'].isna() ]
        nuevas_cuentas=nuevas_cuentas.iloc[:,0:3]

        """Si no encuentra el valor de los parametros cuando se realiza el join
        entre caidas y parametros, el valor de dicho parametro sera nulo.
        Esto implica que es una cuenta nueva, o que no se encuentra en el archivo input.
        Si tal es el caso, los considero dentro del modelo pero les asumo unos parametros predeterminados"""

        relleno_valores_nulos={'Indice':'Fix','Flag_cuenta':1,'Capitalizacion_tasa':12}
        df3.fillna(value=relleno_valores_nulos,inplace=True)
        
        """Elimino las filas que no voy a considerar (Flag=0)"""
        df3.drop(df3[df3['Flag_cuenta'] == 0].index, inplace = True)##dropeo las filas que novan a considerarse dentro del calculo
        df3.drop('Flag_cuenta',axis=1,inplace=True)
        df3.loc[df3['Flag CER']==1,'Moneda']='CER'
        df3.drop('Flag CER',axis=1,inplace=True)
        
        """Itero por cada cuenta unica para calcularle los intereses"""
        
        caidas_capital=df3.copy()
        caidas_intereses=df3.copy()
        caidas_capital_e_intereses=df3.copy()
        
        for i in df3.index:
            per=df3.at[i,'Capitalizacion_tasa']
            for col in df3:
                
                try:
                    
                    if type(col)!=int:
                        continue
                    
                    elif df.at[i+2,'Cuenta']!= 'Beg Book Rate':
                        
                        caidas_capital.at[i,col]=df.at[i+1,col]
                        caidas_intereses.at[i,col]=0
                        caidas_capital_e_intereses.at[i,col]=df.at[i+1,col]
                        
                    elif df.at[i+2,'Cuenta']== 'Beg Book Rate' and df.at[i+4,'Cuenta']!= 'Runoff Balance Book':
                        
                        caidas_capital.at[i,col]=df.at[i+1,col]
                        caidas_intereses.at[i,col]=0
                        caidas_capital_e_intereses.at[i,col]=df.at[i+1,col]
                        
                    elif df.at[i+4,'Cuenta']== 'Runoff Balance Book' and df.at[i+5,'Cuenta']== 'Runoff Yield Book':
                        
                        tna=df.at[i+5,col]/100 ##tasa nominal anual
                        tea=pow(1+tna*per/12,12/per)-1 ###tasa efectiva anual
                        tep=pow(1+tea, col/360)-1 ###tasa efectiva plazo
                        
                        caidas_capital.at[i,col]=df.at[i+4,col]
                        caidas_intereses.at[i,col]= df.at[i+4,col]*tep
                        caidas_capital_e_intereses.at[i,col]=df.at[i+4,col]*(1+tep)
                        
                        
                    elif df.at[i+4,'Cuenta']== 'Runoff Balance Book' :
                        
                        caidas_capital.at[i,col]=df.at[i+4,col]
                        caidas_intereses.at[i,col]=0
                        caidas_capital_e_intereses.at[i,col]=df.at[i+4,col]
                        
                    else:
                        
                        caidas_capital.at[i,col]=-1
                        caidas_intereses.at[i,col]=-1
                        caidas_capital_e_intereses.at[i,col]=-1
                        print('Las caidas de uno o mas cuentas no ha sido posible ser calculadas')
                        #raise SystemExit()
                
                except:
                    caidas_capital.at[i,col]=df.at[i+1,col]
                    caidas_intereses.at[i,col]=0
                    caidas_capital_e_intereses.at[i,col]=df.at[i+1,col]
                      
        """Junto todas las caidas en distintas listas"""
        max_caidas=años_caidas*360
        indice=caidas_capital_e_intereses.columns.get_loc(max_caidas)
        caidas_capital_e_intereses[max_caidas]=caidas_capital_e_intereses.iloc[:,indice:].sum(axis=1) ##le cargo al ultimo bucket todo lo que resta
        caidas_capital_e_intereses=caidas_capital_e_intereses.iloc[:,:(indice+1)]
        caidas_intereses[max_caidas]=caidas_intereses.iloc[:,indice:].sum(axis=1) ##le cargo al ultimo bucket todo lo que resta
        caidas_intereses=caidas_intereses.iloc[:,:(indice+1)]
        Lista_caidas_capital.append(caidas_capital)
        Lista_caidas_intereses.append(caidas_intereses)
        Lista_caidas_capital_e_intereses.append(caidas_capital_e_intereses)
        Lista_df.append(df)
        
    if len(set(ListaMes))==1 and len(set(ListaAño))==1:
        mes=ListaMes[0]
        año= ListaAño[0]
        dia = calendar.monthrange(año, mes)[1] ###para que sea el ultimo dia del mes
        fecha = datetime.date(año,mes,dia)
    else:
        print('Los 4 archivos no contienen las mismas fechas')
        raise SystemExit()
    Caidas = pd.concat(Lista_caidas_capital_e_intereses, ignore_index=True)
    Caidas.fillna(value=0,inplace=True)
    Caidas_capital = pd.concat(Lista_caidas_capital, ignore_index=True)
    Caidas_intereses = pd.concat(Lista_caidas_intereses, ignore_index=True)
    dfs = pd.concat(Lista_df, ignore_index=True)
        
    return fecha,Caidas,Caidas_capital,Caidas_intereses,dfs  ###la funcion me devuelve el archivo caidas condensado

fecha_calculo, Caidas,Caidas_capital,Caidas_intereses,dfs= ImportaArchivos()


def CalcVidaPromedio (df ,FiltroMoneda,FiltroBalance,cols=['Cuenta','Total','Moneda','Lugar del balance','Capitalizacion_tasa']):
    
    """Calcula la vida promedio para segun lugar balance y tipo de moneda
    
    Parametro
        ----------
        df : DataFrame
            tabla con las caidas condensadas
        FIltroMoneda : str
            que tipo de moneda queremos calcular
        FIltro balance : DataFrame
            que lugar de balance queremos calcular
            """
    df2=Caidas.copy()
               
    df2 = df2.loc[(df2['Moneda']==FiltroMoneda) & (df2['Lugar del balance']==FiltroBalance)] ##me qeudo con la tabla filtrada
    df2 = df2[df2.columns[~df2.columns.isin(cols)]] ### elimino las columnas que no son numericas
    df2=df2.loc[df2.sum(axis=1) != 0] ##dropeo aquellas filas que sean nulas
    Suma_producto=[]  
    Suma_total=[]
    for fila in range(len(df2)):        ### genero el for loop para que itere por cada cuenta y le caucle la vida promedia
        SerieCuenta = df2.iloc[fila,:].dropna() 
        Suma_producto.append(SerieCuenta.dot(SerieCuenta.index)) ##calculo la sumaproducto [valor*bucket] y lo agrego a una lista
        Suma_total.append(SerieCuenta.sum())    ### suma total del balance de esa cuenta
    
    vida_promedio = sum(Suma_producto)/sum(Suma_total)
    
    #if FiltroMoneda=='ARS' and FiltroBalance=='Activo':
        #tabla_historica = pd.read_excel('Vida Promedio Activo.xlsx')
        #tabla_historica['Fecha'] = pd.Series(pd.period_range("1/1/2020", freq="M", periods=27))
        
    return vida_promedio 


        
def CaidasTarjeta (df, Cuentas=['Minorista Tarjetas PA','Minorista Financiación en Cuenta',
                                'Minorista Tarjetas Resto'],
                        monedas=['ARS'],
                        balance='Activo',
                        parametro_caida=0.8):
    
    """Lleva los montos de tarjeta desde el bucket 30 hacia el bucket de la vida promedio segun
    el parametro caida
    
    Parametro
        ----------
        df : DataFrame
            tabla con las caidas condensadas
        Cuentas : lista
            lista con las cuentas tarjeta que se va a aplicar la caida tarjeta
        Moneda: lista
            para que itere sobre ambos tipos de moneda
        balance: lista
            lugar que se aplica
        parametro_caida: variable
            que proporcion va a caer en el bucket 30 y cuanto en el bucket de la vida prom
            """
        
    for moneda in monedas:
    
        vida_promedio = CalcVidaPromedio(df,FiltroMoneda=moneda,FiltroBalance=balance)
        
        for cuenta in Cuentas:
            for col in df.columns:
                
                if isinstance(col, int) and col >= vida_promedio:
                    
                    df.loc[(df['Cuenta']==cuenta) & (df['Moneda']==moneda),col] += df.loc[(df['Cuenta']==cuenta) & (df['Moneda']==moneda),30]*(1-parametro_caida)
                    df.loc[(df['Cuenta']==cuenta) & (df['Moneda']==moneda) ,30] = df.loc[(df['Cuenta']==cuenta) & (df['Moneda']==moneda),30]*parametro_caida
                    break
                else:
                    continue
    return df                    
                
    

Caidas=CaidasTarjeta(Caidas)
Caidas_capital=CaidasTarjeta(Caidas_capital)
Caidas_intereses=CaidasTarjeta(Caidas_intereses)

def importaTriangulo():
    query = """sel
sav_dly_bal_date as fecha,
case when (acredita_haberes_fl = 1 and mis_elemento_desc3 not in ('Banca Mayorista')) then 'CA trans' 
           when (acredita_haberes_fl = 0  and mis_elemento_desc3 not in ('Banca Mayorista')) then 'CA no trans' 
        else 'CA no Mesa' end as segmento,       
sum (sav_dly_sld_acr) / 1000000 as saldo
from p_dw_explo.sav_dly as ca
left join p_dw_explo.sav_gen as gen
on ca.acct_nbr  = gen.acct_nbr and ca.acct_subsystem_id = gen.acct_subsystem_id
left join p_lk_explo.tva_arbol_segmento as seg 
on seg_code = seg.segment_id
left join p_lk_explo.producto_arbol as prod
on gen.prod_id = prod.prod_id
left join p_dw_explo.ms_cliente as hab
on hab.party_id = gen.party_id
inner join (sel fecha_sla from p_dw_explo.calendario_servicios where ultimo_dia_habil_mes = 'Y' ) cal
on ca.sav_dly_bal_date = cal.fecha_sla
where mis_elemento_desc6 not in ('institucionales' ,'Corporativas', 'Corporativa') 
having saldo > 0
group by 1,2
    """
    Cuadro1 = pd.read_sql(query,conn)
    query = """sel
chk_dly_bal_date as fecha,
--case when  mis_elemento_desc3 = 'Banca Minorista'  then 'CC mino'  
        --else 'CC mayo' end as segmento,   
--case when  codigo_tipo_persona = 'F'  then 'CC mino'  
  --      else 'CC mayo' end as segmento,  
case when  ((codigo_tipo_persona = 'F' and mis_elemento_desc3 = 'Banca Minorista' )or ( flag_marca_mypyme = 1 and mis_elemento_desc3 = 'Banca Minorista')) then 'CC mino'  
        else 'CC mayo' end as segmento,  
sum (chk_dly_sld_acr) / 1000000 as saldo
from p_dw_explo.chk_dly as ca
left join p_dw_explo.chk_gen as gen
on ca.acct_nbr  = gen.acct_nbr and ca.acct_subsystem_id = gen.acct_subsystem_id
left join p_lk_explo.tva_arbol_segmento as seg 
on seg_code = seg.segment_id
left join p_lk_explo.producto_arbol as prod
on gen.prod_id = prod.prod_id
left join p_dw_explo.ms_cliente as hab
on hab.party_id = gen.party_id
left join p_dw_explo.cliente_datos datos
on gen.party_id = datos.party_id
left join p_dw_explo.party_mipyme as pyme 
on gen.party_id = pyme.party_id 
inner join (sel fecha_sla from p_dw_explo.calendario_servicios where ultimo_dia_habil_mes = 'Y' ) cal
on ca.chk_dly_bal_date = cal.fecha_sla
having saldo > 0
group by 1,2
    """
    Cuadro2 = pd.read_sql(query,conn)
    query = """sel
sav_mth_year as anio,
sav_mth_month as mes,
case when (acredita_haberes_fl = 1 and mis_elemento_desc3 not in ('Banca Mayorista')) then 'CA trans' 
           when (acredita_haberes_fl = 0  and mis_elemento_desc3 not in ('Banca Mayorista')) then 'CA no trans' 
        else 'CA no Mesa' end as segmento,       
sum (sav_mth_sld_acr) / 1000000 as saldo
from p_dw_explo.sav_mth as ca
left join p_dw_explo.sav_gen as gen
on ca.acct_nbr  = gen.acct_nbr and ca.acct_subsystem_id = gen.acct_subsystem_id
left join p_lk_explo.tva_arbol_segmento as seg 
on seg_code = seg.segment_id
left join p_lk_explo.producto_arbol as prod
on gen.prod_id = prod.prod_id
left join p_dw_explo.ms_cliente as hab
on hab.party_id = gen.party_id
where mis_elemento_desc6 not in ('institucionales' ,'Corporativas', 'Corporativa') and (anio between 2016 and 2019)
having saldo > 0
group by 1,2,3
order by 1 desc, 2 desc
    """
    Cuadro3 = pd.read_sql(query,conn)
    query = """sel
chk_mth_year as anio,
chk_mth_month as mes,
case when  ((codigo_tipo_persona = 'F' and mis_elemento_desc3 = 'Banca Minorista' )or ( flag_marca_mypyme = 1 and mis_elemento_desc3 = 'Banca Minorista')) then 'CC mino'  
        else 'CC mayo' end as segmento,  
sum (chk_mth_sld_acr) / 1000000 as saldo
from p_dw_explo.chk_mth as ca
left join p_dw_explo.chk_gen as gen
on ca.acct_nbr  = gen.acct_nbr and ca.acct_subsystem_id = gen.acct_subsystem_id
left join p_lk_explo.tva_arbol_segmento as seg 
on seg_code = seg.segment_id
left join p_lk_explo.producto_arbol as prod
on gen.prod_id = prod.prod_id
left join p_dw_explo.ms_cliente as hab
on hab.party_id = gen.party_id
left join p_dw_explo.cliente_datos datos
on gen.party_id = datos.party_id
left join p_dw_explo.party_mipyme as pyme 
on gen.party_id = pyme.party_id 
where anio between 2016 and 2019
having saldo > 0
group by 1,2,3
order by 1 desc, 2 desc
    """
    Cuadro4 = pd.read_sql(query,conn)
    query = """sel indicador_macro_fc, indicador_macro_vl from p_dw_explo.Indicadores_macroeconomicos
where indicador_macro_vl is not null and indicador_macro_cd = 'IPC'
    """
    IPC_Tabla = pd.read_sql(query,conn)    
    
    post2019 = pd.concat([Cuadro1,Cuadro2])
    pre2019 = pd.concat([Cuadro3,Cuadro4])
    
    
    pre2019["fecha"] = pre2019["anio"].map(str) + pre2019["mes"].map(str)
    
    
    post2019['anio'] = pd.DatetimeIndex(post2019['fecha']).year
    post2019['mes'] = pd.DatetimeIndex(post2019['fecha']).month
    post2019["fecha"] = post2019["anio"].map(str) + post2019["mes"].map(str)
    
    post2019 = post2019[post2019.anio != 2019]
    
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
        if producto[0:5] == "Cajas":
            Ponderando[producto] /= saldoCA
        else:
            Ponderando[producto] /= saldoCC
    
    return final, Ponderando

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
        if producto in ("CC mino","CA no trans"):
            for index in Output[producto].index:
                if (total + Output[producto][index] < 0.5) & (finished == False):
                    total += Output[producto][index]
                else:
                    Output[producto][index] = 0
                    finished = True
        else:
            for index in Output[producto].index:
                if (total + Output[producto][index] < 1) & (finished == False):
                    total += Output[producto][index]
                else:
                    Output[producto][index] = 0
                    finished = True
        
    return Output


def distribuyeCaida(caidas,Desarrollos,vida_promedio, ponderacion):
    
    nodos = np.arange(60)*30+30

    for producto in ["Cuentas Corrientes","Cajas de Ahorro Resto"]:
        CaidaAssist = caidas[(caidas["Cuenta"] == producto) & (caidas["Moneda"] == "ARS")]
        if producto == "Cuentas Corrientes":
            ResultadoAssist = [Desarrollos["CC mino"],
                               Desarrollos["CC mayo"]]
        else:
            ResultadoAssist = [Desarrollos["CA no Mesa"],
                               Desarrollos["CA trans"],
                               Desarrollos["CA no trans"]]
        
        Monto = CaidaAssist[30].values[0]
        CaidaAssist[30].values[0] = 0
        
        for desarrollo in ResultadoAssist:
            if desarrollo.name in ("CC mino","CA no trans"):
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

Resultado,ponderacion = ejecutoTriangulo(Caidas)

vida_promedio = CalcVidaPromedio(Caidas,'ARS','Activo')

distribuyeCaida(Caidas,Resultado,vida_promedio,ponderacion)


Titulos= pd.DataFrame(columns=Caidas.columns)



def nearest(items, pivot):
    
    "me devuelve la fecha mas cercana al item que estoy iterando:"
    return min(items, key=lambda x: abs(x - pivot))


query ="""sel * from P_DW_EXPLO.INDICADORES_MACROECONOMICOS WHERE INDICADOR_MACRO_CD = 'BADLAR-NA'
"""
badlar_serie=pd.read_sql(query,conn)
fecha_filtro = nearest(badlar_serie['indicador_macro_fc'],fecha_calculo)
badlar=badlar_serie.loc[badlar_serie['indicador_macro_fc']==fecha_filtro,'indicador_macro_vl'].iloc[0]

def Ons_activa(df1,df2):
    
    "carga archivo:"
    query = """sel fecha_informacion, d.tipo_producto_cd,a.especie,Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
    		   when b.moneda_cd = 'USD' then 'USD' 
    		   else 'ARS' end as Moneda, sum(tenencia2) as nominales, tasa_tipo_tx,base_calculo_cd, cupon_frecuencia_cd ,
        case when cupon_frecuencia_cd = 'ZC' then 'Cupon cero a descuento'
    		when cupon_frecuencia_cd = 'SA' then 180
    		when cupon_frecuencia_cd = 'QTR' then 90
    		when cupon_frecuencia_cd = 'PA' then 'Cupon cero'
    		when cupon_frecuencia_cd = 'NON' then 'Non'
    		when cupon_frecuencia_cd = 'MTH' then 30
    		when cupon_frecuencia_cd = 'DLY' then 1
    		when cupon_frecuencia_cd = 'BIM' then 60
    		ELSE '-' end as dias,
    	    fecha_cashflow_fc, tipo_cash_flow_cd, tasa_interes_rate,tasa_amortizacion_rate,spread_nu,Fecha_vencimiento_fc
    		from p_dw_explo.siaf_posiciones a 
    		left join p_dw_explo.especie_bono B on A.especie = B.rdss_mae_cd 
    		left join p_dw_explo.calendario_servicios c on a.fecha_informacion = c.fecha_sla 
    		left join p_dw_explo.especie_bono_cashflow d on b.especie_cd = d.especie_cd and fecha_cashflow_fc > fecha_informacion
    			where extract(year from Fecha_informacion) > 2019  
    			AND tipo_bono_tx  in ( 'Obligacion Negociable') and emisor_le_id in ( '2040814','2040956')
    			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15 order by 1 desc,3,9 asc
    				having nominales > 0
    				
    				 """
    ons_activa = pd.read_sql(query,conn)
        
    fecha_filtro = nearest(ons_activa['Fecha_Informacion'],fecha_calculo)
    ons_activa= ons_activa.loc[ons_activa['Fecha_Informacion']==fecha_filtro]
    
    titulos_unicos=ons_activa['Especie'].unique().tolist()
    ons_activa['bucket'] = (ons_activa['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    
    df = pd.DataFrame(columns=df1.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=ons_activa.loc[ons_activa['Especie']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        df_titulo.reset_index(drop=True,inplace=True)
        
        df.at[i,'Cuenta']=titulos_unicos[i]+'(ON Activa)'
        df.at[i,'Moneda']=df_titulo['Moneda'].all()
        df.at[i,'Lugar del balance']='Activo'
        
        tipo_tasa = df_titulo['tasa_tipo_tx'].all()
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        amortizacion_acum=0
        
        
        if dias=='Cupon cero a descuento':
            
            spread=float(0 if df_titulo.at[0,'spread_nu'] is None else df_titulo.at[0,'spread_nu'] )
            bucket = df_titulo.at[0,'bucket']
            
                    
            for col in df.columns:
            
                if isinstance(col, int) and col >= bucket:
                
                    if tipo_tasa=='Fija':
                        
                        df.at[i,col] = nominal
                        break
                    
                    elif tipo_tasa=='Badlar':
                        
                        tasa_interes_efectiva = pow( 1 + (badlar + spread)/100,bucket/360)-1
                        
                        df.at[i,col] = nominal*(1 + tasa_interes_efectiva)
                        break
                    else:
                        print(f'No se pudo extraer la ON Activa {titulos_unicos[i]} de forma correcta')
                        raise SystemExit()
            continue
                        
        else:
            dias= float(dias)
                            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per = (1 + df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100))-1
                spread=float(0 if df_titulo.at[fila,'spread_nu'] is None else df_titulo.at[fila,'spread_nu'])
                bucket = df_titulo.at[fila,'bucket']
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                
                
                for col in df.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        if tipo_tasa=='Fija':
                            
                            remanente=nominal*(1-amortizacion_acum)
                            df.at[i,col] = remanente*tasa_interes_efect_per + nominal*tasa_amort
                            amortizacion_acum+=tasa_amort
                            
                            break
                        
                        elif tipo_tasa=='Badlar':
                            
                            remanente=nominal*(1-amortizacion_acum)
                            df.at[i,col] = remanente*((badlar+spread)*dias/(360*100))+nominal*tasa_amort
                            amortizacion_acum+=tasa_amort
                            break
                        
                        else:
                            print(f'No se pudo extraer la ON Activa {titulos_unicos[i]} de forma correcta')
                            raise SystemExit()
    
    return df1.append(df,ignore_index=True),df2.append(df,ignore_index=True)
                      
                    
Caidas,Titulos=Ons_activa(Caidas,Titulos)                       
                    
def Titulos_no_trading(df1,df2):
    
    "carga archivo:"
    query = """sel fecha_informacion, d.tipo_producto_cd,a.especie,tipo_bono_tx, Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
		   when b.moneda_cd = 'USD' then 'USD' 
		   else 'ARS' end as Moneda,sum(tenencia2) as nominales, tasa_tipo_tx,base_calculo_cd, cupon_frecuencia_cd ,
    case when cupon_frecuencia_cd = 'ZC' then 'Cupon cero a descuento'
		when cupon_frecuencia_cd = 'SA' then 180
		when cupon_frecuencia_cd = 'QTR' then 90
		when cupon_frecuencia_cd = 'PA' then 'Cupon cero'
		when cupon_frecuencia_cd = 'NON' then 'Non'
		when cupon_frecuencia_cd = 'MTH' then 30
		when cupon_frecuencia_cd = 'DLY' then 1
		when cupon_frecuencia_cd = 'BIM' then 60
		ELSE '-' end as dias,
	    fecha_cashflow_fc, tipo_cash_flow_cd, tasa_interes_rate,tasa_amortizacion_rate,spread_nu,Fecha_vencimiento_fc
		from p_dw_explo.siaf_posiciones a 
		left join p_dw_explo.especie_bono B on A.especie = B.rdss_mae_cd 
		left join p_dw_explo.calendario_servicios c on a.fecha_informacion = c.fecha_sla 
		left join p_dw_explo.especie_bono_cashflow d on b.especie_cd = d.especie_cd and fecha_cashflow_fc > fecha_informacion
			where extract(year from Fecha_informacion) > 2019 and segmento IN ('FONDEO', 'LIQUIDEZ', 'BANCA' ) 
			AND tipo_bono_tx not in ('Obligacion Negociable', 'Fideicomiso') 
			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,5,7,8,9,10,11,12,13,14,15,16 order by 1 desc,3,9 asc
				having nominales > 0
    				 """
    titulos_sin_garantia = pd.read_sql(query,conn)
        
    fecha_filtro = nearest(titulos_sin_garantia['Fecha_Informacion'],fecha_calculo)
    titulos_sin_garantia= titulos_sin_garantia.loc[titulos_sin_garantia['Fecha_Informacion']==fecha_filtro]
    titulos_sin_garantia['bucket'] = (titulos_sin_garantia['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    
    titulos_sin_garantia.reset_index(drop=True,inplace=True)
    
    titulos_unicos=titulos_sin_garantia['Especie'].unique().tolist()
    
    df = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=titulos_sin_garantia.loc[titulos_sin_garantia['Especie']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        df_titulo.reset_index(drop=True,inplace=True)
        
        df.at[i,'Cuenta']=titulos_unicos[i]+' '+ df_titulo.at[0,'tipo_bono_tx']
        df.at[i,'Moneda']=df_titulo['Moneda'].all()
        df.at[i,'Lugar del balance']='Activo'
               
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        tipo_titulo = df_titulo['tipo_producto_cd'].all()
        amortizacion_acum=0
        
        if tipo_titulo=='BondMMDiscount':
            
            bucket = df_titulo.at[0,'bucket']
            
            for col in df.columns:
            
                if isinstance(col, int) and col >= bucket:
                    
                     df.at[i,col] = nominal
                     break
                 
        elif tipo_titulo=='Bond':
            dias= float(dias)
                            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per =  df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                
                bucket = df_titulo.at[fila,'bucket']
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                
                
                for col in df.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        df.at[i,col] = remanente*tasa_interes_efect_per + nominal*tasa_amort
                        amortizacion_acum+=tasa_amort
                        break
                        
                    elif isinstance(col, int) and col < bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        df.iat[0,-1] += remanente*tasa_interes_efect_per + nominal*tasa_amort
                        amortizacion_acum+=tasa_amort
                        break
                    
                    elif isinstance(col, str):
                        continue
                        
                    else:
                        print(f'No se pudo extraer el Titulo {titulos_unicos[i]} de forma correcta de Titulos sin garantia')
                        raise SystemExit()
                    
                        
    return df1.append(df,ignore_index=True),df2.append(df,ignore_index=True)
                      
                    
Caidas,Titulos=Titulos_sin_garantia(Caidas,Titulos)                        
                        

def Titulos_en_garantia(df1,df2):
    
    "carga archivo:"
    query = """sel fecha_informacion, d.tipo_producto_cd, a.especie, Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
		   when b.moneda_cd = 'USD' then 'USD' 
		   else 'ARP' end as Moneda, sum(-BLOQUEADOS) as nominales, tasa_tipo_tx,base_calculo_cd, cupon_frecuencia_cd ,
    case when cupon_frecuencia_cd = 'ZC' then 'Cupon cero a descuento'
		when cupon_frecuencia_cd = 'SA' then 180
		when cupon_frecuencia_cd = 'QTR' then 90
		when cupon_frecuencia_cd = 'PA' then 'Cupon cero'
		when cupon_frecuencia_cd = 'NON' then 'Non'
		when cupon_frecuencia_cd = 'MTH' then 30
		when cupon_frecuencia_cd = 'DLY' then 1
		when cupon_frecuencia_cd = 'BIM' then 60
		ELSE '-' end as dias,
	    fecha_cashflow_fc, tipo_cash_flow_cd, tasa_interes_rate,tasa_amortizacion_rate,spread_nu,Fecha_vencimiento_fc
		from p_dw_explo.siaf_posiciones a 
		left join p_dw_explo.especie_bono B on A.especie = B.rdss_mae_cd 
		left join p_dw_explo.calendario_servicios c on a.fecha_informacion = c.fecha_sla 
		left join p_dw_explo.especie_bono_cashflow d on b.especie_cd = d.especie_cd and fecha_cashflow_fc > fecha_informacion
			where extract(year from Fecha_informacion) > 2019 
			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,6,7,8,9,10,11,12,13,14,15 order by 1 desc,2,5 asc
				having nominales > 0
    				 """
    titulos_en_garantia = pd.read_sql(query,conn)
        
    fecha_filtro = nearest(titulos_en_garantia['Fecha_Informacion'],fecha_calculo)
    titulos_en_garantia= titulos_con_garantia.loc[titulos_en_garantia['Fecha_Informacion']==fecha_filtro]
    titulos_en_garantia['bucket'] = (titulos_en_garantia['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    
    titulos_con_garantia.reset_index(drop=True,inplace=True)
    
    titulos_unicos=titulos_con_garantia['Especie'].unique().tolist()
    
    df = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=titulos_con_garantia.loc[titulos_con_garantia['Especie']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        df_titulo.reset_index(drop=True,inplace=True)
        
        df.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(Titulo sin Garantia('
        df.at[i,'Moneda']=df_titulo['Moneda'].all()
        df.at[i,'Lugar del balance']='Activo'
        
        tipo_tasa = df_titulo['tasa_tipo_tx'].all()
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        tipo_titulo = df_titulo['tipo_producto_cd'].all()
        amortizacion_acum=0
        
        if tipo_titulo=='BondMMDiscount':
            
            bucket = df_titulo.at[0,'bucket']
            
            for col in df.columns:
            
                if isinstance(col, int) and col >= bucket:
                    
                     df.at[i,col] = nominal
                     break
                 
        elif tipo_titulo=='Bond':
            dias= float(dias)
                            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per = df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                spread=float(0 if df_titulo.at[fila,'spread_nu'] is None else df_titulo.at[fila,'spread_nu'])
                bucket = df_titulo.at[fila,'bucket']
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                
                
                for col in df.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        df.at[i,col] = remanente*tasa_interes_efect_per + nominal*tasa_amort
                        amortizacion_acum+=tasa_amort
                        break
                        
                    elif isinstance(col, int) and col < bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        df.iat[0,-1] += remanente*tasa_interes_efect_per + nominal*tasa_amort
                        amortizacion_acum+=tasa_amort
                        break
                    
                    elif isinstance(col, str):
                        continue
                        
                    else:
                        print(f'No se pudo extraer el Titulo {titulos_unicos[i]} de forma correcta de Titulos sin garantia')
                        raise SystemExit()
                    
                        
    return df1.append(df,ignore_index=True),df2.append(df,ignore_index=True)
                      
                    
Caidas,Titulos=Titulos_con_garantia(Caidas,Titulos)                        
                            
                                                    
def Ons_pasivo(df1,df2):
    
    "carga archivo:"
    query = """sel d.tipo_producto_cd,ticker_cd,Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
		   when b.moneda_cd = 'USD' then 'USD' 
		   else 'ARS' end as Moneda,  sum(total_emitido_nu) as nominales, tasa_tipo_tx,base_calculo_cd, cupon_frecuencia_cd ,
    case when cupon_frecuencia_cd = 'ZC' then 'Cupon cero a descuento'
		when cupon_frecuencia_cd = 'SA' then 180
		when cupon_frecuencia_cd = 'QTR' then 90
		when cupon_frecuencia_cd = 'PA' then 'Cupon cero'
		when cupon_frecuencia_cd = 'NON' then 'Non'
		when cupon_frecuencia_cd = 'MTH' then 30
		when cupon_frecuencia_cd = 'DLY' then 1
		when cupon_frecuencia_cd = 'BIM' then 60
		ELSE '-' end as dias,
	    fecha_cashflow_fc, tipo_cash_flow_cd, tasa_interes_rate,tasa_amortizacion_rate,spread_nu,fecha_emision_fc,Fecha_vencimiento_fc
		from p_dw_explo.especie_bono B  
		left join p_dw_explo.especie_bono_cashflow d on b.especie_cd = d.especie_cd 
			where emisor_le_id = '2040814' and ticker_cd<>'CGGAL' group by 1,2,3,5,6,7,8,9,10,11,12,13,14,15 order by 2 desc,9 asc 
				having nominales > 0  
    				 """
    ons_pasivo = pd.read_sql(query,conn)
    ons_pasivo['bucket'] = (ons_pasivo['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    ons_pasivo['spread_nu']= ons_pasivo['spread_nu'].fillna(0)
    ons_pasivo['fecha_devengamiento']=ons_pasivo['fecha_vencimiento_fc']-ons_pasivo['fecha_emision_fc']
     
    for i in range(len(ons_pasivo['fecha_devengamiento'])):
        ons_pasivo['fecha_devengamiento'][i]=ons_pasivo['fecha_devengamiento'][i].days
    
    titulos_unicos=ons_pasivo['TICKER_cd'].unique().tolist()
    
    df = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=ons_pasivo.loc[ons_pasivo['TICKER_cd']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        
        
        
        if df_titulo['fecha_vencimiento_fc'].max() < fecha_calculo or df_titulo['fecha_emision_fc'].max() > fecha_calculo:
            
            continue
        
        else:
            
            
            df_titulo.reset_index(drop=True,inplace=True)
            df.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(ON pasiva)'
            df.at[i,'Moneda']=df_titulo['Moneda'].all()
            df.at[i,'Lugar del balance']='Pasivo'
            
            tipo_tasa = df_titulo['tasa_tipo_tx'].all()
            dias = df_titulo['dias'].all()
            nominal=df_titulo.at[0,'nominales']/1000
            amortizacion_acum=0
            devengamiento=df_titulo.at[0,'fecha_devengamiento']
                                    
            if tipo_tasa=='Badlar' and dias=='Cupon cero a descuento':
                
                bucket = df_titulo.at[0,'bucket']
                tasa_amort= df_titulo.at[0,'tasa_amortizacion_rate']/100
                spread= float(df_titulo.at[0,'spread_nu'])
            
                for col in df.columns:
                
                    if isinstance(col, int) and col >= bucket:
                        
                        remanente=nominal*amortizacion_acum
                        df.at[i,col] = remanente*(1+(badlar+spread)*devengamiento/(100*360)) + nominal*tasa_amort
                        amortizacion_acum+=tasa_amort
                        break
                    
            elif tipo_tasa=='Fija' and dias=='Cupon cero a descuento':
                
                for col in df.columns:
                
                    if isinstance(col, int) and col >= bucket:
                        
                        
                        df.at[i,col] = nominal
                        
                        break
            elif tipo_tasa=='Fija' and dias!='Cupon cero a descuento':
                
                dias= float(dias)
                            
                for fila in range(len(df_titulo)):
                                    
                    tasa_interes_efect_per = df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                    spread=float(df_titulo.at[fila,'spread_nu'])
                    bucket = df_titulo.at[fila,'bucket']
                    tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                    
                    if bucket<0:
                        amortizacion_acum+=tasa_amort
                        continue
                                       
                    for col in df.columns:
                        
                        if isinstance(col, int) and col >= bucket:
                            
                            remanente=nominal*(1-amortizacion_acum)
                            df.at[i,col] = remanente*tasa_interes_efect_per + nominal*tasa_amort
                            amortizacion_acum+=tasa_amort
                            break
                        
            elif tipo_tasa=='Badlar' and dias!='Cupon cero a descuento':
                
                dias= float(dias)
                            
                for fila in range(len(df_titulo)):
                                  
                    
                    spread=float(df_titulo.at[fila,'spread_nu'])
                    bucket = df_titulo.at[fila,'bucket']
                    tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                    
                    if bucket<0:
                        amortizacion_acum+=tasa_amort
                        continue
                                       
                    for col in df.columns:
                        
                        if isinstance(col, int) and col >= bucket:
                            
                            remanente=nominal*(1-amortizacion_acum)
                            df.at[i,col] = remanente*(badlar+spread)*dias/(360*100) + nominal*tasa_amort
                            amortizacion_acum+=tasa_amort
                            break
            else:
                print(f'No se pudo extraer el Titulo {titulos_unicos[i]} de forma correcta de Ons Pasivo')
                raise SystemExit()
                    
                        
    return df1.append(df,ignore_index=True),df2.append(df,ignore_index=True)                    
                
            

Caidas,Titulos=Ons_pasivo(Caidas,Titulos)


def Leliqs(df1,df2):
    
    query="""select fecha_sla,siaf_alias_especie,fecha_vencimiento_fc, sum(nominal) as VN
    from p_dw_explo.calendario_servicios a
    left join p_dw_explo.SIAF_soporte_contable_pases  c on a.fecha_sla >= c.fec_operacion and a.fecha_sla < c.fec_vto
    left join p_dw_explo.especie_bono d on c.siaf_alias_especie = d.ticker_cd
    where habil = 1  and c.tipo_operacion = 'TIPA' and ultimo_dia_habil_mes = 'Y' and tipo_bono_tx = 'Letra de Liquidez BCRA'
    group by 1 ,2,3
    order by 1 desc"""
    
    leliqs = pd.read_sql(query,conn)
    
    leliqs = leliqs.groupby('SIAF_Alias_Especie', as_index=False)['VN'].sum()
    leliqs['SIAF_Alias_Especie'] = leliqs['SIAF_Alias_Especie'].str.strip()
    
    dic_mes = {'E':1,'F':2,'M':3,'A':4,'Y':5,'J':6,'L':7,'G':8,'S':9,'O':10,'N':11,'D':12}
    dic_año = {0:2020,1:2021,2:2022,3:2023,4:2024,5:2025,6:2026,7:2027,8:2028,9:2019}
    fecha_vencimiento=[]
    
    for leliq in leliqs['SIAF_Alias_Especie']:
                
        año= dic_año[int(leliq[-1])]
        mes= dic_mes[leliq[-2]]
        dia=int(leliq[1:3])
        fecha_vencimiento.append(datetime.date(año,mes,dia))
        
    leliqs['fecha_vencimiento']=fecha_vencimiento
    leliqs=leliqs.loc[leliqs['fecha_vencimiento']>fecha_calculo]
    
    leliqs['bucket'] = (leliqs['fecha_vencimiento']-fecha_calculo)/np.timedelta64(1,'D')
    leliqs.reset_index(drop=True,inplace=True)
    
    df = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(leliqs)):
        
        df.at[i,'Cuenta']=leliqs.at[i,'SIAF_Alias_Especie'] + '(Leliq)'
        df.at[i,'Moneda']='ARS'
        df.at[i,'Lugar del balance']='Activo'
        
        for col in df.columns:
            if isinstance(col, int) and col >= leliqs.at[i,'bucket']:
                df.at[i,col] = leliqs.at[i,'VN']
                break        
                        
    return df1.append(df,ignore_index=True),df2.append(df,ignore_index=True)                    
                
            

Caidas,Titulos=Leliqs(Caidas,Titulos)
                        

# %% Calculo Correlaciones de tasas



# Obtengo la fecha de corrida la cual sirve para filtrar la informacion


t0 = time.time()
 
type('2010-01-01 00:00:00')
def ImportoCurvas(fecha_max = fecha_calculo):
    """Importa el archivo de Curvas historicas de tasa,
        le realiza modificaciones al mismo:
            ordena las filas por tipo de curva y fecha
            elimino filas en caso de duplicados
            fitlro las fechas
        
        Por ultimo, crea un archivo con la informacion importante de las curvas,
        las cuales me van a servi para hacer los filtrados y generar los
        distintos DF para cada curva.           
    
    Parametro
        ----------
        Archivos : list
            Nombre de los 4 archivos input de caidas
    """
    
    df = pd.read_excel('Curva Tasas Historicas.xlsx') ###Cargo el Archivo
    tasasInput = df.copy()
    df["Curva"] = df[['Lugar_balance', 'Moneda']].agg(' '.join, axis=1) ### creo una nueva columna como combinacion de dos
    df.drop(['Lugar_balance', 'Moneda'], axis=1,inplace=True) ### dropeo las cols que no voy a usar
    df.sort_values(by=['Curva','Fecha'],inplace=True)  ##ordeno las curvas por nombre de curva y despues por fecha
    df.drop_duplicates(subset=['Fecha','Curva'],keep='first',inplace=True,ignore_index=True) ###elimino en caso de duplicados
    
    "Filtro fecha:"
    
    ####Filtrar en caso de cambiar la fecha
    df['Fecha'] = pd.to_datetime(df['Fecha']).dt.date
        
    df = df[df['Fecha'] <= fecha_max]
    df.reset_index(drop=True,inplace=True)


    "Creo un nuevo archivo con los parametros de las curvas:"

    df_data = pd.DataFrame(df['Curva'].drop_duplicates()) ###me quedo con las curvas unicas
    df_data['Indices']=df_data.index ###Creo una columna con los valores de los indices
    df_data.reset_index(drop=True,inplace=True)
    
    
    
    
    return df, df_data, tasasInput


# =============================================================================
# Funcion diferencia
# =============================================================================

def Diferencia(df,cols=['Fecha','Curva'],dias=90):
    
    """calcula la diferencia de "n" dias del dataframe en cuestion, lo realiza para
        cada columna numerica del df. Se setea en 90 dias ya que representa el holding period     
                   
    Parametros
        ----------
        df : DataFrame
            
    """
    
    df2=df.copy()
    df2.drop(cols,inplace=True, axis=1)
    df2=df2.diff(periods=dias,axis=0)
    df2.dropna(axis=0,how='any',inplace=True)
    df2.reset_index(drop=True,inplace=True)
    
    return df2

# =============================================================================
# Tests de correlacion
# =============================================================================

def Test_correlacion (df1,df2):
    
    """Realiza la prueba de correlaciones entre dos dataframes (Curvas).     
                   
    Parametros
        ----------
        df : DataFrame
            
    """
    
    corr = df1.corrwith(df2,axis=1)
    total = corr.count()
    positivo = corr[corr > 0.5].count()/total ##en porcentaje
    negativo = corr[corr < -0.5].count()/total ###en porcentaje
    neutro = corr[corr.between(-0.5,0.5)].count()/total
    
    
    if positivo > 0.7:
        
        correlacion = 'positiva'
        
    elif negativo > 0.7:
        
        correlacion = 'negativa'    
    
    else:
        
        correlacion = 'no hay'
        
    return correlacion,positivo,neutro,negativo    
        
# =============================================================================
# Separo los data frames
# =============================================================================


def Separo_DataFrames (df,df_data):
    
    dic_curvas={}
    Indices=df_data['Indices']

    for i,curva in zip(range(len(Indices)),df_data.Curva):
        
        try:
            
            dic_curvas[curva]=df.iloc[Indices[i]:(Indices[i+1]),:]
            
        except:
            dic_curvas[curva] = df.iloc[Indices[i]:,:]
            
    return dic_curvas
            
# =============================================================================
# Calculo de las correlaciones
# =============================================================================

def Calculo_correlaciones (diccionario_df):
    
    """Itera por cada Curva del diccionario y realiza todas las combinaciones posibles
    para testear las correlaciones entre las mismas. Genera un DataFrame el cual
    se indica si las curvas estan o no correlacionadas.     
                   
    Parametro
        ----------
        diccionario_Df : diccionario
            contiene todas las curvas, donde cada nombre de la llave dentro del
            diccionario es el nombre de la curva correspondiente.
    """
    
    dic_curvas = diccionario_df.copy()
    
    """Genero las listas donde voy almacendando los resultados de las correlaciones:"""
    
    Curva1 = []
    Curva2 = []
    Correlacion = []
    Positivo = []
    Neutro = []
    Negativo = []
    lista_keys=list(dic_curvas.keys())
    
    """inicio el for loop donde voy a estar iterando entre los dataframes dentro del diccionario"""
    
    for j in range(len(dic_curvas)-1): 
        
        for i in range(j+1,len(dic_curvas)):
            
            df1 = dic_curvas[lista_keys[j]] ###agarro el dataframe 1
            df2 = dic_curvas[lista_keys[i]] ### agarro el dataframe 2
            
            if len(df1)<=len(df2): ###necesito asegurarme que los dfs tienen el mismo largo            
                
                df2 = df2[df2['Fecha'].isin(df1['Fecha'])]
            else:
                df1 = df1[df1['Fecha'].isin(df2['Fecha'])]
            
            df1 = Diferencia(df1)  ### genero las diferencias dentro de cada df
            df2 = Diferencia (df2) 
            
            """realizo la correlacion entre ambas curvas y almaceno sus resultados"""
            
            correlacion,positivo,neutro,negativo = Test_correlacion(df1,df2)
            
            Curva1.append(lista_keys[j])
            Curva2.append(lista_keys[i])
            Correlacion.append(correlacion)
            Positivo.append(positivo)
            Neutro.append(neutro)
            Negativo.append(negativo)
        
    dic = {'Curva 1':Curva1, 'Curva 2': Curva2, 'Correlacion':Correlacion,
           'Positivo': Positivo,'Neutro':Neutro,'Negativo':Negativo}

    df = pd.DataFrame.from_dict(dic)

    return df    
        
        
# =============================================================================
# Ejecucion del codigo
# =============================================================================

Curvas,info_curvas, tasasInput = ImportoCurvas()

Dic_curvas = Separo_DataFrames(Curvas, info_curvas) 

correlaciones = Calculo_correlaciones(Dic_curvas)

t1 = time.time()

        
    

    



start = time.time()

# %% Funciones para las Simulacion de Tasas de Interes


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


def DiferenciaNodos(df0,HoldingP):
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
            df1[nodo] = df1[nodo].diff(HoldingP)
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


def ValorActualiza(Caidas, Tasas, M=1000):
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
        Actualizados.append(actualizaCaida(Caidas.sum(axis=0), Tasas.values[i]))

    return Actualizados


def actualizaCaida(Caida, Tasas):
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
        if Caida[i] != 0:
            ValorActual += Caida[i] / (1+Tasas[i]) ** ((i*30+30) / 360)
    return ValorActual

# %% Funcion que actualiza las distintas aperturas de caidas


def loopActualiza(Caidas, curva_tasas, nodosTasas, correlaciones,años = 100,holding_period = 90, M = 1000):
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
    fecha_minima = fecha_calculo-datetime.timedelta(days=365*años)
    curva_tasas=tasasInput.copy()
    curva_tasas.Fecha = curva_tasas.Fecha.apply(lambda x: x.date())   
    curva_tasas=curva_tasas.loc[curva_tasas['Fecha']>fecha_minima,:]
    
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
                                                          tasasInput.columns.values.tolist()[1:len(tasasInput.columns)-2]],
            holding_period)
            
            if (LugarBalance== "Activo") & (Moneda == "CER"):
                dfDiferencia.drop([60,90,120],axis=1,inplace=True)
            if (LugarBalance== "Pasivo") & (Moneda == "USD") & (años > 5):
                dfDiferencia.drop([900,1260,1620],axis=1,inplace=True)
            if (LugarBalance== "Pasivo") & (Moneda == "ARS") & (años > 5):
                dfDiferencia.drop([450,540,1620],axis=1,inplace=True)
            
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
            
            ValorActual_assist[Moneda] = ValorActualiza(
                Caidas.fillna(0)[(Caidas["Moneda"] == Moneda) & 
                                 (Caidas['Lugar del balance'] == LugarBalance)].values[:,4:], 
                Tasas,
                M)
            
        ValorActual[LugarBalance] = ValorActual_assist
        sims[LugarBalance] = sims_assist
    return ValorActual, dicSimulacionesCorr, sims, curva_tasas


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
        
    netos["ID"] = netos.index
    netos.sort_values(["Total"],inplace=True)
    
    Media = np.average(netos["Total"])
    
    Percentil999 = np.quantile(netos["Total"],0.001)
    Percentil995 = np.quantile(netos["Total"],0.005)
    Percentil99 = np.quantile(netos["Total"],0.01)
    
    
    CapitalEconomico999 = Media - Percentil999
    CapitalEconomico995 = Media - Percentil995
    CapitalEconomico99 = Media - Percentil99
    
    CapitalEconomico = {"CE 99.9" : CapitalEconomico999*1000,
                        "CE 99.5" : CapitalEconomico995*1000,
                        "CE 99.0" : CapitalEconomico99*1000}
    
    return CapitalEconomico

def curvasRelevantes(Resultados,SimulacionesTasas,LB,TS,M):
    
    Percentil999 = Resultados.iloc[M//1000,-1]
    Percentil995 = Resultados.iloc[M//200,-1]
    Percentil99 = Resultados.iloc[M//100,-1]
    Percentil50 = Resultados.iloc[M//2,-1]
    
    Curvas = {}
    
    for i in LB:
        for j in TS:
            Curvas[i + j + "999"] = SimulacionesTasas[i][j].iloc[Percentil999]
            Curvas[i + j + "995"] = SimulacionesTasas[i][j].iloc[Percentil995]
            Curvas[i + j + "99"] = SimulacionesTasas[i][j].iloc[Percentil99]
            Curvas[i + j + "50"] = SimulacionesTasas[i][j].iloc[Percentil50]
    
    return Curvas





# %% Inputs del modelo


end = time.time()
print(f'el codigo tarda {end - start:.2f} segundos en definir las funciones')
start = time.time()


tasasInput.columns = ["Fecha", 30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900,
                      1080, 1260, 1440, 1620, 1800, 2160, 2520, 2880, 3240, 3600, 'Lugar del balance', "Moneda"]

tasasInput.drop([2160, 2520, 2880, 3240], axis=1, inplace=True)

for nodo in tasasInput:
    if nodo == "Fecha" or nodo == 'Lugar del balance' or nodo == "Moneda":
        continue
    else:
        tasasInput[nodo] = tasasInput[nodo] / 100

end = time.time()

print(
    f'el codigo tarda {end - start:.2f} segundos en realizar el input de tasas y caidas')
start = time.time()

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados

nodosTasas = np.arange(120) * 30 + 30
simulaciones = 10000
HoldingPeriod = 90
historia_variaciones = 10
LB = pd.unique(Caidas['Lugar del balance'])
TS = pd.unique(Caidas["Moneda"])
d = {'Lugar del balance':["Activo","Activo","Activo","Pasivo","Pasivo","Pasivo"],
     "Moneda":["ARS","USD","CER","ARS","USD","CER"],
     "Grupo":["A","B","C","A","D","C"]}
correlaciones = pd.DataFrame(d)

ValoresActuales,ShocksAleatorios,SimulacionesTasas,Tasas_calculo = loopActualiza(Caidas,
                                                                                 tasasInput, 
                                                                                 nodosTasas, 
                                                                                 correlaciones, 
                                                                                 historia_variaciones,
                                                                                 HoldingPeriod,
                                                                                 simulaciones)

print()
print("==============================")

cotizaUSD = 110

neto = neteoAP(ValoresActuales, TS, cotizaUSD)

CapitalEcon = Capitales(neto,TS)

Curvas = curvasRelevantes(neto, SimulacionesTasas, LB, TS, simulaciones)

dfCurvas = pd.DataFrame(Curvas,columns=(Curvas.keys()))
dfCurvas['Nodo'] = dfCurvas.index

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS50', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS50', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD50', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD50', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER50', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER50', ax=ax,xticks = xline)

plt.show()


# %% Time report

end = time.time()
print(
    f'el codigo tarda {(end - start)/60:.2f} minutos en correr {simulaciones} simulaciones para todas las tasas')
