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
from datetime import timedelta 
from decimal import Decimal
start = time.time()

conn = pyodbc.connect("""Driver=Teradata; DBCName=DATA1N1; 
                      MechanismName=LDAP; UID=l1001069;
                      PWD=Nathsu2022!!""")

conn.setencoding(encoding='latin1')
conn.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
conn.setdecoding(pyodbc.SQL_WCHAR, encoding = 'latin1')

"""Parametros"""
cd_sesion = 1
nodosTasas = np.arange(120) * 30 + 30
simulaciones = 10000
HoldingPeriod = 30
historia_variaciones = 1
AjusteDesvio = 1
FlagTriangulo = 0


def Fecha(df):
    """Extrae la fecha del dataframe correspondiente a partir de los valores de las columnas
        La funcion me devuelve el mes y año del dataframe correspondiente:
    
    Parametro
        ----------
        df : DataFrame el cual le voy a extraer la fecha
            
    """
    "genero las listas en las cuales voy a ir anexando las fechas de cada columna iterada"
    lista_mes=[]            # lista del mes    
    lista_año=[]            # lista con el año
    
    "genero el loop el cual va a estar iterando por las columnas del df:"
    
    for i in range(len(df.columns)):
        
        if isinstance(df.columns[i], datetime.datetime)==True:   # filtro para quedarme con aquellas que tengan formato fecha
            
            meses=0         # seteo el parametro mes en "0"
            while ((df.columns[i].month-(i-1))+meses*(12))<=0:   # genero un while tal que le suma un n veces el mes si detecta que la dif es negativa
                meses+=1
            else:
                mes = df.columns[i].month-(i-1) +12*meses   # extraigo el mes   
                año = df.columns[i].year - meses   #extraigo el año      
                lista_mes.append(mes)   #lo inserto en la lista mes
                lista_año.append(año)   #lo inserto en la lista año
        else:
            continue   # si la columna no tiene formato fecha, entonces que pase a la siguiente columna
            
    "la siguiente seccion testea si los elementos dentro de cada fila son iguales entre si:"
        
    if len(set(lista_mes))==1 and len(set(lista_año))==1:
        mes = lista_mes[0]
        año = lista_año[0]
        return mes,año   # en caso de que contengan los mismos elementos, me quedo con el elemento de cada lista
    else:
        print(f'El arhcivo {df} no pasa el control de contener las mismas fechas')   # control en caso de que difieran los elementos
        raise SystemExit()
        
                                         
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
    
    
    df=df.rename(columns={df.columns[0]: 'Cuenta'})    # renombra la priemr columna        
    df['Cuenta']=df['Cuenta'].str.strip()   # le quita los espacios
    df2 = df[df.columns[~df.columns.isin(cols)]]   # filtra el df
    df2.columns = [30*(i+1) for i in range(len(df2.columns))]   # genera bucktes cada 30 dias
    df=df[cols].join(df2)   # genera el merge entre ambos dataframes
    
    return df

def ImportaArchivos (años_caidas=10):
    """Importa los archivos, genera las columnas Moneda y Lugar Balance
        segun el nombre con el que viene y se les realiza las siguientes transformaciones:
            renombro las columnas
            obtiene las cuentas unicas del respectivo dataframe
            calcula los interes para cada cuenta filtrada
            obtiene la fecha del dataframe
            
            
    
    Parametro
        ----------
        años_caidas : int
            maximo de años a considerar para las caidas
    """
        
    Lista_caidas_capital = []   # genero una lista vacia donde voy a ir anexando los dfs importados
    Lista_caidas_intereses = []   # genero una lista vacia donde voy a ir anexando los dfs importados 
    Lista_caidas_capital_e_intereses = []   # genero una lista vacia donde voy a ir anexando los dfs importados
    Lista_df = []   # genero una lista vacia donde voy a ir anexando los dfs importados
    ListaMes = []   # genero una lista vacia donde voy a ir anexando el mes de cada df
    ListaAño = []   # genero una lista vacia donde voy a ir anexando el año de cada df
    Archivos = ['Activo $','Activo U$S','Pasivo $','Pasivo U$S']   # genero una lista con los nombres de los dfs
    
    "genero una lista con los nombres de la columna que no representan una cuenta:"
    
    Lista_descarte_filas=['Total Assets','Total Liab & Equity','Difference',np.NaN,
                          'WART','Maturity Timing','Repricing Spread','Period Cap',
                          'Period Floor','Lifetime Cap','Lifetime Floor',
                          'Beg Book Balance', 'Beg Book Rate', 'Runoff Balance Book',
                          'Runoff Yield Book','Repricing Balance','Repricing Rate']
    
    "Importo la tabla con los parametros de cada cuenta:"
    
    parametros_caidas = pd.read_excel('Parametros Caidas (1).xlsx')  
    
    " realizo el loop el cual va a iterar por cada nombre de archivo de la lista Archivos:"    
    for archivo in tqdm(Archivos):   # itero por cada nombre de archivo de la lista
        
        df = pd.read_excel('{}.xls'.format(archivo))   # cargo el archivo
        
        """Calculo la fecha de calculo en base al arhcivo input y lo
            condenso en una lista
        """              
        mes,año=Fecha(df)   # filtro la fecha de corrida utilizando la funcion Fecha
        ListaMes.append(mes)
        ListaAño.append(año)
                        
        """Les inserto las columnas Moneda y lugar de balance segun el tipo de archivo:"""
        
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
            
        """Llamo a la funcion RenmbraColumnas para modificar el df:"""    
            
        df=RenombraColumnas(df)   
               
        df.drop('Total',axis=1,inplace=True)   # dropeo la columna total

        """Calculo un nuevo df donde cada fila es una cuenta unica del archivo input"""
        
        df2=df[~df['Cuenta'].isin(Lista_descarte_filas)]   # se queda con aquellas filas que el nombre no este en la lista creada previamente
        
        """Realizo una union entre el dataframe con las cuentas unicas y el archivo input
            de los parametros, para de esta forma obtener los parametros de cada cuenta"""
                
        df3 = pd.merge(df2.reset_index(), parametros_caidas,  how='left', on=['Cuenta','Moneda','Lugar del balance']).set_index('index')
        
        """Reordeno el orden de las columnas para que aquellas que contengan informacion
        de los parametros queden al inicio"""
        
        Flag_variable = df3.pop('Flag variable')   # con .pop se extrae dicha columna del df
        Flag_cuenta=df3.pop('Flag cuenta')
        Capitalizacion_tasa=df3.pop('Capitalizacion tasa')
        df3.insert(3,'Capitalizacion_tasa',Capitalizacion_tasa)   # con .insert se inserta la columna en la posicion deseada
        df3.insert(3,'Flag_cuenta',Flag_cuenta)
        df3.insert(3,'Flag_variable',Flag_variable)
        
        
        """Aplico un control tal que detecte que haya cuentas nuevas que no esten
        contemplados dentro del archivo input de Parametros caidas"""

        nuevas_cuentas=df3[df3['Capitalizacion_tasa'].isna() | 
                           df3['Flag_variable'].isna() | 
                           df3['Flag_cuenta'].isna() ]
        nuevas_cuentas=nuevas_cuentas.iloc[:,0:3]   # si se detecta que algun campo es nulo posterior al merge, entonces implica que es una cuenta nueva

        """Si no encuentra el valor de los parametros cuando se realiza el join
        entre caidas y parametros, el valor de dicho parametro sera nulo.
        Esto implica que es una cuenta nueva, o que no se encuentra en el archivo input.
        Si tal es el caso, los considero dentro del modelo pero les asumo unos parametros predeterminados"""

        relleno_valores_nulos={'Flag_variable':0,'Flag_cuenta':1,'Capitalizacion_tasa':12}   # seteo los parametros de aquellas cuentas nuevas detectadas
        df3.fillna(value=relleno_valores_nulos,inplace=True)   # reemplazo los valores nulos por los parametros seteados
        
        """Elimino las filas que no voy a considerar (Flag_cuenta = 0):"""
        
        df3.drop(df3[df3['Flag_cuenta'] == 0].index, inplace = True)   # dropeo las filas que novan a considerarse dentro del calculo
        df3.drop('Flag_cuenta',axis=1,inplace=True)
        df3.loc[df3['Flag CER']==1,'Moneda']='CER'
        df3.drop('Flag CER',axis=1,inplace=True)
        
        " genero los 3 dataframes unicos, capital, capital e interses y solo intereses:"
        
        caidas_capital = df3.copy()   # df el cual va a contener unicamente las caidas de capital
        caidas_intereses = df3.copy()   # df el cual va a contener unicamente las caidas de intereses
        caidas_capital_e_intereses = df3.copy()   # df el cual va a contener capital + intereses
        
        """Itero por cada cuenta unica para calcularle los intereses"""
        
        for i in df3.index:
            per=df3.at[i,'Capitalizacion_tasa']   # obtengo la periodicidad de dicha cuenta
            
            for col in df3:   # realizo el loop por cada columna del dataframe
                
                try:
                    
                    if type(col)!=int:  # si la columna no es int, que pase a la siguiente
                        continue
                    
                    elif df.at[i+2,'Cuenta']!= 'Beg Book Rate':   # si el valor de la fila+2 de la col Cuenta no es beg book rate
                        
                        caidas_capital.at[i,col] = df.at[i+1,col]
                        caidas_intereses.at[i,col] = 0
                        caidas_capital_e_intereses.at[i,col] = df.at[i+1,col]
                        
                    elif df.at[i+2,'Cuenta']== 'Beg Book Rate' and df.at[i+4,'Cuenta']!= 'Runoff Balance Book':
                        
                        caidas_capital.at[i,col] = df.at[i+1,col]
                        caidas_intereses.at[i,col] = 0
                        caidas_capital_e_intereses.at[i,col] = df.at[i+1,col]
                        
                    elif df.at[i+4,'Cuenta']== 'Runoff Balance Book' and df.at[i+5,'Cuenta']== 'Runoff Yield Book':
                        
                        tna=df.at[i+5,col]/100   # tasa nominal anual
                        tea=pow(1+tna*per/12,12/per)-1   # tasa efectiva anual
                        tep=pow(1+tea, col/360)-1   # tasa efectiva plazo
                        
                        caidas_capital.at[i,col] = df.at[i+4,col]
                        caidas_intereses.at[i,col] = df.at[i+4,col]*tep
                        caidas_capital_e_intereses.at[i,col] = df.at[i+4,col]*(1+tep)
                        
                        
                    elif df.at[i+4,'Cuenta']== 'Runoff Balance Book' :
                        
                        caidas_capital.at[i,col] = df.at[i+4,col]
                        caidas_intereses.at[i,col] = 0
                        caidas_capital_e_intereses.at[i,col] = df.at[i+4,col]
                        
                    else:
                        
                        caidas_capital.at[i,col] = -1
                        caidas_intereses.at[i,col] = -1
                        caidas_capital_e_intereses.at[i,col] = -1
                        print('Las caidas de uno o mas cuentas no ha sido posible ser calculadas')
                        raise SystemExit()
                
                except:   # este expect se genera debido a que las ultimas cuentas del df no tienen fila+n>2
                    caidas_capital.at[i,col] = df.at[i+1,col]
                    caidas_intereses.at[i,col] = 0
                    caidas_capital_e_intereses.at[i,col] = df.at[i+1,col]
            
            """ Luego de haber iterado por cada columna y haber realizado la caida de capital e interes,
                llevo el total de la caida al bucket de 30 dias para aquellas cuentas variables:"""
                
            columna = caidas_capital.columns.get_loc(30)   # obtengo el indice de la columna/ bucket 30
            fila = caidas_capital.index.get_loc(i)   # obtengo la posicion del indice/ fila que me encuentro iterando
            
            if df3.at[i,'Flag_variable']==1:   # detecta si la cuenta es variable y le aplico los cambios correspondientes:
            
                suma = caidas_capital.iloc[fila,columna:].sum()   #  calculo la sumatoria de la caida de la cuenta
                caidas_capital.at[i,30] = suma   # le inserto esa suma al primer bucket 
                caidas_capital.iloc[fila,(columna+1):] = 0   # transformo el resto de los buckets en "0" para no duplicar el efecto
                
                caidas_capital_e_intereses.at[i,30]=suma 
                caidas_capital_e_intereses.iloc[fila,(columna+1):] = 0
                
                caidas_intereses.at[i,30]=suma 
                caidas_intereses.iloc[fila,(columna+1):] = 0
                      
        """topeo las caidas en el max de años, al ultimo bucket le sumo las caidas posteriores a ese bucket:"""
        
        max_caidas = años_caidas*360   # ultimo bucket de las caidas
        indice = caidas_capital_e_intereses.columns.get_loc(max_caidas)    # obtengo la posicion de ese ultimo bucket/columna
        
        caidas_capital_e_intereses[max_caidas] = caidas_capital_e_intereses.iloc[:,indice:].sum(axis=1)   # le cargo al ultimo bucket todo lo que resta por caer de dicha cuenta
        caidas_capital_e_intereses = caidas_capital_e_intereses.iloc[:,:(indice+1)]   # trunco el dataframe  
        
        caidas_intereses[max_caidas]=caidas_intereses.iloc[:,indice:].sum(axis=1) 
        caidas_intereses=caidas_intereses.iloc[:,:(indice+1)]
        
        caidas_capital[max_caidas] = caidas_capital.iloc[:,indice:].sum(axis=1) 
        caidas_capital = caidas_capital.iloc[:,:(indice+1)]
        
        """Con el dataframe ya filtrado, lo inserto en las respectivas listas para luego generar un dataframe unico:"""
        
        Lista_caidas_capital.append(caidas_capital) 
        Lista_caidas_intereses.append(caidas_intereses)
        Lista_caidas_capital_e_intereses.append(caidas_capital_e_intereses)
        Lista_df.append(df)
    
    """Obtengo la fecha de calculo de la corrida, la cual sera aquella que resulte de los 4 archivos importrados con las caidas"""
    
    if len(set(ListaMes))==1 and len(set(ListaAño))==1:   # verifico que las 4 fechas de los 4 arhcivos coincidan
        mes=ListaMes[0]
        año= ListaAño[0]
        dia = calendar.monthrange(año, mes)[1]   # para que sea el ultimo dia del mes de corrida
        fecha = datetime.date(año,mes,dia)   # genero la fecha de calculo con el formato correspondiente
    else:
        print('Los 4 archivos no contienen las mismas fechas')   # control en caso de que no contengan las mismas fechas los archivos
        raise SystemExit()
        
    """Con los 4 dataframes ya importados y transformados, los unifico en un unico DF:"""
    
    Caidas = pd.concat(Lista_caidas_capital_e_intereses, ignore_index=True)   # los concateno
    Caidas.fillna(value=0,inplace=True)   # relleno los espacios en blanco con ceros
    
    Caidas_capital = pd.concat(Lista_caidas_capital, ignore_index=True)
    Caidas_capital.fillna(value=0,inplace=True)
    
    Caidas_intereses = pd.concat(Lista_caidas_intereses, ignore_index=True)
    Caidas_intereses.fillna(value=0,inplace=True)
    
    dfs = pd.concat(Lista_df, ignore_index=True)   
        
    return fecha,Caidas,Caidas_capital,Caidas_intereses,dfs   # la funcion me devuelve el archivo caidas condensado y la fecha de calculo

fecha_calculo, Caidas,Caidas_capital,Caidas_intereses,dfs= ImportaArchivos()


def CalcVidaPromedio (df ,FiltroMoneda,FiltroBalance,cols=['Cuenta','Total','Moneda','Lugar del balance','Capitalizacion_tasa','Flag_variable']):
    
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
               
    df2 = df2.loc[(df2['Moneda']==FiltroMoneda) & (df2['Lugar del balance']==FiltroBalance)]   # me qeudo con la tabla filtrada
    df2 = df2[df2.columns[~df2.columns.isin(cols)]]   # elimino las columnas que no son numericas
    df2=df2.loc[df2.sum(axis=1) != 0]   # dropeo aquellas filas que sean nulas
    df2.reset_index(drop=True,inplace=True)
    Suma_producto=[]  
    Suma_total=[]
    for fila in range(len(df2)):   # genero el for loop para que itere por cada cuenta y le caucle la vida promedia
        SerieCuenta = df2.iloc[fila,:].dropna() 
        Suma_producto.append(SerieCuenta.dot(SerieCuenta.index))   # calculo la sumaproducto [valor*bucket] y lo agrego a una lista
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
                
    

Caidas = CaidasTarjeta(Caidas)
Caidas_capital = CaidasTarjeta(Caidas_capital)
Caidas_intereses = CaidasTarjeta(Caidas_intereses)



def importaTriangulo():
    print("Cargando CC Post2019")
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
    print("Cargando CA Post2019")
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
    print("Cargando CC Pre2019")
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
    print("Cargando CA Pre2019")
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
    IPC_Tabla = pd.read_sql(query,conn)    # Importa serie de IPC
    
    post2019 = pd.concat([Cuadro1,Cuadro2])     # Junta las tablas de post 2019
    pre2019 = pd.concat([Cuadro3,Cuadro4])      # Junta las tablas de pre 2019
    
    
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
                elif finished == False:
                    Output[producto][index] = 0.5-total
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
            ResultadoAssist = [Desarrollos["CC mino"],
                               Desarrollos["CC mayo"]]
        elif producto == "Cajas de Ahorro Resto":
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

if FlagTriangulo == 1:
    Resultado,ponderacion = ejecutoTriangulo(Caidas)

else:
    Resultado = pd.read_excel("DesarrollosTriangulo.xlsx",index_col=0)
    ponderacion = {'CC mino': 0.314887811651208, 
                   'CC mayo': 0.6851121883487921, 
                   'CA no Mesa': 0.01873675956500111, 
                   'CA trans': 0.5416886055738716, 
                   'CA no trans': 0.4395746348611273}


vida_promedio = CalcVidaPromedio(Caidas,'ARS','Activo')

distribuyeCaida(Caidas,Resultado,vida_promedio,ponderacion)

Titulos= pd.DataFrame(columns=Caidas.columns)



def nearest(items, pivot):
    
    "me devuelve la fecha mas cercana al item que estoy iterando:"
    return min(items, key=lambda x: abs(x - pivot))

def Badlar():
    """Obtiene la tasa badlar a la fecha de calculo (o la mas cercana a dicha fecha)"""
    
    query ="""sel * from P_DW_EXPLO.INDICADORES_MACROECONOMICOS WHERE INDICADOR_MACRO_CD = 'BADLAR-NA'"""   # realizo la query
    
    badlar_serie=pd.read_sql(query,conn)   # obtengo la tabla de teradata
    badlar_serie.sort_values(by=['indicador_macro_fc'],inplace=True)   # ordeno la tabla por fecha
    fecha_filtro = nearest(badlar_serie['indicador_macro_fc'],fecha_calculo)   # obtengo la fecha mas cercana a la fecha de calculo
    
    badlar=badlar_serie.loc[badlar_serie['indicador_macro_fc']==fecha_filtro,'indicador_macro_vl'].iloc[0]   # obtengo la tasa badlar correspondiente
    
    return badlar

badlar = Badlar()

def Ons_activa(df1,df2,df3,df4):
    
    "carga archivo:"
    query = """sel fecha_informacion,fecha_emision_fc ,d.tipo_producto_cd,a.especie,Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
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
    			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,5,7,8,9,10,11,12,13,14,15,16 order by 1 desc,3,9 asc
    				having nominales > 0
    				
    				 """
    ons_activa = pd.read_sql(query,conn)
        
    fecha_filtro = nearest(ons_activa['Fecha_Informacion'],fecha_calculo)   # obtengo la fecha mas cercana a la fecha de calculo
    ons_activa= ons_activa.loc[ons_activa['Fecha_Informacion']==fecha_filtro]   # filtro los titulos por fecha de calculo
    
    titulos_unicos=ons_activa['Especie'].unique().tolist()   # genero una lista con los titulos unicos
    ons_activa['bucket'] = (ons_activa['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')   # calculo los dias hasta vencimiento del pago
    
    "genero los 3 dataframes en el cual voy a ir cargando los titulos"
    
    df_capital_e_intereses = pd.DataFrame(columns=Caidas.columns)
    df_capital = pd.DataFrame(columns=Caidas.columns)
    df_intereses = pd.DataFrame(columns=Caidas.columns)
    
    "genero un loop el cual va a estar iterando por la lista de titulos unicos:"
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=ons_activa.loc[ons_activa['Especie']==titulos_unicos[i],:]   # genero un df con el titulo correspondiente
        df_titulo=df_titulo.sort_values(by='bucket')   # ordeno los vencimientos de mas cercano a mas lejano
        df_titulo.reset_index(drop=True,inplace=True)   # reseteo el indice para poder iterar
        
        " seteo los parametros a incorporar al DF general con el nombre de la cuenta, moneda y lugar de balance:"
        
        df_capital_e_intereses.at[i,'Cuenta']=titulos_unicos[i]+'(ON Activa)'
        df_capital_e_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital_e_intereses.at[i,'Lugar del balance']='Activo'
        
        df_capital.at[i,'Cuenta']=titulos_unicos[i]+'(ON Activa)'
        df_capital.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital.at[i,'Lugar del balance']='Activo'
        
        df_intereses.at[i,'Cuenta']=titulos_unicos[i]+'(ON Activa)'
        df_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_intereses.at[i,'Lugar del balance']='Activo'
        
        " obtengo el tipo de tasa, la periodicidad y el nominal del titulo correspondiente:"
        
        tipo_tasa = df_titulo['tasa_tipo_tx'].all()
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        amortizacion_acum=0
        
        " obtengo el interes y capital de cada flujo de la especie dependiendo el tipo de tasa que es:"
        
        if dias=='Cupon cero a descuento':
            
            spread=float(0 if df_titulo.at[0,'spread_nu'] is None else df_titulo.at[0,'spread_nu'] )
            bucket = df_titulo.at[0,'bucket']   # fecha de vencimiento
            
            for col in df_capital_e_intereses.columns:   # itero por cada columna del df
            
                if isinstance(col, int) and col >= bucket:   # si la columna es numerica (bucket) y mayor igual a la fecha de vencimiento, entonces implica que la caida se ingresa en dicho bucket"
                
                    if tipo_tasa=='Fija':
                        
                        capital = nominal
                        interes = 0
                    
                    elif tipo_tasa=='Badlar':
                        
                        tasa_interes_efectiva = pow( 1 + (badlar + spread)/100,bucket/360)-1
                        capital = nominal
                        interes = tasa_interes_efectiva*nominal
                        
                    else:
                        print(f'No se pudo extraer la ON Activa {titulos_unicos[i]} de forma correcta')
                        raise SystemExit()
                    
                    "con el capital e interes calculado, lo ingreso al df correspondiente"
                    
                    df_capital_e_intereses.at[i,col] = capital + interes
                    df_capital.at[i,col] = capital
                    df_intereses.at[i,col] = interes
                    break   # el break se aplica para que deje de iterar por cada columna
                        
        else:
            dias = float(dias)   #transformo el dias en valor numerico (float)
             
            """realizo una iteracion por cada fila/ flujo que contenga dicha especie
            para ir calculandole las caidas de capital e intereses y adjuntarlos al df correspondiente:"""
            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per = (1 + df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100))-1   # obtengo la tasa de interes efectiva del plazo
                spread=float(0 if df_titulo.at[fila,'spread_nu'] is None else df_titulo.at[fila,'spread_nu'])
                bucket = df_titulo.at[fila,'bucket']   # obtengo la fecha de vencimiento
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100   # obtengo la tasa de amortizacion de dicho flujo
                
                "realizo la iteracion por las columnas del df:"
                
                for col in df_capital_e_intereses.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        if tipo_tasa=='Fija':
                            
                            remanente=nominal*(1-amortizacion_acum)
                            capital = nominal*tasa_amort
                            interes = remanente*tasa_interes_efect_per
                            amortizacion_acum+=tasa_amort
                            
                        elif tipo_tasa=='Badlar':
                            
                            remanente=nominal*(1-amortizacion_acum)
                            capital = nominal*tasa_amort
                            interes = remanente*((badlar+spread)*dias/(360*100))     
                            amortizacion_acum+=tasa_amort
                            break
                        
                        else:
                            print(f'No se pudo extraer la ON Activa {titulos_unicos[i]} de forma correcta')
                            raise SystemExit()
                        
                        df_capital_e_intereses.at[i,col] = capital + interes
                        df_capital.at[i,col] = capital 
                        df_intereses.at[i,col] =  interes
                        break
    
    """genero un df el cual los titulos van a estar agrupados por moneda:"""
    
    df_capital_e_intereses_condensado = df_capital_e_intereses.copy()
    df_capital_e_intereses_condensado = df_capital_e_intereses_condensado.groupby('Moneda', as_index=False).sum()   # agrupo por moneda, sumando los flujos
    df_capital_e_intereses_condensado['Cuenta']  = 'Ons Activo'
    df_capital_e_intereses_condensado['Lugar del balance'] = 'Activo'
    
    """la funcion me devuelve los 4 dfs con los titulos incorporados"""
    return (df1.append(df_capital_e_intereses_condensado,ignore_index=True)
            ,df2.append(df_capital_e_intereses,ignore_index=True)
            ,df3.append(df_capital,ignore_index=True)
            ,df4.append(df_intereses,ignore_index=True))
                      
try:
    print('Corriendo la funcion "Ons Activa"')
    Caidas,Titulos,Caidas_capital,Caidas_intereses=Ons_activa(Caidas,Titulos,Caidas_capital,Caidas_intereses)  
    print('Finalizo la funcion Ons Activa"')
except:
    print('Hay un error en la funcion de Ons Activa. Revisar')
    raise SystemExit() 
                                                                                  
def Titulos_no_trading(df1,df2,df3,df4):
    
    "carga archivo:"
    query = """sel fecha_informacion,fecha_emision_fc, d.tipo_producto_cd,a.especie,tipo_bono_tx, Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
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
			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,5,6,8,9,10,11,12,13,14,15,16,17 order by 1 desc,3,9 asc
				having nominales > 0
    				 """
    titulos_no_trading = pd.read_sql(query,conn)
        
    fecha_filtro = nearest(titulos_no_trading['Fecha_Informacion'],fecha_calculo)
    titulos_no_trading= titulos_no_trading.loc[titulos_no_trading['Fecha_Informacion']==fecha_filtro]
    titulos_no_trading['bucket'] = (titulos_no_trading['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    
    titulos_no_trading.reset_index(drop=True,inplace=True)
    
    titulos_unicos=titulos_no_trading['Especie'].unique().tolist()
    
    df_capital_e_intereses = pd.DataFrame(columns=Caidas.columns)
    df_capital = pd.DataFrame(columns=Caidas.columns)
    df_intereses = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=titulos_no_trading.loc[titulos_no_trading['Especie']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        df_titulo.reset_index(drop=True,inplace=True)
        
        df_capital_e_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ df_titulo.at[0,'tipo_bono_tx']
        df_capital_e_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital_e_intereses.at[i,'Lugar del balance']='Activo'
        
        df_capital.at[i,'Cuenta']=titulos_unicos[i]+' '+ df_titulo.at[0,'tipo_bono_tx']
        df_capital.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital.at[i,'Lugar del balance']='Activo'
        
        df_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ df_titulo.at[0,'tipo_bono_tx']
        df_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_intereses.at[i,'Lugar del balance']='Activo'
               
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        tipo_titulo = df_titulo['tipo_producto_cd'].all()
        amortizacion_acum=0
        
        if tipo_titulo=='BondMMDiscount':
            
            bucket = df_titulo.at[0,'bucket']
            
            for col in df_capital_e_intereses.columns:
            
                if isinstance(col, int) and col >= bucket:
                    
                    df_capital_e_intereses.at[i,col] = nominal
                    df_capital.at[i,col] = nominal
                    df_intereses.at[i,col] = nominal
                    break
                 
        elif tipo_titulo=='Bond':
            dias= float(dias)
                            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per =  df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                
                bucket = df_titulo.at[fila,'bucket']
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                
                
                for col in df_capital_e_intereses.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        capital = nominal*tasa_amort
                        interes = remanente*tasa_interes_efect_per
                        
                        df_capital_e_intereses.at[i,col] = capital + interes
                        df_capital.at[i,col] = capital
                        df_intereses.at[i,col] = interes
                        
                        amortizacion_acum+=tasa_amort
                        break
                        
                    elif isinstance(col, int) and col < bucket:
                        
                        remanente=nominal*(1-amortizacion_acum)
                        capital = nominal*tasa_amort
                        interes = remanente*tasa_interes_efect_per
                        
                        df_capital_e_intereses.iat[0,-1] += capital + interes
                        df_capital.iat[0,-1] += capital
                        df_intereses.iat[0,-1] += interes
                        
                        amortizacion_acum+=tasa_amort
                        break
                    
                    elif isinstance(col, str):
                        continue
                        
                    else:
                        print(f'No se pudo extraer el Titulo {titulos_unicos[i]} de forma correcta de Titulos sin garantia')
                        raise SystemExit()
                        
    df_capital_e_intereses_condensado=df_capital_e_intereses.copy()    
    df_capital_e_intereses_condensado = df_capital_e_intereses_condensado.groupby('Moneda', as_index=False).sum()
    df_capital_e_intereses_condensado['Cuenta']  = 'Titulos no trading'
    df_capital_e_intereses_condensado['Lugar del balance'] = 'Activo'  
                                      
    return (df1.append(df_capital_e_intereses_condensado,ignore_index=True)
            ,df2.append(df_capital_e_intereses,ignore_index=True)
            ,df3.append(df_capital,ignore_index=True)
            ,df4.append(df_intereses,ignore_index=True))
                      
try:
    print('Corriendo la funcion "Titulos no trading"')
    Caidas,Titulos,Caidas_capital,Caidas_intereses=Titulos_no_trading(Caidas,Titulos,Caidas_capital,Caidas_intereses)  
    print('Finalizo la funcion Titulos no trading"')
except:
    print('Hay un error en la funcion de Titulos no trading. Revisar')
    raise SystemExit() 
                  
def Titulos_en_garantia(df1,df2,df3,df4):
    
    "carga archivo:"
    query = """sel fecha_informacion,fecha_emision_fc, d.tipo_producto_cd, a.especie, Case when  (bg_descripcion_reducida_tx like '%CER%' or indice_teorico_tx like '%CER%') then 'CER' 
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
			and ultimo_dia_habil_mes = 'Y' group by 1,2,3,4,5,7,8,9,10,11,12,13,14,15,16 order by 1 desc,2,5 asc
				having nominales > 0
    				 """
    titulos_en_garantia = pd.read_sql(query,conn)
       
    fecha_filtro = nearest(titulos_en_garantia['Fecha_Informacion'],fecha_calculo)
    titulos_en_garantia= titulos_en_garantia.loc[titulos_en_garantia['Fecha_Informacion']==fecha_filtro]
    titulos_en_garantia['fecha_devengamiento']=titulos_en_garantia['fecha_vencimiento_fc']-titulos_en_garantia['fecha_emision_fc']
    titulos_en_garantia.reset_index(drop=True,inplace=True)
    
    for i in range(len(titulos_en_garantia['fecha_devengamiento'])):
        titulos_en_garantia.at[i,'fecha_devengamiento']=titulos_en_garantia.at[i,'fecha_devengamiento'].days
    titulos_en_garantia['bucket'] = (titulos_en_garantia['fecha_cashFlow_fc']-fecha_calculo)/np.timedelta64(1,'D')
    
    titulos_en_garantia.reset_index(drop=True,inplace=True)
    
    titulos_unicos=titulos_en_garantia['Especie'].unique().tolist()
    
    df_capital_e_intereses = pd.DataFrame(columns=Caidas.columns)
    df_capital = pd.DataFrame(columns=Caidas.columns)
    df_intereses = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=titulos_en_garantia.loc[titulos_en_garantia['Especie']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        df_titulo.reset_index(drop=True,inplace=True)
        
        df_capital_e_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(Titulo sin Garantia('
        df_capital_e_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital_e_intereses.at[i,'Lugar del balance']='Activo'
        
        df_capital.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(Titulo sin Garantia('
        df_capital.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_capital.at[i,'Lugar del balance']='Activo'
        
        df_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(Titulo sin Garantia('
        df_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
        df_intereses.at[i,'Lugar del balance']='Activo'
        
        dias = df_titulo['dias'].all()
        nominal=df_titulo.at[0,'nominales']/1000
        tipo_titulo = df_titulo['tipo_producto_cd'].all()
        amortizacion_acum=0
        
        if tipo_titulo=='BondMMDiscount': 
            bucket = df_titulo.at[0,'bucket']
            
            for col in df_capital_e_intereses.columns:
                if isinstance(col, int) and col >= bucket:
                                        
                    capital = nominal
                    
                    df_capital_e_intereses.at[i,col] = capital
                    df_capital.at[i,col] = capital
                    df_intereses.at[i,col] = 0
                    break
                 
        elif tipo_titulo=='Bond':
            dias= float(dias)
                            
            for fila in range(len(df_titulo)):
                                
                tasa_interes_efect_per = df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                bucket = df_titulo.at[fila,'bucket']
                tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                remanente=nominal*(1-amortizacion_acum)
                amortizacion_acum+=tasa_amort
                
                if bucket>df_capital_e_intereses.columns[-1]:
                    
                    capital = nominal*tasa_amort
                    interes = remanente*tasa_interes_efect_per
                    
                    df_capital_e_intereses.at[i,df_capital_e_intereses.columns[-1]] += capital + interes
                    df_capital.at[i,df_capital_e_intereses.columns[-1]] += capital 
                    df_intereses.at[i,df_capital_e_intereses.columns[-1]] += interes
                    
                    amortizacion_acum+=tasa_amort

                for col in df_capital_e_intereses.columns:
                    if isinstance(col, int) and col >= bucket:
                        capital = nominal*tasa_amort
                        interes = remanente*tasa_interes_efect_per
                    
                        df_capital_e_intereses.at[i,df_capital_e_intereses.columns[-1]] += capital + interes
                        df_capital.at[i,df_capital_e_intereses.columns[-1]] += capital 
                        df_intereses.at[i,df_capital_e_intereses.columns[-1]] += interes
                            
                        amortizacion_acum+=tasa_amort
                        break
        else:
           print(f'No se pudo extraer el Titulo {titulos_unicos[i]} de forma correcta de Titulos sin garantia')
           raise SystemExit()
             
    df_capital_e_intereses_condensado = df_capital_e_intereses.copy()    
    df_capital_e_intereses_condensado = df_capital_e_intereses_condensado.groupby('Moneda', as_index=False).sum()
    df_capital_e_intereses_condensado['Cuenta']  = 'Titulos en garantia'
    df_capital_e_intereses_condensado['Lugar del balance'] = 'Activo'

    return (df1.append(df_capital_e_intereses_condensado,ignore_index=True)
                ,df2.append(df_capital_e_intereses,ignore_index=True)
                ,df3.append(df_capital,ignore_index=True)
                ,df4.append(df_intereses,ignore_index=True))
                      
try:
    print('Corriendo la funcion "Titulos en garantia"')
    Caidas,Titulos,Caidas_capital,Caidas_intereses=Titulos_en_garantia(Caidas,Titulos,Caidas_capital,Caidas_intereses)  
    print('Finalizo la funcion Titulos no trading"')
except:
    print('Hay un error en la funcion de Titulos en garantia. Revisar')
    raise SystemExit()               
                                                             
                                                                                                        
def Ons_pasivo(df1,df2,df3,df4):
    
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
        ons_pasivo.at[i,'fecha_devengamiento']=ons_pasivo.at[i,'fecha_devengamiento'].days
    
    titulos_unicos=ons_pasivo['TICKER_cd'].unique().tolist()
    
    df_capital_e_intereses = pd.DataFrame(columns=Caidas.columns)
    df_capital = pd.DataFrame(columns=Caidas.columns)
    df_intereses = pd.DataFrame(columns=Caidas.columns)
    
    for i in range(len(titulos_unicos)):
        
        df_titulo=ons_pasivo.loc[ons_pasivo['TICKER_cd']==titulos_unicos[i],:]
        df_titulo=df_titulo.sort_values(by='bucket')
        
        if df_titulo['fecha_vencimiento_fc'].max() < fecha_calculo or df_titulo['fecha_emision_fc'].max() > fecha_calculo:
            continue
        else:
            df_titulo.reset_index(drop=True,inplace=True)
            
            df_capital_e_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(ON pasiva)'
            df_capital_e_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
            df_capital_e_intereses.at[i,'Lugar del balance']='Pasivo'
            
            df_capital.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(ON pasiva)'
            df_capital.at[i,'Moneda']=df_titulo['Moneda'].all()
            df_capital.at[i,'Lugar del balance']='Pasivo'
            
            df_intereses.at[i,'Cuenta']=titulos_unicos[i]+' '+ '(ON pasiva)'
            df_intereses.at[i,'Moneda']=df_titulo['Moneda'].all()
            df_intereses.at[i,'Lugar del balance']='Pasivo'
            
            
            tipo_tasa = df_titulo['tasa_tipo_tx'].all()
            dias = df_titulo['dias'].all()
            nominal=df_titulo.at[0,'nominales']/1000
            amortizacion_acum=0
            devengamiento=df_titulo.at[0,'fecha_devengamiento']
                                    
            if dias=='Cupon cero a descuento':
                
                bucket = df_titulo.at[0,'bucket']
                tasa_amort= df_titulo.at[0,'tasa_amortizacion_rate']/100
                spread= float(df_titulo.at[0,'spread_nu'])
            
                for col in df_capital_e_intereses.columns:
                    
                    if isinstance(col, int) and col >= bucket:
                        
                        if tipo_tasa=='Badlar':
                            capital = nominal
                            interes = nominal*((badlar+spread)*devengamiento/(100*360)) 
                      
                        elif tipo_tasa=='Fija':
                            capital = nominal
                            interes = 0
                     
                        else:
                            print(f'No se pudo extraer el Titulo {titulos_unicos[i]} (cupon cero) de forma correcta de Ons Pasivo ya que la tasa no se corresponde ni con fija, ni con badlar')
                            raise SystemExit()
                        
                        df_capital_e_intereses.at[i,col] = capital + interes
                        df_capital.at[i,col] = capital
                        df_intereses.at[i,col] = interes
                        break
                        
                        
            elif dias!='Cupon cero a descuento':
                dias= float(dias)
                            
                for fila in range(len(df_titulo)):
                                    
                    tasa_interes_efect_per = df_titulo.at[fila,'tasa_interes_rate']*dias/(360*100)
                    spread=float(df_titulo.at[fila,'spread_nu'])
                    bucket = df_titulo.at[fila,'bucket']
                    tasa_amort= df_titulo.at[fila,'tasa_amortizacion_rate']/100
                    remanente=nominal*(1-amortizacion_acum)
                    amortizacion_acum+=tasa_amort
                    
                    if bucket<0:
                        amortizacion_acum+=tasa_amort
                        continue
                                       
                    for col in df_capital_e_intereses.columns:
                        
                        if isinstance(col, int) and col >= bucket:
                            if tipo_tasa=='Fija':
                                
                                capital = nominal*tasa_amort
                                interes = remanente*tasa_interes_efect_per
                                
                            elif tipo_tasa=='Badlar':
                                
                                capital = nominal*tasa_amort
                                interes = remanente*(badlar+spread)*dias/(360*100)
                                
                            else:
                                print(f'No se pudo extraer el Titulo {titulos_unicos[i]} (no cupon cero) de forma correcta de Ons Pasivo ya que la tasa no se corresponde ni con fija, ni con badlar')
                                raise SystemExit()
                                
                            df_capital_e_intereses.at[i,col] = capital + interes
                            df_capital.at[i,col] = capital
                            df_intereses.at[i,col] = interes
                            break
    df_capital_e_intereses_condensado = df_capital_e_intereses.copy()    
    df_capital_e_intereses_condensado = df_capital_e_intereses_condensado.groupby('Moneda', as_index=False).sum()
    df_capital_e_intereses_condensado['Cuenta']  = 'Ons Pasivo'
    df_capital_e_intereses_condensado['Lugar del balance'] = 'Pasivo'
    
    return (df1.append(df_capital_e_intereses_condensado,ignore_index=True)
                ,df2.append(df_capital_e_intereses,ignore_index=True)
                ,df3.append(df_capital,ignore_index=True)
                ,df4.append(df_intereses,ignore_index=True))
                      
try:
    print('Corriendo la funcion "Ons Pasivo"')
    Caidas,Titulos,Caidas_capital,Caidas_intereses = Ons_pasivo(Caidas,Titulos,Caidas_capital,Caidas_intereses)  
    print('Finalizo la funcion "Ons pasivo"')
except:
    print('Hay un error en la funcion de Ons Pasivo. Revisar')
    raise SystemExit()
                        

def Leliqs_garantia_pase(df1,df2,df3,df4):
    
    query="""select fecha_sla,siaf_alias_especie,fecha_vencimiento_fc, sum(nominal) as VN
    from p_dw_explo.calendario_servicios a
    left join p_dw_explo.SIAF_soporte_contable_pases  c on a.fecha_sla >= c.fec_operacion and a.fecha_sla < c.fec_vto
    left join p_dw_explo.especie_bono d on c.siaf_alias_especie = d.ticker_cd
    where habil = 1  and c.tipo_operacion = 'TIPA' and ultimo_dia_habil_mes = 'Y' and tipo_bono_tx = 'Letra de Liquidez BCRA'
    group by 1 ,2,3
    order by 1 desc"""
    
    leliqs = pd.read_sql(query,conn)
    fecha_filtro = nearest(leliqs['fecha_SLA'],fecha_calculo)
    leliqs= leliqs.loc[leliqs['fecha_SLA']==fecha_filtro]
    
    
    leliqs['SIAF_Alias_Especie'] = leliqs['SIAF_Alias_Especie'].str.strip()
    leliqs = leliqs.groupby('SIAF_Alias_Especie', as_index=False)['VN'].sum()
    leliqs['VN']=leliqs['VN']/1000
    
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
    
    df_capital_e_intereses = pd.DataFrame(columns=df1.columns)
    df_capital = pd.DataFrame(columns=df1.columns)
    df_intereses = pd.DataFrame(columns=df1.columns)
    
    for i in range(len(leliqs)):
        
        df_capital_e_intereses.at[i,'Cuenta']=leliqs.at[i,'SIAF_Alias_Especie'] + '(Leliq_garantia_pase)'
        df_capital_e_intereses.at[i,'Moneda']='ARS'
        df_capital_e_intereses.at[i,'Lugar del balance']='Activo'
        
        df_capital.at[i,'Cuenta']=leliqs.at[i,'SIAF_Alias_Especie'] + '(Leliq_garantia_pase)'
        df_capital.at[i,'Moneda']='ARS'
        df_capital.at[i,'Lugar del balance']='Activo'
        
        df_intereses.at[i,'Cuenta']=leliqs.at[i,'SIAF_Alias_Especie'] + '(Leliq_garantia_pase)'
        df_intereses.at[i,'Moneda']='ARS'
        df_intereses.at[i,'Lugar del balance']='Activo'
        
        for col in df_capital_e_intereses.columns:
            if isinstance(col, int) and col >= leliqs.at[i,'bucket']:
                
                df_capital_e_intereses.at[i,col] = leliqs.at[i,'VN']
                df_capital.at[i,col] = leliqs.at[i,'VN']
                df_capital.at[i,col] = 0
                break 
            
    df_capital_e_intereses_condensado = df_capital_e_intereses.copy()    
    df_capital_e_intereses_condensado = df_capital_e_intereses_condensado.groupby('Moneda', as_index=False).sum()
    df_capital_e_intereses_condensado['Cuenta']  = 'Leliqs garantia pase'
    df_capital_e_intereses_condensado['Lugar del balance'] = 'Activo'
    
    return (df1.append(df_capital_e_intereses_condensado,ignore_index=True)
                ,df2.append(df_capital_e_intereses,ignore_index=True)
                ,df3.append(df_capital,ignore_index=True)
                ,df4.append(df_intereses,ignore_index=True))
                      
try:
    print('Corriendo la funcion "Leliqs garantia pase"')
    Caidas,Titulos,Caidas_capital,Caidas_intereses = Leliqs_garantia_pase(Caidas,Titulos,Caidas_capital,Caidas_intereses)  
    print('Finalizo la funcion Leliqs garantia pase"')
except:
    print('Hay un error en la funcion de Leliqs garantia pase. Revisar')
    raise SystemExit()
                    

# %% Calculo Correlaciones de tasas

# Obtengo la fecha de corrida la cual sirve para filtrar la informacion


t0 = time.time()


fecha_minima = fecha_calculo-datetime.timedelta(days=365*historia_variaciones)

def Importa_curvas():
    
    curvas_excel = pd.read_excel('Curva Tasas Historicas.xlsx')
    curvas_excel['Fecha'] = pd.to_datetime(curvas_excel['Fecha']).dt.date
    
    
    curvas_excel = curvas_excel[curvas_excel['Fecha']>=fecha_minima]
    
    fecha_max_historico=curvas_excel['Fecha'].max()
    
    
    
    if fecha_max_historico > fecha_calculo:
        
        Curvas_condensado = curvas_excel[curvas_excel['Fecha']<=fecha_calculo]
      
    else:
        
        query = """select * from p_dw_explo.view_tasa_transfer"""
        curvas_teradata = pd.read_sql(query,conn)
        
        curvas_teradata = curvas_teradata[(curvas_teradata['fecha']<=fecha_calculo) 
                                    & (curvas_teradata['fecha']>fecha_max_historico)]
        
        curvas_teradata = pd.melt(curvas_teradata, id_vars =['fecha','descripcion','plazo']
                                , value_vars =['fija_pesos','fija_dolar','UVI'])
        
        curvas_teradata = curvas_teradata.pivot_table('value',['fecha','variable','descripcion'],'plazo')

        curvas_teradata.reset_index(inplace=True)

        curvas_teradata.rename(columns = {'fecha':'Fecha','variable':'Moneda', 'descripcion':'Lugar_balance'}, inplace = True)
        
        for row in range(len(curvas_teradata)):
            lugar_balance = curvas_teradata.at[row,'Lugar_balance']
            moneda = curvas_teradata.at[row,'Moneda']
            
            if moneda=='fija_pesos':
                if lugar_balance=='Activa_mensual':
                    curvas_teradata.at[row,'Lugar_balance']='Activo'
                    curvas_teradata.at[row,'Moneda']='ARS'
                elif lugar_balance=='Pasiva':
                    curvas_teradata.at[row,'Lugar_balance']='Pasivo'
                    curvas_teradata.at[row,'Moneda']='ARS'
                else:
                    curvas_teradata.drop(row,inplace=True)
                    
            elif moneda=='UVI':
                if lugar_balance=='Activa':
                    curvas_teradata.at[row,'Lugar_balance']='Activo'
                    curvas_teradata.at[row,'Moneda']='CER'
                elif lugar_balance=='Pasiva':
                    curvas_teradata.at[row,'Lugar_balance']='Pasivo'
                    curvas_teradata.at[row,'Moneda']= 'CER'
                else:
                    curvas_teradata.drop(row,inplace=True)
                    
            elif moneda=='fija_dolar':
                if lugar_balance=='Activa':
                    curvas_teradata.at[row,'Lugar_balance']='Activo'
                    curvas_teradata.at[row,'Moneda']='USD'
                elif lugar_balance=='Pasiva':
                    curvas_teradata.at[row,'Lugar_balance']='Pasivo'
                    curvas_teradata.at[row,'Moneda']= 'USD'
                else:
                    curvas_teradata.drop(row,inplace=True)
            else:
                    curvas_teradata.drop(row,inplace=True)
            
        columnas=curvas_teradata.columns.to_list()       
        
        for i in range(len(columnas)):
            try:
                columnas[i]=int(columnas[i])
                
            except:
                pass        
                    
        cols = curvas_teradata.select_dtypes(np.number).columns
        curvas_teradata[cols] = curvas_teradata[cols]*100
        
        calendario = pd.DataFrame({"Fecha": pd.date_range(fecha_max_historico + timedelta(days = 1), fecha_calculo)})
        calendario['Fecha'] = pd.to_datetime(calendario['Fecha']).dt.date
        
        tera_cond=df = pd.DataFrame(columns=curvas_teradata.columns)
        
        for moneda in curvas_teradata['Moneda'].unique():
            for lugar_balance in curvas_teradata['Lugar_balance'].unique():
        
                df=curvas_teradata.loc[(curvas_teradata['Moneda']==moneda) & (curvas_teradata['Lugar_balance']==lugar_balance),:]
                
                cond=calendario.merge(df,how='left')
                cond.fillna(method='ffill',inplace=True)
        
                tera_cond = tera_cond.append(cond)
                 
        Curvas_condensado=pd.concat([curvas_excel,tera_cond],axis=0,join='inner')           
        
    
    Curvas_condensado.sort_values(by=['Fecha','Lugar_balance','Moneda'],inplace=True)  ##ordeno las curvas por nombre de curva y despues por fecha
    Curvas_condensado.drop_duplicates(subset=['Fecha','Lugar_balance','Moneda']
                        ,keep='first',inplace=True,ignore_index=True) ###elimino en caso de duplicados
        
    return Curvas_condensado    
        
tasasInput=Importa_curvas()
 

def ImportoCurvas(df=tasasInput.copy(),fecha_max = fecha_calculo):
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
    
    df["Curva"] = df[['Lugar_balance', 'Moneda']].agg(' '.join, axis=1) ### creo una nueva columna como combinacion de dos
    df.drop(['Lugar_balance', 'Moneda'], axis=1,inplace=True) ### dropeo las cols que no voy a usar
    df.sort_values(by=['Curva','Fecha'],inplace=True)  ##ordeno las curvas por nombre de curva y despues por fecha
    df.drop_duplicates(subset=['Fecha','Curva'],keep='first',inplace=True,ignore_index=True) ###elimino en caso de duplicados
    

    "Creo un nuevo archivo con los parametros de las curvas:"

    df_data = pd.DataFrame(df['Curva'].drop_duplicates()) ###me quedo con las curvas unicas
    df_data['Indices']=df_data.index ###Creo una columna con los valores de los indices
    df_data.reset_index(drop=True,inplace=True)
    
    
    
    
    return df, df_data


# =============================================================================
# Funcion diferencia
# =============================================================================

def Diferencia(df,cols=['Fecha','Curva'],dias=HoldingPeriod):
    
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

Curvas,info_curvas = ImportoCurvas()

Dic_curvas = Separo_DataFrames(Curvas, info_curvas) 

CERT_correlaciones = Calculo_correlaciones(Dic_curvas)

t1 = time.time()


# %% Funciones para las Simulacion de Tasas de Interes
start = time.time()

tasasInput.columns = ["Fecha", 30, 60, 90, 120, 150, 180, 270, 360, 450, 540, 720, 900,
                      1080, 1260, 1440, 1620, 1800, 2160, 2520, 2880, 3240, 3600, 'Lugar del balance', "Moneda"]


for nodo in tasasInput:
    if nodo == "Fecha" or nodo == 'Lugar del balance' or nodo == "Moneda":
        continue
    else:
        tasasInput[nodo] = tasasInput[nodo] / 100


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
        if len(dfDiferencia.columns) < 5:
            dfCovarianza, cholesky = "Error","Error"
        else:
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


def simulacionCurva(dfDiferencia, ultimaCurva, cholesky,ajusteDesvio, M, arrayShockIndependiente="PRIMERO"):
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
            shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky, ajusteDesvio))     ### uso la funcion shockCorrelacionado para correlacionar los shocks
            curvaSimulada = np.add(ultimaCurva, shockCorr)                      ### sumo la curva de shocks correlacionados a la ultima curva
            arraySimulaciones[i] = curvaSimulada                                ### guardo la curva simulada en el array de simulaciones

    else:                                                                       ### aca se realiza el mismo proceso de antes, tomando los shocks aleatorios de una tasa ya simulada y correlacionada
        arraySimulaciones = np.zeros(shape=(M, len(ultimaCurva)))
        arrayRandom = arrayShockIndependiente                                   ### Genero array de valores aleatorios
        
        dfRandom = pd.DataFrame(arrayRandom)                                    ### paso el array a un df de pandas para operar por nodos
        
        dfRandom = dfRandom.drop(columns=[col for col in dfRandom if col not in nodosindex])
        dfRandom.columns = nodosAsist
        
        for nodo in dfRandom:                                                   ### ciclo todos los nodos
            dfRandom[nodo] = ((dfDiferencia[nodo].quantile(
                dfRandom[nodo]) - avg[nodo]) / std[nodo]).values                ### tomo el percentil y lo estandarizo

        arrayShockIndependiente = dfRandom.values   
        for i in tqdm(range(M)):
            shockIndep = arrayShockIndependiente[i]
            shockCorr = np.array(shockCorrelacionado(shockIndep, cholesky, ajusteDesvio))
            curvaSimulada = np.add(ultimaCurva, shockCorr)
            arraySimulaciones[i] = curvaSimulada
            arrayShockIndependiente = arrayShockIndependiente
    arraySimulaciones = np.clip(arraySimulaciones,a_min=0,a_max=1)
    return arraySimulaciones, arrayRandom,avg,std


def shockCorrelacionado(shockIndependiente, cholesky, ajuste_desvio):
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
        shock = np.dot(shockIndependiente, nodo) * ajuste_desvio
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


def ValorActualiza(Caidas, Tasas, M):
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
    
    for i in tqdm(range(M+1)):
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


def loopActualiza(Caidas, curva_tasas, nodosTasas, correlaciones,años = 100,holding_period = 90, M = 1000, Ajuste_Desvio = 1):
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
    avg = {}
    std = {}
    curva_tasas = tasasInput.copy()
    for LugarBalance in LB:
        sims_assist = {}
        ValorActual_assist = {}
        promedio_assist = {}
        desvio_assist = {}
        
        for Moneda in TS:
            print(f"""==============================
Comienza proceso para la tasa {LugarBalance} {Moneda}""")
            
            dfDiferencia = DiferenciaNodos(curva_tasas.loc[(curva_tasas['Lugar del balance'] == LugarBalance) &
                                                          (curva_tasas["Moneda"] == Moneda),
                                                          curva_tasas.columns.values.tolist()[1:len(curva_tasas.columns)-2]],
            holding_period)
            
            if (LugarBalance== "Activo") & (Moneda == "CER"):
                dfDiferencia.drop([60,90,120],axis=1,inplace=True)
            if (LugarBalance== "Pasivo") & (Moneda == "CER"):
                dfDiferencia.drop([60,90,120],axis=1,inplace=True)
            if (LugarBalance== "Pasivo") & (Moneda == "USD") & (años >= 5):
                dfDiferencia.drop([900,1260,1620],axis=1,inplace=True)
            if (LugarBalance== "Pasivo") & (Moneda == "ARS") & (años >= 5):
                dfDiferencia.drop([450,540,1620],axis=1,inplace=True)
            
            dfCovarianza, cholesky = parametrosSimulaciones(dfDiferencia)
            
            if type(dfCovarianza) == str:
            
                dfDiferencia = DiferenciaNodos(curva_tasas.loc[(curva_tasas['Lugar del balance'] == 'Activo') &
                                                              (curva_tasas["Moneda"] == 'ARS'),
                                                              curva_tasas.columns.values.tolist()[1:len(curva_tasas.columns)-2]],
                holding_period)
                
                if (LugarBalance== "Activo") & (Moneda == "CER"):
                    dfDiferencia.drop([60,90,120],axis=1,inplace=True)
                if (LugarBalance== "Pasivo") & (Moneda == "CER"):
                    dfDiferencia.drop([60,90,120],axis=1,inplace=True)
                if (LugarBalance== "Pasivo") & (Moneda == "USD") & (años >= 5):
                    dfDiferencia.drop([900,1260,1620],axis=1,inplace=True)
                if (LugarBalance== "Pasivo") & (Moneda == "ARS") & (años >= 5):
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
    
            ultimaCurva = curva_tasas.loc[(curva_tasas['Lugar del balance'] == LugarBalance) &
                                         (curva_tasas["Moneda"] == Moneda),
                                         dfDiferencia.columns.values.tolist()].iloc[-1]
            
            print("Simulo Tasa")
            
            if not(Grupo in dicSimulacionesCorr):
                arrayTasasSimuladas, arrayShockInependiente,promedio,desvio = simulacionCurva(dfDiferencia,
                                                                                             ultimaCurva,
                                                                                                cholesky,
                                                                                                Ajuste_Desvio,
                                                                                                M)
                dicSimulacionesCorr[Grupo] = arrayShockInependiente
            
            else:
                arrayTasasSimuladas, arrayShockInependiente,promedio,desvio = simulacionCurva(dfDiferencia,
                                                                                             ultimaCurva,
                                                                                             cholesky,
                                                                                             Ajuste_Desvio,
                                                                                             M,
                                                                                             dicSimulacionesCorr[Grupo])
            
            promedio_assist[Moneda] = promedio
            desvio_assist[Moneda] = desvio
            
            ultimaCurva = pd.DataFrame(curva_tasas.loc[(curva_tasas['Lugar del balance'] == LugarBalance) &
                                                      (curva_tasas["Moneda"] == Moneda),
                                                      dfDiferencia.columns].iloc[-1]).transpose()
                
            dfSimulaciones = pd.DataFrame(arrayTasasSimuladas,
                                              columns=dfDiferencia.columns)
            
            dfSimulaciones = pd.concat([dfSimulaciones,ultimaCurva])
            dfSimulaciones.reset_index(drop=True,inplace=True)
            
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
            time.sleep(0.1)
            ValorActual_assist[Moneda] = ValorActualiza(
                Caidas.fillna(0)[(Caidas["Moneda"] == Moneda) & 
                                 (Caidas['Lugar del balance'] == LugarBalance)].values[:,5:], 
                Tasas,
                M)
            
        ValorActual[LugarBalance] = ValorActual_assist
        sims[LugarBalance] = sims_assist
        avg[LugarBalance] = promedio_assist
        std[LugarBalance] = desvio_assist
        
    return ValorActual, dicSimulacionesCorr, sims, curva_tasas,avg,std


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
    
    CapitalEconomico = {"CE 99.9" : "{:0,.2f}".format(float(CapitalEconomico999*1000)),
                        "CE 99.5" : "{:0,.2f}".format(float(CapitalEconomico995*1000)),
                        "CE 99.0" : "{:0,.2f}".format(float(CapitalEconomico99*1000))}
    
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
            Curvas[i + j + "Base"] = SimulacionesTasas[i][j].iloc[M]
    
    return Curvas

end = time.time()
print(f'el codigo tarda {end - start:.2f} segundos en definir las funciones')
start = time.time()

# %% Simulo M veces la siguiente curva y generlo un array con todos los resultados


LB = pd.unique(Caidas['Lugar del balance'])
TS = pd.unique(Caidas["Moneda"])
d = {'Lugar del balance':["Activo","Activo","Activo","Pasivo","Pasivo","Pasivo"],
     "Moneda":["ARS","USD","CER","ARS","USD","CER"],
     "Grupo":["A","B","C","A","D","C"]}
correlaciones = pd.DataFrame(d)

ValoresActuales,ShocksAleatorios,SimulacionesTasas,Tasas_calculo,Promedios,Desvios= loopActualiza(Caidas,
                                                                                                  tasasInput, 
                                                                                                  nodosTasas, 
                                                                                                  correlaciones, 
                                                                                                  historia_variaciones,
                                                                                                  HoldingPeriod,
                                                                                                  simulaciones,
                                                                                                  AjusteDesvio)


print()
print("==============================")

def dolar ():        
    query="""sel exch_fecha, exch_hist_sell_rate from p_dw_tables.EXCHANGE_OM_DAILY where exchange_market_code = 20 and currency_code = 2"""
    serie_dolar=pd.read_sql(query,conn)
    
    fecha_filtro = nearest(serie_dolar['Exch_Fecha'],fecha_calculo)
    dolar=serie_dolar.loc[serie_dolar['Exch_Fecha']==fecha_filtro,'Exch_Hist_Sell_Rate'].iloc[0]
    return dolar

cotizaUSD = dolar()

neto = neteoAP(ValoresActuales, TS, cotizaUSD)

CapitalEcon = Capitales(neto,TS)

Curvas = curvasRelevantes(neto, SimulacionesTasas, LB, TS, simulaciones)

dfCurvas = pd.DataFrame(Curvas,columns=(Curvas.keys()))
dfCurvas['Nodo'] = dfCurvas.index

Curvas = dfCurvas.copy()
Curvas = pd.melt(Curvas,id_vars=['Nodo'],value_vars=Curvas.columns[:-1])
Curvas['cd_sesion'] = cd_sesion
Curvas['Lugar del balance'] = Curvas['variable'].str[:6]
Curvas['Moneda'] = Curvas['variable'].str[6:9]
Curvas['Percentil'] = Curvas['variable'].str[9:]
Curvas['Tasa'] = Curvas['value']

CERT_Curvas = Curvas[['cd_sesion','Lugar del balance', 'Moneda', 'Percentil','Nodo','Tasa']]
CERT_Curvas.columns = ['cd_sesion','Lugar de balance', 'Moneda', 'Percentil','Bucket','Tasa']

CERT_CapitalEconomico = pd.DataFrame({'cd_sesion':[cd_sesion] * len(CapitalEcon),
                                      'Percentil':CapitalEcon.keys(),
                                      'CapitalEconomico':CapitalEcon.values()})

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARS50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoARSBase', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARS50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoARSBase', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSD50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoUSDBase', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSD50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoUSDBase', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCER50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='ActivoCERBase', ax=ax,xticks = xline)

plt.show()

ax = plt.gca()
xline = np.arange(360,3601,360)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER999', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER995', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER99', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCER50', ax=ax,xticks = xline)
dfCurvas.plot(kind='line',x='Nodo',y='PasivoCERBase', ax=ax,xticks = xline)

plt.show()


# %% Time report

end = time.time()
print(
    f'el codigo tarda {(end - start)/60:.2f} minutos en correr {simulaciones} simulaciones para todas las tasas')


df=Caidas.copy()
curva_tasas=SimulacionesTasas.copy()

def Duration (df,curva_tasas):
    
    Lugar_balance=[]
    Moneda=[]
    Total=[]
    Duration=[]
    Duration_gap=[]
    valor_actual_activo=0
    valor_actual_pasivo=0
    total_activo=0
    total_pasivo=0
    
    
    for lugar_balance in LB:
        for moneda in TS:
            
            
            curva_tasa=curva_tasas[lugar_balance][moneda].iloc[-1]
            caidas_gap= df.loc[(df['Moneda']==moneda)&(df['Lugar del balance']==lugar_balance)]
            caidas= df.loc[(df['Moneda']==moneda)&(df['Lugar del balance']==lugar_balance)]
            caidas_gap.drop(caidas_gap[caidas_gap['Flag_variable'] == 1].index, inplace = True)
            
            for col in caidas_gap.columns:
                if not isinstance(col, int):
                    caidas_gap.drop(col,axis=1,inplace=True)
                    caidas.drop(col,axis=1,inplace=True)
                else:
                    serie=caidas_gap[col]*col
                    caidas_gap.loc[:,col]=serie
                    
            suma_por_nodo = caidas_gap.sum(axis=0)
            valor_actual=0
            
            for indice,valor in suma_por_nodo.iteritems():
                tasa = curva_tasa[indice]
                valor_actual += valor*pow(1+tasa,(-indice/360))
            
            total=caidas.to_numpy().sum() 
            
            Lugar_balance.append(lugar_balance)
            Moneda.append(moneda)
            Total.append(total)
            Duration.append(valor_actual/total)
            
           
            if lugar_balance=='Activo':
                valor_actual_activo += valor_actual
                total_activo += caidas.to_numpy().sum()
            else:
                valor_actual_pasivo += valor_actual
                total_pasivo += caidas.to_numpy().sum()
            
    dic = {'Lugar_balance':Lugar_balance, 'Moneda':Moneda, 'Monto':Total, 'Duration':Duration}

    df = pd.DataFrame.from_dict(dic)
                
    Dur_moneda=[]    
    for moneda in df.Moneda.unique():
        
        dur_moneda_activo = df.loc[(df['Moneda']==moneda) & (df['Lugar_balance']=='Activo'),'Duration'].values[0]
        total_moneda_activo = df.loc[(df['Moneda']==moneda) & (df['Lugar_balance']=='Activo'),'Monto'].values[0]
        dur_moneda_pasivo = df.loc[(df['Moneda']==moneda) & (df['Lugar_balance']=='Pasivo'),'Duration'].values[0]
        total_moneda_pasivo = df.loc[(df['Moneda']==moneda) & (df['Lugar_balance']=='Pasivo'),'Monto'].values[0]
        
        Dur_moneda.append(moneda)
        duration_gap_moneda=dur_moneda_activo-(total_moneda_pasivo/total_moneda_activo)*dur_moneda_pasivo
        Duration_gap.append(duration_gap_moneda)
    
    duration_gap_total=(valor_actual_activo/total_activo)-(valor_actual_pasivo/total_pasivo)*(total_pasivo/total_activo)
    Dur_moneda.append('Total')
    Duration_gap.append(duration_gap_total)
    
    dic = { 'Moneda':Dur_moneda, 'Duration_Gap':Duration_gap}
    df2 = pd.DataFrame.from_dict(dic)
    
    return df,df2

Duration,Duration_gap=Duration(Caidas,SimulacionesTasas)
