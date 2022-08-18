# Se importan las librerias que se van a utilizar

import pandas as pd
import numpy as np
from tqdm import tqdm
from scipy.stats import norm
from datetime import datetime, timedelta, date
import pyodbc
from functools import reduce


# Se importan todas las tablas input que requiere el modelo

print('Comienza el import de inputs!')
bgdatos = pd.read_excel('bgdatos.xlsx',index_col=[0]) # importo bgdatos
clientes = pd.read_excel('clientes.xlsx') # importo base de clientes
pd_mas_valorar = pd.read_excel('pd_mas_valorar.xlsx') # importo pd mas valorar
CCF_NoExced = pd.read_excel('CCF.xlsx','Sheet1',index_col=[0]) # importo ccf para los no excedidos
CCF_Exced = pd.read_excel('CCF.xlsx','Sheet2',index_col=[0]) # importo ccf para los excedidos
LGD_df = pd.read_excel('LGD.xlsx','Sheet1',index_col=[0]) # importo lgd sin garantia
LGD_con_garantia = pd.read_excel('LGD.xlsx','Sheet2',index_col=[0]) # importo lgd con garantia
calendario = pd.read_excel('calendar_look.xlsx', index_col=[0]) # importo el calendario


# Se crea la funcion "strip()" para eliminar espacios en blanco

def strip(df):
    columnas = df.columns # creo lista de columnas en el dataframe input
    for col in columnas: # loopeo a traves de todas las columnas
        if isinstance(df[col][0],str): # condicion si la columna tiene contenido de texto
            df[col] = df[col].str.strip() # si pasa la condicion anterior, elimina los espacios al final de cada celda en esa columna

# Se eliminan los espacios en blanco de cada tabla
strip(bgdatos)
strip(clientes)
strip(pd_mas_valorar)
strip(CCF_NoExced)
strip(CCF_Exced)
strip(LGD_df)
strip(LGD_con_garantia)


# Se crea una funcion para obtener la fecha de calculo

def ultimo_dia_mes(dia):
    # Calculo el mes siguiente: como el dia 28 esta en todos los meses, le sumo 4 dias para estar en el proximo mes
    next_month = dia.replace(day=28) + timedelta(days=4)
    # Al mes siguiente le resto los dias que pasan al ultimo del mes de modo de obtener el ultimo del mes corriente
    return next_month - timedelta(days=next_month.day)

# Se ingresa como input la fecha de calculo (se guarda en la variable "cd_periodo")
cd_periodo = '202112'

# Se calcula la fecha de calculo con la funcion creada anteriormente: "ultimo_dia_mes"
fecha_calculo = ultimo_dia_mes(date(int(cd_periodo[:4]), int(cd_periodo[4:]), 1)) # los parametros que se ingresan son (año, mes, 1)
fecha_calculo


# Se define funcion control de columnas

def control_columnas(bgdatos_df):
    columnas_bgdatos = ['cd_tipo_doc', 'cdi', 'cd_operacion', 'cd_sistema', 'tx_linea',
           'cd_garantia', 'fc_alta', 'fc_cierre', 'vl_monto_inicial', 'vl_saldo',
           'vl_limite', 'cd_garantia_grupo', 'vl_tna', 'vl_tem', 'vl_cft',
           'cd_tipo_producto', 'nu_mora', 'cd_segmento', 'cd_clasificacion_bcra',
           'cd_moneda', 'cd_ajuste_cer', 'fc_ini_mora', 'tx_campo_extra'] # creo lista de columnas que tiene que tener bgdatos
    if bgdatos_df.columns.to_list() != columnas_bgdatos: # esto es un control
        print('Las columnas del input bgdatos no son correctas!') 
        
control_columnas(bgdatos) # Se ejecuta la funcion control_columnas


# Se define la funcion ccf_producto para calcular el ccf de cada producto que corresponde

def ccf_producto(df):
    output = pd.DataFrame(columns=['producto_cd','ccf_vl']) # Genero df output
    for producto in df.iloc[:,0].unique(): # loopeo por todos los tipos de productos
        df1 = df[df['producto_cd']==producto].copy() # tomo un subset con solo el producto que estoy iterando
        saldo_total = sum(df1.saldo_restante) # calculo el saldo total
        df1['saldo_ponderado'] = df1['saldo_restante']/saldo_total # agrego columna con saldo ponderado
        df1['ccf_ponderado'] = df1['CCF'] * df1['saldo_ponderado'] # agrego columna con ccf ponderado
        ccf = [producto,sum(df1.ccf_ponderado)] # genero lista para poblar el df output (el ccf del producto es la suma de todos los ccf ponderados)
        output.loc[len(output)] = ccf # inserto la lista al final del dataframe
    return output # la salida de la funcion es un df de pandas con la columna producto_cd y ccf_vl

print('Comienza ccf_producto()')
ccf = ccf_producto(CCF_NoExced) # se corre la funcion ccf_producto
ccf_excedidos = ccf_producto(CCF_Exced) # se corre la funcion ccf_producto


# Se define la funcion lgd_sin_garantia que calcula la lgd para operaciones sin garantias.

def lgd_sin_garantia(df): 
    saldo_total = sum(df.saldo_restante) # calculo saldo total
    df['saldo_ponderado'] = df['saldo_restante']/saldo_total # agrego columna con saldo ponderado
    df['lgd_ponderado'] = df['LGD'] * df['saldo_ponderado'] # agrego columna con lgd ponderada
    lgd = sum(df.lgd_ponderado) # calculo la lgd como la suma de todas las lgd ponderadas
    lgd_std = np.std(df.lgd_ponderado) # calculo el desvio de las lgd ponderadas
    return lgd, lgd_std # la salida de esta funcion es un valor flotante que representa la lgd para cuentas sin garantia y otro que representa el desvio estandard de la lgd

print('Comienza lgd_sin_garantia()')
lgd_sg, lgd_sg_std = lgd_sin_garantia(LGD_df) # se corre la funcion lgd_sin_garantia


# Se define la funcion tabla_lgd que importa las lgd de los productos con garantia y agrega sin garantia

def tabla_lgd(sin_garantia_vl,sin_garantia_std,con_garantia_df):
    df = con_garantia_df.copy() # copio el df input de todos los tipos de garantia excepto sin garantia
    df.rename(columns={'LGD':'lgd_vl'},inplace=True)  # renombro columna LGD a lgd_vl
    sin_garantia = [6,sin_garantia_vl,sin_garantia_std] # genero lista para poblar la tabla lgd (el cd_garantia es 0 y el valor es input de la funcion)
    df.loc[len(df)] = sin_garantia # inserto el registro de lgd de sin garantia
    return df # la salida de esta funcion es un df de pandas con la columna cd_garantia y lgd_vl

print('Comienza tabla_lgd()')
lgd = tabla_lgd(lgd_sg,lgd_sg_std,LGD_con_garantia) # se corre la funcion tabla_lgd


# Se define la funcion asigna_pd que le asigna a cada operacion una pd basada en la definicion metodologica

def asigna_pd(bgdatos_df,pd_mv):
    pd_mv.rename(columns={'cd_cuit':'cdi'},inplace=True) # se le cambia el nombre a la columna cd_cuit en pd_mas_valorar para que pueda hacer join con tabla df
    pd_mv = pd_mv.sort_values('PD', ascending=False).drop_duplicates('cdi').sort_index() # drop filas donde se encuentra el mismo cliente duplicado en la base de pd_mas_valorar mantengo la fila que tiene la PD mas alta
    df = pd.merge(bgdatos_df, pd_mv, on = ['cdi'], how='left') # merge entre df y pd_mas_valorar para obtener los datos de clientes que la tabla clientes no tiene.
    df.loc[(df['cd_clasificacion_bcra'] == 'C ') | (df['cd_clasificacion_bcra'] == 'D ') | (df['cd_clasificacion_bcra'] == 'E '),'PD'] = 1
    df_prom = df[['cd_clasificacion_bcra','cd_segmento_x','PD']].copy() # creamos dataframe con las variables cd_clasificacion_bcra, cd_segmento y PD para generar las medias de PD
    df_prom = df_prom.groupby(['cd_clasificacion_bcra', 'cd_segmento_x']).mean().reset_index() # agrupamos las PD por los primeros dos campos de la tabla
    df_prom.columns = ['cd_clasificacion_bcra','cd_segmento_x','PD_mean'] # renombramos las columnas
    df = pd.merge(df, df_prom, on= ['cd_clasificacion_bcra','cd_segmento_x'], how = 'left') # asignamos la PD_mean a todas las operaciones
    df_PD = df[['cd_tipo_doc','cdi','cd_operacion','cd_clasificacion_bcra','cd_segmento_x','PD','PD_mean']].copy() # tomamos un subset de columnas del df completo
    df_PD['pd_vl'] = np.where(df_PD['PD'].isnull(),df_PD['PD_mean'],df_PD['PD']) # creo columna pd_vl y le asigno el valor de pd de la operacion
    df_PD.drop(['PD','PD_mean'],axis=1,inplace=True) # dropeo columnas de pd que no son finales
    if df_PD['pd_vl'].isnull().values.any(): # generamos Log de error si todavía quedan nulos en la base
        print('Hay operaciones sin ninguna PD asignada') # esto es un control
    df_PD.rename(columns={'cd_segmento_x':'cd_segmento'},inplace=True) # renombro columna cd_segmento_x a cd_segmento para poder mergear con bgdatos
    df_PD.drop_duplicates(inplace=True) # drop de registros duplicados
    df_output = pd.merge(bgdatos_df,df_PD,on=['cd_tipo_doc','cdi','cd_operacion','cd_clasificacion_bcra','cd_segmento'],how='left') # mergeo bgdatos con la pd final
    return df_output # la salida de esta funcion es el df de bgdatos con una columna extra que es la pd asignada

print('Comienza asigna_pd()')
bgdatos_PD = asigna_pd(bgdatos,pd_mas_valorar) # se corre la funcion asigna_pd


# Se define la funcion asigna_ccf que le va a asignar a la base bgdatos un valor de ccf para excedidos y otro para no excedidos segun el tipo de producto

def asigna_ccf(bgdatos_df,ccf_df,ccf_excedidos_df):
    ccf_excedidos_df.rename(columns={'ccf_vl':'ccf_excedido_vl'},inplace=True) # renombro columna ccf_vl para poder hacer merge con la tabla de ccf no excedido
    ccf_consolidado_df = pd.merge(ccf_df,ccf_excedidos_df,on=['producto_cd'],how='left') # merge entre tablas de ccf por producto_cd
    ccf_consolidado_df.rename(columns={'producto_cd':'cd_sistema'},inplace=True) # renombro columna producto_cf a cd_sistema para poder hacer merge
    df = pd.merge(bgdatos_df,ccf_consolidado_df,on=['cd_sistema'],how='left') # merge entre bgdatos y ccf
    df['ccf_vl'] = df['ccf_vl'].fillna(0) # para aquellos productos que no esten en la tabla de ccf se reemplaza el valor nulo por 0
    df['ccf_excedido_vl'] = df['ccf_excedido_vl'].fillna(0) # para aquellos productos que no esten en la tabla de ccf se reemplaza el valor nulo por 0
    if df['ccf_vl'].isnull().values.any(): # generamos Log de error si todavía quedan nulos en la base
        print('Hay operaciones sin ningun ccf asignado') # esto es un control
    if df['ccf_excedido_vl'].isnull().values.any(): # generamos Log de error si todavía quedan nulos en la base
        print('Hay operaciones sin ningun ccf_excedido asignado') # Esto es un control
    return df # la salida de esta funcion es el df bgdatos_PD con dos columnas extra que es el ccf asignado para no excedido y excedido respectivamente

print('Comienza asigna_ccf()')
bgdatos_PD_CCF = asigna_ccf(bgdatos_PD,ccf,ccf_excedidos) # se corre la funcion asigna_ccf


# Se define la funcion asigna_lgd que le va a asignar el valor de lgd segun el tipo de garantia

def asigna_lgd(bgdatos_df,lgd_df):
    lgd_df.rename(columns={'cd_garantia':'cd_garantia_grupo'},inplace=True) # renombro columna cd_garantia a cd_garantia_grupo para poder hacer merge con bgdatos
    df = pd.merge(bgdatos_df,lgd_df,on=['cd_garantia_grupo'],how='left') # merge entre tabla de lgd por grupo garantia y bgdatos
    if df['lgd_vl'].isnull().values.any(): # generamos Log de error si todavía quedan nulos en la base
        print('Hay operaciones sin ningun lgd asignado') # esto es un control
    return df # la salida de esta funcion es el df de bgdatos_PD_CCF con dos columnas extra que representan la lgd y su desvio

print('Comienza asigna_lgd()')
bgdatos_PD_CCF_LGD = asigna_lgd(bgdatos_PD_CCF,lgd) # se corre la funcion asigna_lgd


# Se define la funcion calcula_ead que calcula el valor de ead utilizando el ccf previamente asignado, el saldo y el limite

def calcula_ead(df):
    df = df.copy() # copio la base bgdatos
    df['ead_excedidos'] = df['vl_saldo'] * df['ccf_excedido_vl'] # genero columna de ead para cuentas excedidas
    df['ead_no_excedidos'] = df['vl_saldo'] + (df['vl_limite'] - df['vl_saldo']) * df['ccf_vl'] # genero columna ead para cuentas no excedidas
    df['ead_vl'] = np.where(df['vl_saldo']<=df['vl_limite'], df['ead_no_excedidos'], df['ead_excedidos']) # asigno ead correcto para cada cuenta en una columna nueva
    df.drop(columns=['ead_excedidos','ead_no_excedidos'],inplace=True) # drop de columnas ead_excedidos y ead_no_excedidos
    return df # la salida de esta funcion es el df de bgdatos con una columna extra que es el ead

print('Comienza calcula_ead()')
bgdatos_final = calcula_ead(bgdatos_PD_CCF_LGD) # se corre la funcion calcula_ead


# Se define la funcion calcula_perdida_esperada que calcula la perdida esperada y el vector por operacion de perdida esperada

def calcula_perdida_esperada(bgdatos_df):
    bgdatos_df['perdida_esperada_vl'] = bgdatos_df['pd_vl'] * bgdatos_df['ead_vl'] * bgdatos_df['lgd_vl'] # agrega columna a base bgdatos con los valores de perdida esperada
    perdida_esperada_vector = bgdatos_df['perdida_esperada_vl'].copy() # extrae el vector de perdida esperada
    perdida_esperada_vl = sum(perdida_esperada_vector) # suma elementos del vector de perdida esperada
    return perdida_esperada_vl, perdida_esperada_vector # la salida de esta funcion son el valor de perdida esperada y el vector con el que se calculo esa perdida esperada

print('Comienza calcula_perdida_esperada()')
pe_vl, pe_vec = calcula_perdida_esperada(bgdatos_final)


# Se crea una funcion para modificar a bgdatos_final

def modifica_bgdatos(df):
    df = df.copy()
    df['fc_calculo'] = fecha_calculo # Se crea un campo con la fecha de calculo ingresada al comienzo
    df['fc_alta_mod1'] = df['fc_alta'] # Se crea el campo 'fc_alta_mod1' con la informacion de 'fc_alta'
    df.loc[(df['cd_sistema']=='HA') | (df['cd_sistema']=='HC') | (df['cd_sistema']=='GT'), 'fc_alta_mod1'] = df['fc_calculo'] # Si "cd_sistema" es HA, HC, GT entonces a "fc_alta_mod1" se le asigna el valor del campo "fc_calculo"
    df.loc[((df['cd_sistema']=='DE') | (df['cd_sistema']=='SAA')) & (df['fc_alta']=='0000-00-00'), 'fc_alta_mod1'] = df['fc_calculo'] # Se "cd_sistema" es DE ó SAA y "fc_alta"=0000-00-00 entonces a "fc_alta_mod1" se le asigna el valor del campo "fc_calculo"
    df['fc_alta_mod1'] = np.where((df['cd_sistema']=='HA') | (df['cd_sistema']=='HC') | (df['cd_sistema']=='GT'), df['fc_calculo']+timedelta(days=30), df['fc_cierre']) # Se crea el campo 'fc_cierre_mod1' con las condiciones correspondientes   
    df['vl_saldo_sin_aval'] = np.where((df['vl_saldo']==0) & (df['cd_tipo_producto']==1), 1, df['vl_saldo']) # Se realizan las modificaciones sobre 'vl_saldo_sin_aval'  
    df['vl_limite_mod1'] = df['vl_limite'] # Se crea el campo 'vl_limite_mod1' con la informacion de 'vl_limite'    
    # NOTA: se cambió en la condiciones siguientes de "cd_tipo_producto!=0" a "cd_tipo_producto!=1"
    df.loc[(df['cd_tipo_producto']==1) & ((df['cd_sistema']=='HA') | (df['cd_sistema']=='HC') | (df['cd_sistema']=='GT')), 'vl_limite_mod1'] = df['vl_saldo']
    df.loc[(df['cd_tipo_producto']!=1) & ((df['cd_sistema']=='HA') | (df['cd_sistema']=='HC') | (df['cd_sistema']=='GT')), 'vl_limite_mod1'] = df['vl_saldo'] + df['vl_limite']
    df.loc[(df['cd_tipo_producto']!=1) & ((df['cd_sistema']!='HA') & (df['cd_sistema']!='HC') & (df['cd_sistema']!='GT')), 'vl_limite_mod1'] = df['vl_limite']   
    # Se crea el campo 'vl_limite_ccf' con las condiciones correspondientes
    df['vl_limite_ccf'] = np.where((df['cd_sistema']=='HA') | (df['cd_sistema']=='HC') | (df['cd_sistema']=='GT'), df['vl_limite'], df['vl_saldo'])
    return df

print("Comienza modifica_bgdatos()")
bgdatos_mod = modifica_bgdatos(bgdatos_final)


# Se calculan los intereses promedio de la cartera

def calcula_intereses(df1, df2): 
    df1 = df1.copy()
    df2 = df2.copy()
    df1['day_of_calendar'] = df2['day_of_calendar'] # se crea el campo 'day_of_calendar' en el primer dataframe (bgdatos_mod) con la informacion del segundo dataframe (calendario)
    df1['vl_intereses_pond'] = sum((df1['vl_saldo_sin_aval']) * df1['vl_tna']) / sum(df1['vl_saldo_sin_aval']) # Se crea el campo 'vl_intereses_ponderador'
    df1['vl_dia_cierre_pond'] = sum(df2['day_of_calendar'] * abs(df1['vl_saldo_sin_aval'])) / sum(abs(df1['vl_saldo_sin_aval'])) # Se crea el campo 'vl_dia_cierre_pond'
    df1['vl_saldoC'] = sum(df1['vl_saldo_sin_aval']) # Se crea el campo 'vl_saldoC' como la suma de 'vl_saldo_sin_aval'
    df1['fc_calculo'] = min(df1['fc_calculo'])  # Se cambia la informacion de 'fc_calculo' como el minimo de dicho campo
    return df1

print("Se calculan los intereses promedio")
bg_cerce_bgdatos_cierre_1 = calcula_intereses(bgdatos_mod, calendario)


# Se define una funcion para calcular el Plazo Operacion

def plazo_operacion(df):
    df = df.copy()    
    # Para los campos 'fc_cierre' y 'fc_alta' se modifican todos aquellos registros que tengan fecha '0000-00-00' o '1900-01-01'
    df['fc_cierre'].loc[df['fc_cierre']=='0000-00-00'] = datetime(2100, 12, 31)
    df['fc_cierre'].loc[df['fc_cierre']=='1900-01-01'] = datetime(2100, 12, 31)
    df['fc_alta'].loc[df['fc_alta']=='0000-00-00'] = datetime(2100, 12, 31)
    df['fc_alta'].loc[df['fc_alta']=='1900-01-01'] = datetime(2100, 12, 31)
    # Se crea un campo auxilair 'fl_cierre' donde se pone en 0 a aquellos que estan en '0000-00-00' o '1900-01-01'; y 1 son aquellos que sí tienen fecha
    df['fl_cierre'] = np.where(np.logical_or(df['fc_cierre']==datetime(2100, 12, 31),df['fc_alta']==datetime(2100, 12, 31)),0,1)
    # Se crea una columna 'plazo_operacion' como la diferencia entre 'fc_cierre' y 'fc_alta' para los registros que en 'fl_cierre' estan en 1, sino se toma la diferencia entre la fecha actual y la 'fc_alta'
    df['plazo_operacion'] = np.where(df['fl_cierre'] == 1, df['fc_cierre']-df['fc_alta'], datetime.today()-df['fc_alta'])
    # Se crea el campo 'fc_cierre_de_saa_est'
    df.loc[datetime.now()-df['fc_cierre'] > timedelta(days=365), 'fc_cierre_de_saa_est'] = df['fc_cierre']+timedelta(days=365)   
    df.loc[(datetime.now()-df['fc_cierre'] > timedelta(days=0)) & (datetime.now()-df['fc_cierre'] < timedelta(days=365)), 'fc_cierre_de_saa_est'] = datetime.now()
    return df

print("Se calcula el plazo de la operacion")
bg_cerce_bgdatos_cierre_2 = plazo_operacion(bg_cerce_bgdatos_cierre_1)    


# Se estima una fecha de alta para operaciones en legales un año previo al inicio de la mora 

def estima_fecha_alta(df):
    df = df.copy()
    df1 = df[['cdi', 'cd_operacion', 'fc_ini_mora']].loc[df['cd_sistema']=='LEG'].copy()
    df1 = df1.groupby(['cdi', 'cd_operacion']).min().reset_index() # 'fc_iniu_mora' todo en nan o 0000-00-00
    return df1

print("Se estima una fecha de alta")
bg_cerce_bgdatos_alta_1 = estima_fecha_alta(bgdatos_final)


# Se define una funcion para modificar bgdatos_mod

def modifica_bgdatos_mod(df):
    df = df.copy()   
    df['fecha_cierre_de_saa_est'] = 'Falta Codificar' # Se crea el campo fecha_cierre_de_saa_est y se asifna por default el valor 'Falta Codificar'
    df['fc_cierre_ok'] = np.where((df['cd_sistema']=='DE') | (df['cd_sistema']=='SAA'), df['fecha_cierre_de_saa_est'], df['fc_cierre']) # Se crea el campo fc_cierre_ok y se le asigna el valor de 'fecha_cierre_de_saa_est' o 'fc_cierre_mod1' segun corresponda 
    fc_calculo_auxiliar = '1990/01/01' # Se crea una variable auxiliar en formato str
    fecha_calculo_aux = datetime.strptime(fc_calculo_auxiliar, '%Y/%m/%d') # Se modifica la variable 'fc_calculo_auxiliar' de formato str a formato fecha
    # Se modifican los valores de 'fc_alta' que esten en formato str y se los pasa a formato fecha (se pone por default la fecha 31/12/2100)
    df['fc_alta'].loc[df['fc_alta']=='0000-00-00'] = datetime(2100, 12, 31)
    df['fc_alta'].loc[df['fc_alta']=='1900-01-01'] = datetime(2100, 12, 31)
    # Se crea un campo auxiliar 'fl_alta' con valor 0 para aquellos registros en donde 'fc_alta' esta en '0000-00-00' o '1900-01-01'; o en valor 1 para aquellos registros donde 'fc_alta' sí tiene valor en formato fecha
    df['fl_alta'] = np.where(df['fc_alta']==datetime(2100, 12, 31),0,1)
    # Finalmente se define 'fc_alta_ok'
    df['fc_alta_ok'] = np.where((df['cd_sistema']=='LEG') & ((df['fc_alta']=='0000-00-00') | (df['fl_alta']==1 & (df['fc_alta']<fecha_calculo_aux))), df['fecha_cierre_de_saa_est'], df['fc_alta']-timedelta(365))
    # Se crea el campo 'nu_mora_ok'
    df['nu_mora_ok'] = df['nu_mora']      
    # Se crea el campo 'vl_ponderador'
    df['vl_ponderador'] = np.where(df['vl_saldo_sin_aval'] > df['vl_limite_mod1'], df['vl_saldo_sin_aval']+0.001, df['vl_limite_mod1']+0.001)    
    return df
    
print("Se realizan las modificaciones sobre 'bg_cerce_bgdatos_1'")
bg_cerce_bgdatos_2 = modifica_bgdatos_mod(bg_cerce_bgdatos_cierre_1)
    

# Se define una funcion para modificar 'bg_cerce_bgdatos_2'

def modifica_bg_cerce_bgdatos_2(dff1, dff2):
    dff1 = dff1.copy()
    dff2 = dff2.copy()
    df = pd.merge(dff1, dff2, on =['cdi'], how='left')   
    # Tabla 1
    df1 = df[['cdi', 'cd_operacion_x', 'fc_calculo']].copy() # Se crea un dataframe con las columnas 'cdi', 'cd_operacion_x', 'fc_calculo'
    df1 = df1.groupby(['cdi', 'cd_operacion_x']).min() #.Se agrupa por 'cdi' y 'cd_operacion' y luego se calcula el minimo sobre 'fc_calculo' 
    df1.rename(columns = {'fc_calculo':'fc_op_calculo_min'}, inplace = True) # Se cambia el nombre del campo 'fc_calculo' por 'fc_op_calculo_min'
    # Tabla 2
    df2 = df[['cdi', 'cd_operacion_x', 'fc_alta_x']].copy()
    df2 = df2.groupby(['cdi', 'cd_operacion_x']).min() 
    df2.rename(columns = {'fc_alta_x':'fc_op_alta_min'}, inplace = True)
    # Tabla 3
    df3 = df[['cdi', 'cd_operacion_x', 'fc_cierre_x']].copy()
    df3 = df3.groupby(['cdi', 'cd_operacion_x']).max() 
    df3.rename(columns = {'fc_cierre_x':'fc_op_cierre_max'}, inplace = True)
    # Tabla 4
    df4 = df[['cdi', 'cd_operacion_x', 'vl_monto_inicial_x']].copy()
    df4 = df4.groupby(['cdi', 'cd_operacion_x']).sum() 
    df4.rename(columns = {'vl_monto_inicial_x':'vl_op_monto_inicial_sum'}, inplace = True)
    # Tabla 5
    df5 = df[['cdi', 'cd_operacion_x', 'vl_saldo_sin_aval']].copy()
    df5 = df5.groupby(['cdi', 'cd_operacion_x']).sum() 
    df5.rename(columns = {'vl_saldo_sin_aval':'vl_op_saldo_sum'}, inplace = True)
    # Tabla 6
    df6 = df[['cdi', 'cd_operacion_x', 'vl_limite_mod1']].copy()
    df6 = df6.groupby(['cdi', 'cd_operacion_x']).sum() 
    df6.rename(columns = {'vl_limite_mod1':'vl_op_limite_sum'}, inplace = True)
    # Tabal 7
    df7 = df[['cdi', 'cd_operacion_x', 'vl_limite_ccf']].copy()
    df7 = df7.groupby(['cdi', 'cd_operacion_x']).sum() 
    df7.rename(columns = {'vl_limite_ccf':'vl_op_limite_ccf'}, inplace = True)
    # Tabla 8
    auxiliar =  sum(df['vl_ponderador']*df['vl_tna_x']) / sum(df['vl_ponderador'])
    df['vl_op_tasa_pond'] = auxiliar
    df8 = df[['cdi', 'cd_operacion_x', 'vl_op_tasa_pond']].copy()
    # Tabla 9
    df9 = df[['cdi', 'cd_operacion_x', 'nu_mora_x']].copy()
    df9 = df9.groupby(['cdi', 'cd_operacion_x']).max() 
    df9.rename(columns = {'nu_mora_x':'nu_op_mora_max'}, inplace = True)
    # Tabla 10
    df10 = df[['cdi', 'cd_operacion_x', 'nu_mora_x']].copy()
    df10 = df10.groupby(['cdi', 'cd_operacion_x']).max() 
    df10.rename(columns = {'nu_mora_x':'nu_op_mora_bcra_max'}, inplace = True)
    # Tabla 11
    df11 = df[['cdi', 'cd_operacion_x', 'cd_clasificacion_bcra_x']].copy()
    df11 = df11.groupby(['cdi', 'cd_operacion_x']).max() 
    df11.rename(columns = {'cd_clasificacion_bcra_x':'cd_op_clasificacion_bcra_max'}, inplace = True)
    # Tabla 12
    df12 = df[['cdi', 'cd_operacion_x', 'cd_moneda_x']].copy()
    df12 = df12.groupby(['cdi', 'cd_operacion_x']).min() 
    df12.rename(columns = {'cd_moneda_x':'cd_op_moneda_min'}, inplace = True)
    # Tabla 13
    df13 = df[['cdi', 'cd_operacion_x', 'cd_ajuste_cer_x']].copy()
    df13 = df13.groupby(['cdi', 'cd_operacion_x']).mean() 
    df13.rename(columns = {'cd_ajuste_cer_x':'cd_op_sistema_amort_avg'}, inplace = True)
    # Tabla 14
    df14 = df[['cdi', 'cd_operacion_x', 'vl_ponderador']].copy()
    df14 = df14.groupby(['cdi', 'cd_operacion_x']).sum() 
    df14.rename(columns = {'vl_ponderador':'vl_op_ponderador_sum'}, inplace = True)
    # Tabla Output
    df15 = [df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11, df12, df13, df14]  
    df_output = reduce(lambda  left,right: pd.merge(left,right,on=['cdi','cd_operacion_x'], how='outer'), df15)
    return df_output
  
print("Se realizan calculos aritmeticos sobre 'bg_cerce_bgdatos_2'")
bg_cerce_bgdatos_agrup_op_00 = modifica_bg_cerce_bgdatos_2(bg_cerce_bgdatos_2, bgdatos_PD) # bg_cerce_bgdatos_2


# Se define una funcion para seleccionar la garantia grupo de la linea con mayor deuda

def garantia_mayor_deuda(df): 
    df = df.copy()
    df1 = df[['cdi', 'cd_operacion', 'cd_garantia_grupo' ,'vl_saldo_sin_aval']].copy() #  Se crea un nuevo dataframe con los campos 'cdi', 'cd_operacion', 'cd_garantia_grupo' ,'vl_saldo_sin_aval'
    df1 = df1.sort_values('vl_saldo_sin_aval', ascending=False).reset_index() # Se ordena en forma descendente por 'vl_saldo_sin_aval'
    df2 = df1.groupby(['cdi', 'cd_operacion']).first() # Se agrupa por 'cdi' y 'cd_operacion' y se toma el primer valor de 'cd_garantia_grupo' ,'vl_saldo_sin_aval'
    df2.rename(columns = {'cd_garantia_grupo':'cd_garantia_grupo_max'}, inplace = True)
    #df3 = pd.merge(df, df2, on=['cdi'], how='left') # se unen los 2 dataframes creados
    #df3.drop(labels='cd_garantia_grupo_x', axis=1)
    #df3.rename(columns = {'cd_garantia_grupo_y':'cd_garantia_grupo_max'}, inplace = True)
    return df2

print("Se selecciona la garantia grupo de la linea con mayor deuda")
cd_garantia_grupo_0 = garantia_mayor_deuda(bg_cerce_bgdatos_2)


# Se define una funcion para rankear el cd_producto por operación, según cual tenga mayor saldo ponderador

def ranking_producto(df): 
    df = df.copy()
    df1 = df[['cdi', 'cd_operacion', 'vl_ponderador']].copy() # Se crea un dataframe con los campos 'cdi', 'cd_operacion', 'vl_ponderador'
    df1 = df1.sort_values('vl_ponderador', ascending=False).reset_index() # Se ordena el dataframe de mayor a menos por 'vl_ponderador'
    df2 = df1.groupby(['cdi', 'cd_operacion']).first()
    df2.rename(columns = {'vl_ponderador':'vl_ponderador_max'}, inplace = True)
    return df2

print("Se selecciona la operación con mayor saldo ponderador")
bg_cerce_bgdatos_agrup_prod_0 = ranking_producto(bg_cerce_bgdatos_2)


def poblar_tabla(df):
    pass



'''
simulaciones = 10
simulacion_default = pd.DataFrame(bgdatos_final[['pd_vl','nu_mora']])
simulacion_lgd = pd.DataFrame(bgdatos_final[['lgd_vl','lgd_std']])
for sim in tqdm(range(simulaciones)):
    simulacion_default['random_val'] = np.random.rand(len(simulacion_default))
    simulacion_default['default_bl'] = np.where(np.logical_or(simulacion_default['random_val'] < simulacion_default['pd_vl'], simulacion_default['nu_mora'] >= 90),1,0)
    simulacion_lgd['random_val'] = np.random.rand(len(simulacion_lgd))
    simulacion_lgd['lgd_aplica'] = norm.ppf(simulacion_lgd['random_val'],loc=simulacion_lgd['lgd_vl'],scale=simulacion_lgd['lgd_std'])
    simulacion_lgd['lgd_aplica'] = np.where(simulacion_lgd['lgd_aplica']<0,0,simulacion_lgd['lgd_aplica'])
    simulacion_lgd['lgd_aplica'] = np.where(simulacion_lgd['lgd_aplica']>1,1,simulacion_lgd['lgd_aplica'])
'''
