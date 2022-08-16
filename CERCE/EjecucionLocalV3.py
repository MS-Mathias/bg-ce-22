# Se importan las librerias 
import pandas as pd
import numpy as np
from tqdm import tqdm
from scipy.stats import norm
import math

# Se importan todas las tablas input que requiere el modelo
print('Comienza el import de inputs!')
bgdatos = pd.read_excel('bgdatos.xlsx',index_col=[0]) # importo bgdatos
pd_mas_valorar = pd.read_excel('pd_mas_valorar.xlsx') # importo pd mas valorar
CCF_NoExced = pd.read_excel('CCF.xlsx','Sheet1',index_col=[0]) # importo ccf para los no excedidos
CCF_Exced = pd.read_excel('CCF.xlsx','Sheet2',index_col=[0]) # importo ccf para los excedidos
LGD_df = pd.read_excel('LGD.xlsx','Sheet1',index_col=[0]) # importo lgd sin garantia
LGD_con_garantia = pd.read_excel('LGD.xlsx','Sheet2',index_col=[0]) # importo lgd con garantia

# Se define la funcion strip que elimina espacios de todas las columnas str de un dataframe

def strip(df):
    columnas = df.columns # creo lista de columnas en el dataframe input
    for col in columnas: # loopeo a traves de todas las columnas
        if isinstance(df[col][0],str): # condicion si la columna tiene contenido de texto
            df[col] = df[col].str.strip() # si pasa la condicion anterior, elimina los espacios al final de cada celda en esa columna

strip(bgdatos)
strip(pd_mas_valorar)
strip(CCF_NoExced)
strip(CCF_Exced)
strip(LGD_df)
strip(LGD_con_garantia)

# Se define funcion control de columnas

def control_columnas(bgdatos_df):
    bgdatos_df=bgdatos_df.copy()
    columnas_bgdatos = ['cd_tipo_doc', 'cdi', 'cd_operacion', 'cd_sistema', 'tx_linea',
           'cd_garantia', 'fc_alta', 'fc_cierre', 'vl_monto_inicial', 'vl_saldo',
           'vl_limite', 'cd_garantia_grupo', 'vl_tna', 'vl_tem', 'vl_cft',
           'cd_tipo_producto', 'nu_mora', 'cd_segmento', 'cd_clasificacion_bcra',
           'cd_moneda', 'cd_ajuste_cer', 'fc_ini_mora', 'tx_campo_extra'] # creo lista de columnas que tiene que tener bgdatos
    if bgdatos_df.columns.to_list() != columnas_bgdatos: # esto es un control
        print('Las columnas del input bgdatos no son correctas!') 
        
control_columnas(bgdatos) # se ejecuta la funcion control_columnas

# Se define la funcion ccf_producto para calcular el ccf de cada producto que corresponde

def ccf_producto(df):
    df = df.copy()
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
    df=df.copy()
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
    bgdatos_df, pd_mv = bgdatos_df.copy(), pd_mv.copy()
    pd_mv.rename(columns={'cd_cuit':'cdi'},inplace=True) # se le cambia el nombre a la columna cd_cuit en pd_mas_valorar para que pueda hacer join con tabla df
    pd_mv = pd_mv.sort_values('PD', ascending=False).drop_duplicates('cdi').sort_index() # drop filas donde se encuentra el mismo cliente duplicado en la base de pd_mas_valorar mantengo la fila que tiene la PD mas alta
    df = pd.merge(bgdatos_df, pd_mv, on = ['cdi'], how='left') # merge entre df y pd_mas_valorar para obtener los datos de clientes que la tabla clientes no tiene.
    df.loc[(df['cd_clasificacion_bcra'] == 'C') | (df['cd_clasificacion_bcra'] == 'D') | (df['cd_clasificacion_bcra'] == 'E'),'PD'] = 1
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
bgdatos_mod = asigna_pd(bgdatos,pd_mas_valorar) # se corre la funcion asigna_pd

# Se define la funcion asigna_ccf que le va a asignar a la base bgdatos un valor de ccf para excedidos y otro para no excedidos segun el tipo de producto

def asigna_ccf(bgdatos_df, ccf_df, ccf_excedidos_df):
    bgdatos_df, ccf_df, ccf_excedidos_df = bgdatos_df.copy(), ccf_df.copy(), ccf_excedidos_df.copy()
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
bgdatos_mod = asigna_ccf(bgdatos_mod,ccf,ccf_excedidos) # se corre la funcion asigna_ccf

# Se define la funcion asigna_lgd que le va a asignar el valor de lgd segun el tipo de garantia

def asigna_lgd(bgdatos_df,lgd_df):
    lgd_df.rename(columns={'cd_garantia':'cd_garantia_grupo'},inplace=True) # renombro columna cd_garantia a cd_garantia_grupo para poder hacer merge con bgdatos
    df = pd.merge(bgdatos_df,lgd_df,on=['cd_garantia_grupo'],how='left') # merge entre tabla de lgd por grupo garantia y bgdatos
    if df['lgd_vl'].isnull().values.any(): # generamos Log de error si todavía quedan nulos en la base
        print('Hay operaciones sin ningun lgd asignado') # esto es un control
    return df # la salida de esta funcion es el df de bgdatos_PD_CCF con dos columnas extra que representan la lgd y su desvio

print('Comienza asigna_lgd()')
bgdatos_mod = asigna_lgd(bgdatos_mod,lgd) # se corre la funcion asigna_lgd

# Se define la funcion calcula_ead que calcula el valor de ead utilizando el ccf previamente asignado, el saldo y el limite

def calcula_ead(bgdatos_df):
    df = bgdatos_df.copy()  # copio parametros como variables temporales de la funcion operativa
    df['ead_excedidos'] = df['vl_saldo'] * df['ccf_excedido_vl'] # genero columna de ead para cuentas excedidas
    df['ead_no_excedidos'] = df['vl_saldo'] + (df['vl_limite'] - df['vl_saldo']) * df['ccf_vl'] # genero columna ead para cuentas no excedidas
    df['ead_vl'] = np.where(df['vl_saldo']<=df['vl_limite'],df['ead_no_excedidos'],df['ead_excedidos']) # asigno ead correcto para cada cuenta en una columna nueva
    df.drop(columns=['ead_excedidos','ead_no_excedidos'],inplace=True) # drop de columnas ead_excedidos y ead_no_excedidos
    return df # la salida de esta funcion es el df de bgdatos con una columna extra que es el ead

print('Comienza calcula_ead()')
bgdatos_mod = calcula_ead(bgdatos_mod) # se corre la funcion calcula_ead

# Se define la funcion calcula_perdida_esperada que calcula la perdida esperada y el vector por operacion de perdida esperada

def calcula_perdida_esperada(bgdatos_df):
    bgdatos_df = bgdatos_df.copy() # copio parametros como variables temporales de la funcion operativa
    bgdatos_df['perdida_esperada_vl'] = bgdatos_df['pd_vl'] * bgdatos_df['ead_vl'] * bgdatos_df['lgd_vl'] # agrega columna a base bgdatos con los valores de perdida esperada
    perdida_esperada_vector = bgdatos_df['perdida_esperada_vl'].copy() # extrae el vector de perdida esperada
    perdida_esperada_vl = sum(perdida_esperada_vector) # suma elementos del vector de perdida esperada
    return perdida_esperada_vl, perdida_esperada_vector # la salida de esta funcion son el valor de perdida esperada y el vector con el que se calculo esa perdida esperada

print('Comienza calcula_perdida_esperada()')
pe_vl, pe_vec = calcula_perdida_esperada(bgdatos_mod) # se corre la funcion calcula_perdida_esperada

# Se define la funcion prepara_simulacion que agrega algunos parametros claves para realizar las simulaciones

def prepara_simulacion(df,lamda = 1):
    df = df.copy() # copio parametros como variables temporales de la funcion operativa
    
    desvio_sectorial = df[['cd_segmento','pd_vl']].copy() # tomo un extracto de la tabla con las variables que me interesan para calcular los desvios sectoriales
    desvio_sectorial = desvio_sectorial.groupby(['cd_segmento']).std().reset_index() # agrupo la tabla por segmento (deberia ser sector) y pido que me traiga el desvio de la PD
    desvio_sectorial['pd_vl'] *= lamda # multiplico el desvio por el parametro lamda
    desvio_sectorial.rename(columns={'pd_vl':'desvio_sectorial'},inplace=True) # renombro la columna pd_vl a desvio_sectorial
    
    df['plazo_devengado'] = 30 # np.where(np.logical_or(df['cd_garantia_grupo'] == 6, df['plazo'] >= 360),30,df['plazo']) # calculo el plazo devengado
    df['beta'] = ((math.e ** (5 * df['nu_mora'] / 90)) - 1) / ((math.e ** 5) - 1) # calculo beta
    df['PDI'] = np.where(df['nu_mora'] >= 90, 1, 1 - ((1 - df['pd_vl']) ** ((df['plazo_devengado'] + df['nu_mora']) / 360))) # calculo PDI
    df['PDI_ajust'] = np.where(df['nu_mora'] >= 90, 1, df['beta'] + df['PDI'] * (1 - df['beta'])) # calculo PDI ajustada
    df['PD_ajust'] = np.where(df['nu_mora'] >= 90, 1, df['beta'] + df['pd_vl'] * (1 - df['beta'])) # calculo PD ajustada
    
    return df, desvio_sectorial # el output de esta funcion son dos tablas, una contiene toda la base bgdatos con los nuevos parametros ya calculados, la otra tiene los desvios por sector (segmento)

df_simular, df_desvio_sectorial = prepara_simulacion(bgdatos_mod) # se corre la funcion prepara_simulacion
df_simular.drop_duplicates('cdi',inplace=True)
sims_nu = 10000 # defino el numero de simulaciones


# Se define la funcion simula que simula con valores aleatorios el comportamiento de la cartera de empresas.

def simula(df,ds_df,simulaciones,lgd_vl,lgd_std):
    df,ds_df = df[['cdi','cd_segmento','cd_garantia_grupo','PD_ajust','PDI_ajust','lgd_vl','ead_vl']].copy(),ds_df.copy() # copio parametros como variables temporales de la funcion operativa
    
    df = pd.merge(df,ds_df[['cd_segmento','desvio_sectorial']],on=['cd_segmento'],how='left') # le asigno el valor de Fs a todas las operaciones segun el segmento con un pd.merge
    pd_sim = np.random.randn(simulaciones,len(df)) # genero valores aleatorios de distribucion normal para cada sector para las n simulaciones
    
    for sim in tqdm(range(simulaciones)):
        pd_sim[sim] = math.e ** ((-0.5 * (df['desvio_sectorial'].values ** 2)) + (df['desvio_sectorial'].values * pd_sim[sim])) # calculo en el df de desvios el valor de Fs
        pd_sim[sim] = df['PD_ajust'].values * pd_sim[sim] # calculo PDf
        pd_sim[sim] = df['PDI_ajust'] + ((1 - df['PDI_ajust']) * pd_sim[sim]) # calculo PD simulada
        pd_sim[sim] = np.clip(pd_sim[sim], 0, 1,)
    
    default = np.random.rand(simulaciones,len(df)) # genero el numero aleatorio de distribucion uniforme 0-1 para simular el default   
    
    for sim in tqdm(range(simulaciones)):
        default[sim] = np.less(default[sim],pd_sim[sim])
        default[sim] = np.where(default[sim],1,0)

    lgd_sim = np.random.randn(simulaciones,len(df)) * lgd_std + lgd_vl # genero el numero aleatorio de distribucion normal para simular la lgd de cada cliente
    
    for sim in tqdm(range(simulaciones)):
        lgd_sim[sim] = np.where(df['cd_garantia_grupo']==6,lgd_sim[sim],df['lgd_vl']) # reemplazo a los clientes sin garantia el lgd simulado y a los otros mantengo el lgd input
    
    vector_ead = df['ead_vl'].values
    results = np.zeros((simulaciones,len(df)))
    for sim in tqdm(range(simulaciones)):
        results[sim] = np.where(default[sim],lgd_sim[sim]*vector_ead,0)
        
    
    return results

result_sims = simula(df_simular,df_desvio_sectorial,sims_nu,lgd_sg,lgd_sg_std)

# Se define la funcion capital economico que calcula la perdida de cada simulacion y calcula el percentil 99.9%, 99.5% y 99%

def capital_economico(df):
    df = df.copy() # copio parametros como variables temporales de la funcion operativa
    
    perdidas = np.sum(df,axis = 1) # genero la suma de todas las perdidas por simulacion
    
    capitales = np.quantile(perdidas, (0.990, 0.995, 0.999)) # caculo los tres percentiles para el capital economico
    
    return perdidas, capitales # la devolucion son dos variable, una es el vector de perdidas donde cada elemento es la perdida de la simulacion y el otro es una lista con los 3 percentiles

perdidas, capitales = capital_economico(result_sims) # se corre la funcion capital_economico que calcula el capital economico
