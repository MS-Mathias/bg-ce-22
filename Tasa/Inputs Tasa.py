# -*- coding: utf-8 -*-
"""
Created on Thu May 26 16:27:45 2022

@author: agustin.ferrer
"""
import pandas as pd
import numpy as np

#df = pd.read_csv('Desktop/PruebaActivo')

import os

cwd = os.getcwd()

# Change the current working directory
os.chdir('C:/Users/agustin.ferrer/Documents/tasa')

# Print the current working directory
print("Current working directory: {0}".format(os.getcwd()))

##Importo los 4 archivos con las caidas:

ActivoPesos = pd.read_excel("Activo $.xls")
ActivoDolares = pd.read_excel("Activo U$S.xls")
PasivoPesos = pd.read_excel("Pasivo $.xls")
PasivoDolares = pd.read_excel("Pasivo U$S.xls")

##Funcion que filtra las tablas input Caidas

def TransformaPrimerColumna(df):
    df=df.rename(columns={'Unnamed: 0':'Tipo'})
    df['Tipo']=df['Tipo'].str.strip()
    return df
    
ActivoPesos = TransformaPrimerColumna(ActivoPesos)
ActivoDolares = TransformaPrimerColumna(ActivoDolares)
PasivoPesos = TransformaPrimerColumna(PasivoPesos)
PasivoDolares = TransformaPrimerColumna(PasivoDolares)
    
    
def FiltroRunOff(df):
    df2=df.loc[df['Tipo']=='Runoff Balance Book']
    for i in df2.index:
        if pd.isna(df.iloc[i-4]['Tipo'])!=True:
            df2.at[i,'Tipo']=df.iloc[i-4]['Tipo']
        else:
            df2.at[i,'Tipo']=df.iloc[i-3]['Tipo']
    return df2
            
ActivoPesos = FiltroRunOff(ActivoPesos)
ActivoDolares = FiltroRunOff(ActivoDolares)
PasivoPesos = FiltroRunOff(PasivoPesos)
PasivoDolares = FiltroRunOff(PasivoDolares)



    
###Control: No tiene que haber dentro de la tabla algunos de estos nombres."
ListaVariables = ['Caja','Exigencias Remuneradas','Beg Book Balance','BegBookRate','WART','Runoff Balance Book','Repricing Balance','Repricing Rate','Period Cap','Period Floor','Lifetime Cap','Lifetime Floor','Maturity Timing',np.NAN]

def Control1(DataFrame):
    for i in range()
    
    
    
    
    
#ActivoPesos['Tipo']
errores = []
for i in range(len(ListaVariables)):
    if ListaVariables[i] in ActivoPesos['Tipo'].values:
        errores.append(ListaVariables[i])
        continue
if len(errores) > 0:
    raise SystemExit(f'Control 1, no aprobada: {errores} se encuentra dentro de la tabla')
print ('Control 1 Aprobado')


        

