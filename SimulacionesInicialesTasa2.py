# Importo librerias 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

###############################################

#Defino Funciones para el script
def VasicekCalib(tasas, dt=1/252):
    n = len(tasas)
    
    # Implementa maxima verosimilitud para estimar parametros
    Sx = sum(tasas[0:(n-1)])
    Sy = sum(tasas[1:n])
    Sxx = np.dot(tasas[0:(n-1)], tasas[0:(n-1)])
    Sxy = np.dot(tasas[0:(n-1)], tasas[1:n])
    Syy = np.dot(tasas[1:n], tasas[1:n])
    
    media = (Sy * Sxx - Sx * Sxy) / (n * (Sxx - Sxy) - (Sx**2 - Sx*Sy))
    lamda = -np.log((Sxy - media * Sx - media * Sy + n * media**2) / (Sxx - 2*media*Sx + n*media**2)) / dt
    a = np.exp(-lamda * dt)
    sigmah2 = (Syy - 2*a*Sxy + a**2 * Sxx - 2*media*(1-a)*(Sy - a*Sx) + n*media**2 * (1-a)**2) / n
    sigma = np.sqrt(sigmah2*2*lamda / (1-a**2))
    if sigma < 0.42:
        sigma = 0.42
    r0 = tasas[n-1]
    
    return [lamda, media, sigma, r0]

def VasicekSiguienteTasa(r, lamda, media, sigma, dt=1/252):
    # Utiliza parametros de VasicekCalib para estimar la proxima tasa    
    val1 = np.exp(-1*lamda*dt)
    val2 = (sigma**2)*(1-val1**2) / (2*lamda)
    out = r*val1 + media*(1-val1) + (np.sqrt(val2))*np.random.normal()
    return out

def VasicekSim(N, r0, lamda, media, sigma, dt = 1/252):
    tasa_r = [0]*N # Genero vector para almacenar las tasas   
    tasa_r[0] = r0 # Reemplazo primer valor por la ultima tasa disponible    
    
    for i in range(1,N):
        tasa_r[i] = VasicekSiguienteTasa(tasa_r[i-1], lamda, media, sigma, dt)
    
    return tasa_r

def VasicekMultiSim(M, N, r0, lamda, media, sigma, dt = 1/252):
    sim_arr = np.ndarray((N, M))
    
    for i in range(0,M):
        sim_arr[:, i] = VasicekSim(N, r0, lamda, media, sigma, dt)
    
    return sim_arr

def AnualADiario(Sim):
    SimTasasDiarias = []
    for i in range(len(Sim)):
        SimTasasDiarias.append((1 + Sim[i]) ** (1 / 360) - 1)
        if (5 - (i % 5) == 1):
            SimTasasDiarias.append(SimTasasDiarias[-1])
            SimTasasDiarias.append(SimTasasDiarias[-1])
    return SimTasasDiarias

def AcumulaTasas(Sim):
    SimTasasAcumuladas = [Sim[0]]
    for i in range(1,len(Sim)):
        SimTasasAcumuladas.append(((1+SimTasasAcumuladas[i-1]) * (1+Sim[i]))-1)
    return SimTasasAcumuladas

def TratamientoTasas(TasasSim):
    TasasDiarias = []
    for Simulacion in TasasSim:
        TasasDiarias.append(AnualADiario(Simulacion))
    del Simulacion
    TasasDiarias = np.array(TasasDiarias)
    
    TasasAcum = []
    for Simulacion in TasasDiarias:
        TasasAcum.append(AcumulaTasas(Simulacion))
    del Simulacion
    TasasAcum = np.array(TasasAcum)
    return TasasAcum

###############################################

#Importo las tasas esto es temporal hasta que tengamos los inputs oficiales
BADLAR = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/BADLAR.xlsx")
tasas_input = BADLAR['Valor'].values.tolist()
for i in range(len(tasas_input)):
    tasas_input[i] = tasas_input[i]/100
del i, BADLAR
    
###############################################

#Calibro los parametros de las simulaciones
params = VasicekCalib(tasas_input)
lamda = params[0]
media = params[1]
sigma = params[2]
r0 = params[3]
del params
N = 252

###############################################

#Simulo tasas
M = 1000
TasasSim = np.transpose(VasicekMultiSim(M, N, r0, lamda, media, sigma))
TasasSim = TratamientoTasas(TasasSim)

#Importo Activos
Activos = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Activos.xlsx")

#Agrego columna "Dias" y elimino columnas "vencimiento" e "identificador"
Activos["Dias"] = (Activos["Vencimiento"] * 252) // 1
Activos.drop(['Vencimiento','Identificador'],axis=1,inplace=True)


#Hago un left join de las tasas a la tabla de activos con dias como union
ActivosSim = pd.merge(Activos.groupby(["Dias"]).sum(),
                       Tasas,
                       how="left",
                       on="Dias")

#Actualizo montos con las tasas
for i in range(M):
    ActivosSim.iloc[:,i+2]=ActivosSim["Monto"]/(1+ActivosSim.iloc[:,i+2])

