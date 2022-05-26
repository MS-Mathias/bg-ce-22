# Importo librerias 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

###############################################

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

###############################################

BADLAR = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/BADLAR.xlsx")
tasas_input = BADLAR['Valor'].values.tolist()
for i in range(len(tasas_input)):
    tasas_input[i] = tasas_input[i]/100

params = VasicekCalib(tasas_input)
lamda = params[0]
media = params[1]
sigma = params[2]
r0 = params[3]

years = 1
N = years * 252
t = np.arange(0,N)/252

test_sim = VasicekSim(N, r0, lamda, media, sigma, 1/252)
plt.plot(t,test_sim)
plt.show()

###############################################

M = 25
tasas_arr = VasicekMultiSim(M, N, r0, lamda*3, media, sigma)

plt.plot(t,tasas_arr)
plt.hlines(y=media, xmin = -100, xmax=100, zorder=10, 
           linestyles = 'dashed', label='Media')
plt.annotate('Media', xy=(1.0, media+0.0005))
plt.xlim(-0.05, 1.05)
plt.ylabel("Tasa BADLAR")
plt.xlabel("Tiempo (años)")
plt.title("Simulación de Tasas de Interés")
plt.show()

###############################################

M = 25
tasas_arr = VasicekMultiSim(M, N, r0, lamda, media, 0.42)
plt.plot(t,tasas_arr)
plt.hlines(y=media, xmin = -100, xmax=100, zorder=10, 
           linestyles = 'dashed', label='Media')
plt.annotate('Media', xy=(1.0, media+0.0005))
plt.xlim(-0.05, 1.05)
plt.ylabel("Tasa BADLAR")
plt.xlabel("Tiempo (años)")
plt.title("Simulacicón de Tasas con Volatilidad 42%")
plt.show()

###############################################

#Importo Activos
Activos = pd.read_excel("C:/Users/mathias.ezequiel.va1/Desktop/Activos.xlsx")

#Agrego columna "Dias" y elimino columnas "vencimiento" e "identificador"
Activos["Dias"] = (Activos["Vencimiento"] * 252) // 1
Activos.drop(['Vencimiento','Identificador'],axis=1,inplace=True)

#Simulo tasas
tasas_arr = VasicekMultiSim(M, N, r0, lamda, media, sigma)

#Convierto las tasas anuales a diarias utilizando 252 ruedas
for columna in range(M):
    for fila in range(252):
        tasas_arr[fila][columna] = ((1 + tasas_arr[fila][columna]) ** (1 / 252)) - 1

#Covierto tasas individuales en tasas acumuladas
for columna in range(M):
    for fila in range(252):
        if fila == 0:
            continue
        else:
            tasas_arr[fila][columna] = ((1 + tasas_arr[fila-1][columna]) * 
            (1 + tasas_arr[fila][columna])) - 1

#Genero df de las tasas simuladas y le agrego dias
Tasas = pd.DataFrame(tasas_arr)
Tasas["Dias"]=np.arange(1,len(Tasas)+1)

#Hago un left join de las tasas a la tabla de activos con dias como union
ActivosSim = pd.merge(Activos.groupby(["Dias"]).sum(),
                       Tasas,
                       how="left",
                       on="Dias")

#Actualizo montos con las tasas
for i in range(M):
    ActivosSim.iloc[:,i+2]=ActivosSim["Monto"]/(1+ActivosSim.iloc[:,i+2])

