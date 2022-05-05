# Importo librerias 
import pandas as pd
import numpy as np
import matplotlib as plt

# Importo tabla (por ahora como no tengo la tabla original estoy generando una aleatoria :P)
# serieTasaBadlar = pd.read_csv("C:/Users/mathias.ezequiel.va1/")
serieTasaBadlar = [1.1]
for i in range(359):
    serieTasaBadlar.append(serieTasaBadlar[i]+np.random.normal(0,0.005))

# Extraigo la utlima tasa, la media de las tasas y el desvio estandar
tasaBadlar = serieTasaBadlar[-1]
stdBadlar = np.std(serieTasaBadlar)


tasaFutura = [tasaBadlar]
periodos = [0]
for i in range(3):
    periodos.append(periodos[i] + 30)
    shock = np.random.lognormal()
    tasaFutura.append(tasaFutura[i] * np.e ** ((-0.5 * (stdBadlar ** 2)) + stdBadlar * shock))

df = pd.DataFrame(tasaFutura, index=periodos, columns=["Tasa"])
df

df["Tasa"].plot(title="Tasas Simuladas",
                xticks=periodos,
                yticks=[1,1.2,1.4,1.6,1.8,2],
                xlabel="Dias",
                ylabel="Tasa",
                fontsize=9,
                )

help(df.plot)