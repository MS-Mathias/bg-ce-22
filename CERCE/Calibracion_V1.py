import pandas as pd
import numpy as np
import statsmodels.formula.api as smf

df = pd.read_excel('Calibracion PD.xlsx')

#Paso las features de numpy a Pandas

df_X = pd.DataFrame(df, columns = ['score'])

# Agrego la variable dependiente

df_X["Y"] = df['entra en mora en los proximos 12 meses']

# Ejecuto la regresión logistica

logit_modelo = smf.logit('Y ~ score', data = df_X).fit()

# Si le pongo + 0 entonces el modelo no tiene intercepto

# Me dan los betas (incluido el intercepto)

params = logit_modelo.params

print(logit_modelo.summary())

df_X['X_modelo2'] = params.loc['Intercept'] + df_X['score'] * params.loc['score']

# Ejecuto la regresión logistica

logit_modelo = smf.logit('Y ~ X_modelo2', data = df_X).fit()

# Si le pongo + 0 entonces el modelo no tiene intercepto

# Me dan los betas (incluido el intercepto)

params = logit_modelo.params

# Miro los principales resultados: pseudo R cuadrado, cual es la
# variable dependiente, que método se usó, betas, p value
# y la cantidad de observaciones

print(logit_modelo.summary())

# Calculo la predicción del modelo (me da el score)

df_X['prediccion'] = logit_modelo.predict(df_X)

