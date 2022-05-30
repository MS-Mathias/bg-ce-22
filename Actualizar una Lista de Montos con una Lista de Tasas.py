def ActualizacionMontos(ListaMontos, ListaTasas):
    MontosActualizados = []
    for i in range(len(ListaMontos)):
        MontosActualizados.append(ListaMontos[i] * ListaTasas[i])
    return MontosActualizados