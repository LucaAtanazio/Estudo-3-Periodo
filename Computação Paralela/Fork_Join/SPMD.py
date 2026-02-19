from concurrent.futures import ProcessPoolExecutor
import os

THRESHOLD = 10_000

def soma_bloco(v, ini, fim):
    """Mesmo código executado por todos os processos (SPMD)"""
    return sum(v[ini:fim])


def soma_spmd(v):
    n = len(v)
    n_procs = os.cpu_count()

    # Tamanho do bloco respeitando o threshold
    bloco = max(THRESHOLD, n // n_procs)

    tarefas = []
    for i in range(0, n, bloco):
        tarefas.append((i, min(i + bloco, n)))

    with ProcessPoolExecutor() as executor:
        parciais = executor.map(
            lambda p: soma_bloco(v, p[0], p[1]),
            tarefas
        )

    # Reduce
    return sum(parciais)


if __name__ == "__main__":
    v = list(range(1, 1_000_001))
    resultado = soma_spmd(v)
    print(resultado)
