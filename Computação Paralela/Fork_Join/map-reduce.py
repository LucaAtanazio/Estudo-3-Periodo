from concurrent.futures import ProcessPoolExecutor
from collections import defaultdict
import random

THRESHOLD = 10_000

def agrega_sequencial(dados, ini, fim):
    parcial = defaultdict(int)
    for user_id, valor in dados[ini:fim]:
        parcial[user_id] += valor
    return parcial


def merge_dicts(a, b):
    for k, v in b.items():
        a[k] += v
    return a


def map_reduce_fork_join(dados):
    blocos = [
        (i, min(i + THRESHOLD, len(dados)))
        for i in range(0, len(dados), THRESHOLD)
    ]

    with ProcessPoolExecutor() as executor:
        parciais = executor.map(
            lambda p: agrega_sequencial(dados, p[0], p[1]),
            blocos
        )

    resultado = defaultdict(int)
    for parcial in parciais:
        merge_dicts(resultado, parcial)

    return resultado


if __name__ == "__main__":
    N = 100_000
    dados = [(random.randint(1, 1000), random.randint(1, 10)) for _ in range(N)]

    resultado = map_reduce_fork_join(dados)
    print(dict(list(resultado.items())[:5]))
