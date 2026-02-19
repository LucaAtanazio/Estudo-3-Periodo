from concurrent.futures import ProcessPoolExecutor

LIMIAR = 1000

def soma_fork_join(v, ini, fim):
    # Caso base
    if fim - ini <= LIMIAR:
        return sum(v[ini:fim])

    meio = (ini + fim) // 2

    # Divide (fork)
    futuro_esq = executor.submit(soma_fork_join, v, ini, meio)
    futuro_dir = executor.submit(soma_fork_join, v, meio, fim)

    # Junta (join)
    return futuro_esq.result() + futuro_dir.result()


if __name__ == "__main__":
    v = list(range(1, 10001))

    with ProcessPoolExecutor() as executor:
        resultado = soma_fork_join(v, 0, len(v))

    print(resultado)
