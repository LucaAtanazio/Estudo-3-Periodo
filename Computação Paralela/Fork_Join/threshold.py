from concurrent.futures import ProcessPoolExecutor

THRESHOLD = 2  # número mínimo de linhas para paralelizar

def soma_matriz_fork_join(A, B, ini, fim):
    # Caso base: problema pequeno → sequencial
    if fim - ini <= THRESHOLD:
        return [
            [a + b for a, b in zip(A[i], B[i])]
            for i in range(ini, fim)
        ]

    # Divide o problema
    meio = (ini + fim) // 2

    # Fork
    futuro_esq = executor.submit(soma_matriz_fork_join, A, B, ini, meio)
    futuro_dir = executor.submit(soma_matriz_fork_join, A, B, meio, fim)

    # Join
    return futuro_esq.result() + futuro_dir.result()


if __name__ == "__main__":
    A = [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
        [10, 11, 12]
    ]

    B = [
        [10, 20, 30],
        [40, 50, 60],
        [70, 80, 90],
        [100, 110, 120]
    ]

    with ProcessPoolExecutor() as executor:
        C = soma_matriz_fork_join(A, B, 0, len(A))

    print(C)
