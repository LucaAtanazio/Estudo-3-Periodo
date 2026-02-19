from concurrent.futures import ProcessPoolExecutor

def soma_linha(args):
    a, b = args
    return [x + y for x, y in zip(a, b)]

if __name__ == "__main__":
    a = [
        [1, 2, 3],
        [4, 5, 6]
    ]
    
    b = [
        [9, 8, 7],
        [5, 2, 6]
    ]
    
    with ProcessPoolExecutor() as executor:
        c = list(executor.map(soma_linha, zip(a, b)))
        
    print(c)
