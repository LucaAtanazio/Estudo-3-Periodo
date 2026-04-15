#include <iostream>
#include <thread>
#include <vector>
#include <chrono> // Para o sleep
#include <random> // Para o atraso aleatório

int counter = 0; // Variável global compartilhada (sem proteção)

void inc(int id, int iteracoes) {
    // Gerador de números aleatórios para o sleep
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(1, 3);

    for (int i = 0; i < iteracoes; i++) {
        // Simula processamento real com um sleep aleatório
        int ms = dis(gen);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));

        // REGIÃO CRÍTICA (Desprotegida)
        // 1. Lê o valor de counter
        // 2. Incrementa em um registrador
        // 3. Escreve de volta na memória
        counter++;
    }
    std::cout << "Thread " << id << " finalizou.\n";
}

int main() {
    const int num_threads = 2;
    const int iteracoes_por_thread = 100; // Valor baixo para o sleep não demorar muito
    const int esperado = num_threads * iteracoes_por_thread;

    std::vector<std::thread> threads;

    std::cout << "Iniciando threads... O resultado esperado e: " << esperado << "\n";

    // Criação das threads
    for (int i = 0; i < num_threads; i++) {
        threads.push_back(std::thread(inc, i, iteracoes_por_thread));
    }

    // Aguarda todas terminarem
    for (auto& t : threads) {
        t.join();
    }

    std::cout << "------------------------------------------\n";
    std::cout << "VALOR FINAL DO CONTADOR: " << counter << "\n";
    std::cout << "DIFERENCA (PERDA): " << (esperado - counter) << "\n";
    std::cout << "------------------------------------------\n";

    return 0;
}