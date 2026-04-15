#include <iostream>
#include <thread>
#include <vector>
#include <semaphore>
#include <queue>
#include <chrono>
#include <string>
#include <conio.h> 

const int BUFFER_SIZE = 10;
std::queue<int> buffer;

// Semáforos conforme sua modelagem
std::counting_semaphore<BUFFER_SIZE> sem_empty(BUFFER_SIZE);
std::counting_semaphore<BUFFER_SIZE> sem_full(0);
std::binary_semaphore sem_mutex(1);

bool running = true;

// Callback: chamado SEMPRE de dentro de uma região protegida por mutex
void update_ui(std::string acao, int id) {
    std::string visual = "[";
    int temp_size = (int)buffer.size();
    for (int i = 0; i < BUFFER_SIZE; i++) {
        visual += (i < temp_size) ? "X" : " ";
    }
    visual += "] Conteudo: " + std::to_string(temp_size) + "/" + std::to_string(BUFFER_SIZE);

    // Mostra quem agiu e o estado atual
    std::cout << "\r" << visual << " | Ultimo: " << acao << id << " | 'Q' para sair" << std::flush;
}

void produtor(int id) {
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1500 + 500));

        sem_empty.acquire(); // wait(empty)
        sem_mutex.acquire(); // wait(mutex)

        // Seção Crítica
        buffer.push(1);
        update_ui("P", id);

        sem_mutex.release(); // signal(mutex)
        sem_full.release();  // signal(full)
    }
}

void consumidor(int id) {
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 1600 + 1000));

        sem_full.acquire();  // wait(full)
        sem_mutex.acquire(); // wait(mutex)

        // Seção Crítica
        if (!buffer.empty()) {
            buffer.pop();
            update_ui("C", id);
        }

        sem_mutex.release(); // signal(mutex)
        sem_empty.release(); // signal(empty)
    }
}

int main() {
    std::cout << "Simulacao Produtor-Consumidor (Dijkstra)\n";
    std::cout << "Semaforos: empty=" << BUFFER_SIZE << ", full=0, mutex=1\n\n";

    std::vector<std::thread> workers;
    for (int i = 0; i < 2; i++) workers.emplace_back(produtor, i);
    for (int i = 0; i < 2; i++) workers.emplace_back(consumidor, i);

    while (running) {
        if (_kbhit()) {
            char c = _getch();
            if (c == 'q' || c == 'Q') running = false;
        }
    }

    std::cout << "\n\nFinalizando... Todos os semaforos liberados." << std::endl;
    exit(0);
}