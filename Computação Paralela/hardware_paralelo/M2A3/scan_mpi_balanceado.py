#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Crawler distribuído de arquivos com MPI (versão balanceada para aula)

Características:
- Rank 0 = scheduler centralizado
- Workers recebem LOTES de diretórios (batching)
- Descoberta dinâmica da árvore de diretórios
- Callback de progresso em uma única linha
- Agregação final:
    * total de arquivos
    * tamanho total
    * top N maiores arquivos
- Compatível com Windows e WSL
"""

from mpi4py import MPI
import os
import platform
import heapq
import time
from typing import List, Tuple, Dict, Any

import sys

TOP_N = 20
PROGRESS_EVERY = 100   # callback a cada N arquivos processados por worker
BATCH_SIZE = 12          # quantidade de diretórios por tarefa
USE_DFS = True          # True = pop() / False = pop(0)

TAG_WORK = 1
TAG_STOP = 2
TAG_RESULT = 3
TAG_PROGRESS = 4


def detect_root_path() -> str:
    if platform.system() == "Windows":
        return r"C:\\"
    return "/mnt/c"


ROOT_PATH = detect_root_path()


def human_size(num_bytes: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{num_bytes} B"


def normalize_dir(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def push_top_n(heap: List[Tuple[int, str]], item: Tuple[int, str], n: int) -> None:
    if len(heap) < n:
        heapq.heappush(heap, item)
    else:
        if item[0] > heap[0][0]:
            heapq.heapreplace(heap, item)


def merge_top_n(global_heap: List[Tuple[int, str]], local_items: List[Tuple[int, str]], n: int) -> None:
    for item in local_items:
        push_top_n(global_heap, item, n)


def pop_pending_dir(pending_dirs: List[str]) -> str:
    if USE_DFS:
        return pending_dirs.pop()
    return pending_dirs.pop(0)


def get_batch_dirs(pending_dirs: List[str], batch_size: int) -> List[str]:
    batch: List[str] = []
    while pending_dirs and len(batch) < batch_size:
        batch.append(pop_pending_dir(pending_dirs))
    return batch


def print_progress_line(rank_stats: Dict[int, Dict[str, Any]], workers: List[int], pending_count: int) -> None:
    parts = [f"pend={pending_count:>7}"]
    for w in workers:
        count = rank_stats[w]["files_processed_total"]
        parts.append(f"R{w:02d}:{count:>8}")
    line = "\r" + " | ".join(parts)
    print(line, end="", flush=True)


def send_progress(
    comm: MPI.Comm,
    rank: int,
    total_files_processed: int,
    total_bytes_processed: int
) -> None:
    msg = {
        "rank": rank,
        "total_files_processed": total_files_processed,
        "total_bytes_processed": total_bytes_processed,
        "timestamp": time.time(),
    }
    comm.send(msg, dest=0, tag=TAG_PROGRESS)


def scan_one_directory(
    path: str,
    comm: MPI.Comm,
    rank: int,
    worker_state: Dict[str, int],
    top_n: int = TOP_N,
    progress_every: int = PROGRESS_EVERY,
) -> Dict[str, Any]:
    subdirs: List[str] = []
    file_count = 0
    total_size = 0
    top_files: List[Tuple[int, str]] = []
    errors: List[str] = []

    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append(entry.path)

                    elif entry.is_file(follow_symlinks=False):
                        try:
                            size = entry.stat(follow_symlinks=False).st_size
                        except (PermissionError, FileNotFoundError, OSError) as e:
                            errors.append(f"STAT ERROR | {entry.path} | {repr(e)}")
                            continue

                        file_count += 1
                        total_size += size
                        push_top_n(top_files, (size, entry.path), top_n)

                        worker_state["files_processed_total"] += 1
                        worker_state["bytes_processed_total"] += size

                        if worker_state["files_processed_total"] % progress_every == 0:
                            send_progress(
                                comm=comm,
                                rank=rank,
                                total_files_processed=worker_state["files_processed_total"],
                                total_bytes_processed=worker_state["bytes_processed_total"],
                            )

                except (PermissionError, FileNotFoundError, OSError) as e:
                    entry_path = getattr(entry, "path", "<unknown>")
                    errors.append(f"ENTRY ERROR | {entry_path} | {repr(e)}")

    except (PermissionError, FileNotFoundError, OSError) as e:
        errors.append(f"DIR ERROR | {path} | {repr(e)}")
    except Exception as e:
        errors.append(f"UNEXPECTED ERROR | {path} | {repr(e)}")

    worker_state["dirs_processed_total"] += 1

    return {
        "path": path,
        "subdirs": subdirs,
        "file_count": file_count,
        "total_size": total_size,
        "top_files": top_files,
        "errors": errors,
    }


def worker_loop(comm: MPI.Comm, rank: int) -> None:
    worker_state = {
        "dirs_processed_total": 0,
        "files_processed_total": 0,
        "bytes_processed_total": 0,
    }

    while True:
        status = MPI.Status()
        msg = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()

        if tag == TAG_STOP:
            break

        if tag == TAG_WORK:
            directories = msg["dirs"]
            batch_results = []

            for directory in directories:
                result = scan_one_directory(
                    path=directory,
                    comm=comm,
                    rank=rank,
                    worker_state=worker_state,
                    top_n=TOP_N,
                    progress_every=PROGRESS_EVERY,
                )
                batch_results.append(result)

            comm.send(
                {
                    "batch_results": batch_results,
                    "worker_metrics": {
                        "dirs_processed_total": worker_state["dirs_processed_total"],
                        "files_processed_total": worker_state["files_processed_total"],
                        "bytes_processed_total": worker_state["bytes_processed_total"],
                    },
                },
                dest=0,
                tag=TAG_RESULT,
            )


def master_loop(comm: MPI.Comm, world_size: int, root_path: str) -> None:
    if world_size < 2:
        raise RuntimeError("Execute com pelo menos 2 processos: 1 master + 1 worker.")

    if not os.path.exists(root_path):
        raise FileNotFoundError(f"Raiz não encontrada: {root_path}")

    workers = list(range(1, world_size))
    idle_workers = workers.copy()
    active_workers = set()

    pending_dirs: List[str] = [root_path]
    seen_dirs = {normalize_dir(root_path)}

    total_files = 0
    total_size = 0
    global_top_files: List[Tuple[int, str]] = []
    all_errors: List[str] = []

    rank_stats = {
        w: {
            "dirs_processed_total": 0,
            "files_processed_total": 0,
            "bytes_processed_total": 0,
            "last_progress_ts": None,
        }
        for w in workers
    }

    counters = {
        "dirs_dispatched": 0,
        "results_received": 0,
        "progress_messages_received": 0,
    }

    t0 = time.perf_counter()

    def dispatch_available_work() -> None:
        while pending_dirs and idle_workers:
            worker = idle_workers.pop(0)
            batch = get_batch_dirs(pending_dirs, BATCH_SIZE)
            if not batch:
                idle_workers.insert(0, worker)
                break

            comm.send({"dirs": batch}, dest=worker, tag=TAG_WORK)
            active_workers.add(worker)
            counters["dirs_dispatched"] += len(batch)

    dispatch_available_work()
    print_progress_line(rank_stats, workers, len(pending_dirs))

    while active_workers or pending_dirs:
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        worker = status.Get_source()
        tag = status.Get_tag()

        if tag == TAG_PROGRESS:
            counters["progress_messages_received"] += 1
            rank_stats[worker]["files_processed_total"] = msg["total_files_processed"]
            rank_stats[worker]["bytes_processed_total"] = msg["total_bytes_processed"]
            rank_stats[worker]["last_progress_ts"] = msg["timestamp"]
            print_progress_line(rank_stats, workers, len(pending_dirs))

        elif tag == TAG_RESULT:
            counters["results_received"] += 1

            payload = msg
            batch_results = payload["batch_results"]
            wm = payload["worker_metrics"]

            for result in batch_results:
                total_files += result["file_count"]
                total_size += result["total_size"]
                merge_top_n(global_top_files, result["top_files"], TOP_N)
                all_errors.extend(result["errors"])

                for subdir in result["subdirs"]:
                    try:
                        norm = normalize_dir(subdir)
                        if norm not in seen_dirs:
                            seen_dirs.add(norm)
                            pending_dirs.append(subdir)
                    except Exception as e:
                        all_errors.append(f"NORM ERROR | {subdir} | {repr(e)}")

            rank_stats[worker]["dirs_processed_total"] = wm["dirs_processed_total"]
            rank_stats[worker]["files_processed_total"] = wm["files_processed_total"]
            rank_stats[worker]["bytes_processed_total"] = wm["bytes_processed_total"]

            if worker in active_workers:
                active_workers.remove(worker)
            idle_workers.append(worker)

            dispatch_available_work()
            print_progress_line(rank_stats, workers, len(pending_dirs))

    for w in workers:
        comm.send(None, dest=w, tag=TAG_STOP)

    elapsed = time.perf_counter() - t0
    print()

    top_sorted = sorted(global_top_files, key=lambda x: x[0], reverse=True)

    print("=" * 100)
    print("RESULTADO FINAL")
    print("=" * 100)
    print(f"Raiz analisada              : {root_path}")
    print(f"Total de arquivos           : {total_files}")
    print(f"Tamanho total               : {total_size} bytes ({human_size(total_size)})")
    print(f"Diretórios descobertos      : {len(seen_dirs)}")
    print(f"Diretórios despachados      : {counters['dirs_dispatched']}")
    print(f"Resultados recebidos        : {counters['results_received']}")
    print(f"Mensagens de progresso      : {counters['progress_messages_received']}")
    print(f"Tempo total                 : {elapsed:.2f} s")
    print(f"BATCH_SIZE                  : {BATCH_SIZE}")
    print(f"PROGRESS_EVERY              : {PROGRESS_EVERY}")
    print(f"USE_DFS                     : {USE_DFS}")

    print("\n" + "-" * 100)
    print("CARGA POR RANK")
    print("-" * 100)
    for w in workers:
        rs = rank_stats[w]
        print(
            f"Rank {w:02d} | "
            f"dirs={rs['dirs_processed_total']:>7} | "
            f"files={rs['files_processed_total']:>10} | "
            f"bytes={human_size(rs['bytes_processed_total']):>10}"
        )

    files_by_rank = [rank_stats[w]["files_processed_total"] for w in workers]
    max_files = max(files_by_rank) if files_by_rank else 0
    min_files = min(files_by_rank) if files_by_rank else 0
    imbalance = (max_files / min_files) if min_files > 0 else float("inf")

    print("\n" + "-" * 100)
    print("ANÁLISE DIDÁTICA")
    print("-" * 100)
    print(f"Maior carga (arquivos)      : {max_files}")
    print(f"Menor carga (arquivos)      : {min_files}")
    if imbalance == float("inf"):
        print("Fator de desbalanceamento   : infinito (algum rank processou 0 arquivos)")
    else:
        print(f"Fator de desbalanceamento   : {imbalance:.2f}x")

    print("\n" + "-" * 100)
    print(f"TOP {TOP_N} MAIORES ARQUIVOS")
    print("-" * 100)
    for i, (size_bytes, path) in enumerate(top_sorted, start=1):
        print(f"{i:02d}. {human_size(size_bytes):>10} | {size_bytes:>15} bytes | {path}")

    print("\n" + "-" * 100)
    print("ERROS / AVISOS")
    print("-" * 100)
    print(f"Total: {len(all_errors)}")
    for err in all_errors[:50]:
        print(err)
    if len(all_errors) > 50:
        print(f"... ({len(all_errors) - 50} erros adicionais omitidos)")

    report_path = "relatorio_scan_mpi_balanceado.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("RESULTADO FINAL\n")
        f.write("=" * 100 + "\n")
        f.write(f"Raiz analisada              : {root_path}\n")
        f.write(f"Total de arquivos           : {total_files}\n")
        f.write(f"Tamanho total               : {total_size} bytes ({human_size(total_size)})\n")
        f.write(f"Diretórios descobertos      : {len(seen_dirs)}\n")
        f.write(f"Diretórios despachados      : {counters['dirs_dispatched']}\n")
        f.write(f"Resultados recebidos        : {counters['results_received']}\n")
        f.write(f"Mensagens de progresso      : {counters['progress_messages_received']}\n")
        f.write(f"Tempo total                 : {elapsed:.2f} s\n")
        f.write(f"BATCH_SIZE                  : {BATCH_SIZE}\n")
        f.write(f"PROGRESS_EVERY              : {PROGRESS_EVERY}\n")
        f.write(f"USE_DFS                     : {USE_DFS}\n\n")

        f.write("CARGA POR RANK\n")
        f.write("-" * 100 + "\n")
        for w in workers:
            rs = rank_stats[w]
            f.write(
                f"Rank {w:02d} | "
                f"dirs={rs['dirs_processed_total']:>7} | "
                f"files={rs['files_processed_total']:>10} | "
                f"bytes={human_size(rs['bytes_processed_total']):>10}\n"
            )

        f.write("\nANÁLISE DIDÁTICA\n")
        f.write("-" * 100 + "\n")
        f.write(f"Maior carga (arquivos)      : {max_files}\n")
        f.write(f"Menor carga (arquivos)      : {min_files}\n")
        if imbalance == float("inf"):
            f.write("Fator de desbalanceamento   : infinito (algum rank processou 0 arquivos)\n")
        else:
            f.write(f"Fator de desbalanceamento   : {imbalance:.2f}x\n")

        f.write(f"\nTOP {TOP_N} MAIORES ARQUIVOS\n")
        f.write("-" * 100 + "\n")
        for i, (size_bytes, path) in enumerate(top_sorted, start=1):
            f.write(f"{i:02d}. {human_size(size_bytes):>10} | {size_bytes:>15} bytes | {path}\n")

        f.write("\nERROS / AVISOS\n")
        f.write("-" * 100 + "\n")
        f.write(f"Total: {len(all_errors)}\n")
        for err in all_errors:
            f.write(err + "\n")

    print(f"\nRelatório salvo em: {report_path}")


def main() -> None:
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    world_size = comm.Get_size()

    try:
        if rank == 0:
            master_loop(comm, world_size, ROOT_PATH)
        else:
            worker_loop(comm, rank)
    except Exception as e:
        print(f"[Rank {rank}] Erro fatal: {repr(e)}")


if __name__ == "__main__":
    main()
