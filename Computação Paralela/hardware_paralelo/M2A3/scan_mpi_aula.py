#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mpi4py import MPI
import os
import platform
import heapq
import time
from typing import List, Tuple, Dict, Any

TOP_N = 20
PROGRESS_EVERY = 1000

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


def push_top_n(heap: List[Tuple[int, str]], item: Tuple[int, str], n: int) -> None:
    if len(heap) < n:
        heapq.heappush(heap, item)
    else:
        if item[0] > heap[0][0]:
            heapq.heapreplace(heap, item)


def merge_top_n(global_heap: List[Tuple[int, str]], local_items: List[Tuple[int, str]], n: int) -> None:
    for item in local_items:
        push_top_n(global_heap, item, n)


def normalize_dir(path: str) -> str:
    return os.path.normcase(os.path.abspath(path))


def print_progress_line(rank_stats, workers):
    """
    Mostra progresso dos workers em uma única linha.
    Cada campo tem largura fixa para evitar deslocamento visual.
    """
    parts = []
    
    for w in workers:
        count = rank_stats[w]["files_processed_total"]
        parts.append(f"R{w:02d}:{count:>9}")

    line = "\r" + " | ".join(parts)

    print(line, end="", flush=True)

def send_progress(
    comm: MPI.Comm,
    rank: int,
    current_dir: str,
    dir_files_processed: int,
    total_files_processed: int,
    total_bytes_processed: int
) -> None:
    msg = {
        "rank": rank,
        "current_dir": current_dir,
        "dir_files_processed": dir_files_processed,
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
                                current_dir=path,
                                dir_files_processed=file_count,
                                total_files_processed=worker_state["files_processed_total"],
                                total_bytes_processed=worker_state["bytes_processed_total"],
                            )

                except (PermissionError, FileNotFoundError, OSError) as e:
                    errors.append(f"ENTRY ERROR | {entry.path} | {repr(e)}")

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
        "worker_metrics": {
            "dirs_processed_total": worker_state["dirs_processed_total"],
            "files_processed_total": worker_state["files_processed_total"],
            "bytes_processed_total": worker_state["bytes_processed_total"],
        },
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
            directory = msg["path"]
            result = scan_one_directory(
                path=directory,
                comm=comm,
                rank=rank,
                worker_state=worker_state,
                top_n=TOP_N,
                progress_every=PROGRESS_EVERY,
            )
            comm.send(result, dest=0, tag=TAG_RESULT)


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
            "current_dir": None,
        }
        for w in workers
    }

    dirs_dispatched = 0
    results_received = 0
    progress_messages_received = 0

    t0 = time.perf_counter()

    def dispatch_available_work() -> None:
        nonlocal dirs_dispatched
        while pending_dirs and idle_workers:
            worker = idle_workers.pop(0)
            directory = pending_dirs.pop(0)
            comm.send({"path": directory}, dest=worker, tag=TAG_WORK)
            active_workers.add(worker)
            dirs_dispatched += 1
            rank_stats[worker]["current_dir"] = directory

    # despacho inicial
    dispatch_available_work()
    print_progress_line(rank_stats, workers)

    while active_workers or pending_dirs:
        status = MPI.Status()
        msg = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        worker = status.Get_source()
        tag = status.Get_tag()

        if tag == TAG_PROGRESS:
            progress_messages_received += 1
            rank_stats[worker]["files_processed_total"] = msg["total_files_processed"]
            rank_stats[worker]["bytes_processed_total"] = msg["total_bytes_processed"]
            rank_stats[worker]["last_progress_ts"] = msg["timestamp"]
            rank_stats[worker]["current_dir"] = msg["current_dir"]

            print_progress_line(rank_stats, workers)

        elif tag == TAG_RESULT:
            results_received += 1
            result = msg

            total_files += result["file_count"]
            total_size += result["total_size"]
            merge_top_n(global_top_files, result["top_files"], TOP_N)
            all_errors.extend(result["errors"])

            wm = result["worker_metrics"]
            rank_stats[worker]["dirs_processed_total"] = wm["dirs_processed_total"]
            rank_stats[worker]["files_processed_total"] = wm["files_processed_total"]
            rank_stats[worker]["bytes_processed_total"] = wm["bytes_processed_total"]
            rank_stats[worker]["current_dir"] = None

            # worker terminou e volta para fila de ociosos
            if worker in active_workers:
                active_workers.remove(worker)
            idle_workers.append(worker)

            # novos subdiretórios descobertos
            for subdir in result["subdirs"]:
                try:
                    norm = normalize_dir(subdir)
                    if norm not in seen_dirs:
                        seen_dirs.add(norm)
                        pending_dirs.append(subdir)
                except Exception as e:
                    all_errors.append(f"NORM ERROR | {subdir} | {repr(e)}")

            # agora distribui para TODOS os workers ociosos disponíveis
            dispatch_available_work()
            print_progress_line(rank_stats, workers)

    # encerra workers
    for w in workers:
        comm.send(None, dest=w, tag=TAG_STOP)

    elapsed = time.perf_counter() - t0

    print()  # quebra a linha do progresso

    top_sorted = sorted(global_top_files, key=lambda x: x[0], reverse=True)

    print("=" * 100)
    print("RESULTADO FINAL")
    print("=" * 100)
    print(f"Raiz analisada              : {root_path}")
    print(f"Total de arquivos           : {total_files}")
    print(f"Tamanho total               : {total_size} bytes ({human_size(total_size)})")
    print(f"Diretórios descobertos      : {len(seen_dirs)}")
    print(f"Diretórios despachados      : {dirs_dispatched}")
    print(f"Resultados recebidos        : {results_received}")
    print(f"Mensagens de progresso      : {progress_messages_received}")
    print(f"Tempo total                 : {elapsed:.2f} s")

    print("\n" + "-" * 100)
    print("CARGA POR RANK")
    print("-" * 100)
    for w in workers:
        rs = rank_stats[w]
        print(
            f"Rank {w:02d} | "
            f"dirs={rs['dirs_processed_total']:>6} | "
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