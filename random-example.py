import multiprocessing as mp
import random
import time
import re

from collections import defaultdict
from datetime import datetime

TIMEOUT = 5


def generator(name: str, out: mp.Queue) -> None:
    """
    Logs generator main function
    :param name: logger name
    :param out: queue to write logs in
    :return: None
    """
    values = list(range(10))
    rng = random.Random()
    while True:
        out.put_nowait(f'{name}:{rng.choice(values)}')
        time.sleep(0.1)


def collector(in_q: mp.Queue, out: mp.Queue) -> None:
    """
    Logs collector main function
    :param in_q: queue with input logs
    :param out: queue with output logs
    :return: None
    """
    log_structure = re.compile(r'\w+:\d+')
    while True:
        s = in_q.get()
        if log_structure.match(s) is None:
            print('Bad log')
            continue

        name, value = s.split(':')
        out.put((name, int(value)))


def aggregator(in_q: mp.Queue, out: mp.Queue) -> None:
    """
    Aggregator main function
    :param in_q: queue with input messages
    :param out: queue to write results in
    :return: None
    """
    memory = defaultdict(list)
    while True:
        name, value = in_q.get()
        prev_data = memory[name]
        prev_data.append(value)

        if len(prev_data) > 3:
            prev_data = prev_data[-3:]

        if len(prev_data) == 3:
            out.put((name, prev_data))


def resolver(in_q: mp.Queue) -> None:
    """
    Resolver main function
    :param in_q: queue with input messages
    :return: None
    """
    while True:
        name, history = in_q.get()
        if len(history) != 3:
            print('Got bad message')
            continue

        if history[0] == history[1] == history[2]:
            print(f'{name} is broken, received: {history}')


def main() -> None:
    q1, q2, q3 = mp.Queue(), mp.Queue(), mp.Queue()
    processes = [
        mp.Process(target=collector, args=(q1, q2)),
        mp.Process(target=aggregator, args=(q2, q3)),
        mp.Process(target=resolver, args=(q3,))
    ]
    generators = [
        mp.Process(target=generator, args=(f'p{i}', q1))
        for i in range(10)
    ]
    for g in generators:
        g.start()

    for p in processes:
        p.start()

    time.sleep(10)

    for g in generators:
        g.kill()
    for p in processes:
        p.kill()


if __name__ == '__main__':
    main()
