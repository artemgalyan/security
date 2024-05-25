import multiprocessing as mp
import typing as tp


TIMEOUT = 5


def aggregator(in_q: mp.Queue, out: mp.Queue) -> None:
    while True:
        message = in_q.get(block=True, timeout=TIMEOUT)
        print(f'Collector: {message}')
        out.put(message)


def resolver(in_q: mp.Queue) -> None:
    while True:
        message = in_q.get(block=True, timeout=TIMEOUT)
        print('X' if message.startswith('a') else 'Y')


def main() -> None:
    q1, q2 = mp.Queue(), mp.Queue()
    processes = [
        mp.Process(target=aggregator, args=(q1, q2)),
        mp.Process(target=resolver, args=(q2,))
    ]
    for p in processes:
        p.start()

    while (s := input('Input: ')) != 'X':
        q1.put(s)

    for p in processes:
        p.join()


if __name__ == '__main__':
    main()
