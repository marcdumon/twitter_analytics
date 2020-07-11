# --------------------------------------------------------------------------------------------------------
# 2020/07/11
# src - xxx.py
# md
# --------------------------------------------------------------------------------------------------------

from queue import PriorityQueue
from multiprocessing.managers import SyncManager
from multiprocessing import Process

X = 1
print(f'0) X= {X}')


def f1():
    global X
    print(f'1) X= {X}')
    X = 2


print(f'2) X= {X}')


def f2():
    global X
    print(f'3) X= {X}')
    X = 3


print(f'4) X= {X}')

if __name__ == '__main__':
    f1()
    f2()
