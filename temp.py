import numpy as np

success = False
fail_counter = 0
while (not success) and (fail_counter < 10):
    x = np.random.randint(10)
    s = np.random.choice([False, False, False, False, False,  False, False, True], replace=True)
    print(x, s)
    fail_counter = x
    success = s
if success:
    print('ppppppppppppp')

if __name__ == '__main__':
    pass
