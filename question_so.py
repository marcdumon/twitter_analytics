# --------------------------------------------------------------------------------------------------------
# 2020/07/07
# src - question_so.py
# md
# --------------------------------------------------------------------------------------------------------

"""
What is the difference between setting a class variable and


"""


class UserObject1:
    username = ''

    def __init__(self, data):
        self.data = data

    def process_user(self):
        # print(self.username, self.data)
        return f'{self.username} - {self.data}'
#
# for username in ['u1', 'u2']:
#     print('====>', username)
#     UserObject1.username = username
#     for data in ['d1', 'd2']:
#         print('====>', data)
#         user_object = UserObject1(data)
#         user_object.process_user()


class UserObject2:

    def __init__(self, username, data):
        self.username = username
        self.data = data

    def process_user(self):
        # print(self.username, self.data)
        return f'{self.username} - {self.data}'

#
# for username in ['u1', 'u2']:
#     print('====>', username)
#     # UserObject1.username = username
#     for data in ['d1', 'd2']:
#         print('====>', data)
#         user_object = UserObject2(username, data)
#         user_object.process_user()


def test_uo1():
    usernames = [f'user_{i}' for i in range(10)]
    datas = [f'data_{i}' for i in range(10)]
    for username in usernames:
        UserObject1.username = username
        for data in datas:
            uo_1 = UserObject1(data)
            uo_1.process_user()

def test_uo2():
    usernames = [f'user_{i}' for i in range(10)]
    datas = [f'data_{i}' for i in range(10)]
    for username in usernames:
        for data in datas:
            uo_2 = UserObject2(username,data)
            uo_2.process_user()


if __name__ == '__main__':
    import timeit
    print(timeit.timeit(
        'test_uo1()',
        setup='from __main__ import test_uo1'
    ))

    print(timeit.timeit(
        'test_uo2()',
        setup='from __main__ import test_uo2'
    ))

