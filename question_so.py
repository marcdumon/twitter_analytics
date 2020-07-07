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

    def __init__(self, date):
        self.date = date

    def process_user(self):
        print(self.username, self.date)


for username in ['u1', 'u2']:
    print('====>', username)
    UserObject1.username = username
    for date in ['d1', 'd2']:
        print('====>', date)
        user_object = UserObject1(date)
        user_object.process_user()


class UserObject2:

    def __init__(self, username, date):
        self.username = username
        self.date = date

    def process_user(self):
        print(self.username, self.date)


for username in ['u1', 'u2']:
    print('====>', username)
    # UserObject1.username = username
    for date in ['d1', 'd2']:
        print('====>', date)
        user_object = UserObject2(username, date)
        user_object.process_user()
