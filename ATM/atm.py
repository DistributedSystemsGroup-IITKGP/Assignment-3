import sys
import time
from functools import partial
sys.path.append("../")
import questionary
from pysyncobj import SyncObj, replicated_sync


class ATM(SyncObj):

    def __init__(self, selfNodeAddr, otherNodeAddrs):
        super(ATM, self).__init__(selfNodeAddr, otherNodeAddrs)
        self.balance = dict()

    @replicated_sync
    def signin(self, username):
        if username in self.balance:
            return 0
        self.balance[username] = 0
        return 1

    @replicated_sync
    def deposit(self, username, value):
        self.balance[username] += value
        return self.balance[username]

    @replicated_sync
    def withdraw(self, username, value):
        if self.balance[username] >= value:
            self.balance[username] -= value
            return self.balance[username]

        return -1

    @replicated_sync
    def transfer(self, from_user, to_user, value):
        if to_user not in self.balance:
            return -1
        if self.balance[from_user] < value:
            return -2

        self.balance[from_user] -= value
        self.balance[to_user] += value
        return self.balance[from_user]

    def balance_inquiry(self, username):
        return self.balance[username]


    def signin_logging(self, r, err):
        if r==1:
            print("Signup Successful... Signing in!\n")
        else:
            print("Username already exists... Signing in!\n")

    def withdraw_logging(self, r, err):
        if r==-1:
            print('Insufficient balance!\n')
        else:
            print('Withdrawal successful!')
            print('Updated balance:',r,'\n')

    def deposit_logging(self, r, err):
        print('Deposition successful!')
        print('Updated balance:',r,'\n')

    def transfer_logging(self, r, err):
        if r==-1:
            print('Target user doesn\'t exist!\n')
        elif r==-2:
            print('Insufficient balance!\n')
        else:
            print('Transfer successful!')
            print('Updated balance:',r,'\n')

if __name__ == '__main__':
    # if len(sys.argv) < 3:
    #     print('Usage: %s self_port partner1_port partner2_port ...' % sys.argv[0])
    #     sys.exit(-1)

    port = int(sys.argv[1])
    partners = []
    # partners = ['localhost:%d' % int(p) for p in sys.argv[2:]]

    o = ATM('localhost:%d' % port, partners)

    # print('='*55)
    print('\t+++Welcome to ABD Bank ATM+++')
    # print('='*55)
    print()

    while True:
        time.sleep(0.5)

        print(o.getStatus())

        if o._getLeader() is None:
            continue

        un = input('Enter Username: ')
        o.signin(un, callback=partial(o.signin_logging))


        while True:
            time.sleep(0.5)

            if o._getLeader() is None:
                continue

            r = questionary.select(
                "Operations",
                choices=["Deposit", "Withdraw", "Balance Inquiry", 'Transfer','Logout'],
            ).ask()

            if r=='Deposit':
                amount = int(input('Enter Amount: '))
                o.deposit(un, amount, callback=partial(o.deposit_logging))

            if r=='Withdraw':
                amount = int(input('Enter Amount: '))
                o.withdraw(un, amount, callback=partial(o.withdraw_logging))

            if r=='Balance Inquiry':
                print('Current Balance:',o.balance_inquiry(un),'\n')

            if r=='Transfer':
                to = input('Enter User: ')
                amt = int(input('Enter Amount: '))
                o.transfer(un, to, amt, callback=partial(o.transfer_logging))

            if r=='Logout':
                print()
                break
