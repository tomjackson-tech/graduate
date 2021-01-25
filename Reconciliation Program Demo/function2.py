import itertools
import numpy as np
import pandas as pd
import sys
sys.setrecursionlimit(2000)

if __name__ != '__main__':
    print('IMPORT MODULE:', __name__)

optimal_dict = dict()
fact_dict = dict()


def light_saber(ba_daily_amount_only, ia_daily_amount_only):
    """
    This function generates the combination of and the differences between two accounts.

    :param ba_daily_amount_only:  DataFrame, the original external account file imported
    :param ia_daily_amount_only:  DataFrame, the original internal account file imported

    :return
    """
    combined_daily_amount = pd.concat([ba_daily_amount_only, ia_daily_amount_only], axis=1).fillna(0)
    combined_daily_amount_temp = combined_daily_amount.copy()
    combined_daily_amount_temp.columns = ['BankCredit - InternalDebit', 'BankDebit - InternalCredit', 'BankDebit - InternalCredit', 'BankCredit - InternalDebit']
    difference_daily_amount = combined_daily_amount_temp.iloc[:, 0:2] - combined_daily_amount_temp.iloc[:, 2:4]
    difference_daily_amount = difference_daily_amount.round(2)
    return combined_daily_amount, difference_daily_amount


def wisdom(difference_daily_amount):
    bc_id = np.round(difference_daily_amount['BankCredit - InternalDebit'], 0)
    bd_ic = np.round(difference_daily_amount['BankDebit - InternalCredit'], 0)

    problem_mask = (bc_id != 0).astype('int') + (bd_ic != 0).astype('int')
    imbalance_clone = difference_daily_amount[problem_mask != 0]
    return imbalance_clone


def force(ba_amount, ia_amount, imbalance_clone, identity):
    # create output dataframe
    bank_account_output = pd.DataFrame()
    internal_account_output = pd.DataFrame()
    # caveat
    caveat = list()
    # how many days to be processed
    length = imbalance_clone.shape[0]
    counter = 0
    # processing
    for date, value in imbalance_clone.iterrows():
        # processing message
        counter += 1
        target = np.round(float(value), 0)
        ###########################################################
        # layer #0 skip if the target equals zero
        if target == 0:
            continue
        ###########################################################
        print('\n>> Process {} ** {} [{}/{}]'.format(identity, date, counter, length))
        # parse date
        if date in list(ba_amount.index):     # to see if any transaction occur today in the bank account
            ba_today = pd.DataFrame(ba_amount.loc[[date], :].replace(0, np.nan).dropna())
        else:
            ba_today = pd.DataFrame()
        if date in list(ia_amount.index):     # to see if any transaction occur today in the internal account
            ia_today = pd.DataFrame(ia_amount.loc[[date], :].replace(0, np.nan).dropna())
        else:
            ia_today = pd.DataFrame()
        ###########################################################
        # layer #1 drop duplicates

        print('     >>> Layer #1 Drop Duplicates')
        if not ba_today.empty and not ia_today.empty:
            print('         >>> Sub-layer Single Drop')
            ba_unique, ia_unique = reduce_duplicates(ba_today, ia_today)
            print('         >>> Sub-layer Merge Drop')
            ba_unique, ia_unique = merge_transactions(ba_unique, ia_unique)
            print('         >>> Sub-layer Reversed Merge Drop')
            ba_unique, ia_unique = merge_transactions(ba_unique.iloc[::-1], ia_unique.iloc[::-1])
            ba_unique, ia_unique = ba_unique.iloc[::-1], ia_unique.iloc[::-1]
        else:
            ba_unique, ia_unique = ba_today, ia_today
        print('         >>>> Drop {} Duplicated Value'.format(ba_today.shape[0] - ba_unique.shape[0]))
        ###########################################################
        # layer #2 forward match to get the target
        print('     >>> Layer #2 Forward Match')
        bank_account_output_temp, internal_account_output_temp = forward_match(ba_unique.copy(), ia_unique.copy(), target)
        if (not bank_account_output_temp.empty) or (not internal_account_output_temp.empty):
            bank_account_output = pd.concat([bank_account_output, bank_account_output_temp], axis=0)
            internal_account_output = pd.concat([internal_account_output, internal_account_output_temp], axis=0)
            continue
        ###########################################################
        # layer #3 backward eliminate to get the target
        print('\n     >>> Layer #3 Backward Eliminate')
        bank_account_output_temp, internal_account_output_temp = backward_eliminate(ba_unique.copy(), ia_unique.copy(), target)
        if (not bank_account_output_temp.empty) or (not internal_account_output_temp.empty):
            bank_account_output = pd.concat([bank_account_output, bank_account_output_temp], axis=0)
            internal_account_output = pd.concat([internal_account_output, internal_account_output_temp], axis=0)
            ba_match_len = bank_account_output_temp.shape[0]
            ia_match_len = internal_account_output_temp.shape[0]
            message = 'Use Backward Eliminate'
        else:
            ba_match_len = 0
            ia_match_len = 0
            message = "Can't Find A Match"
        ba_ori_len = ba_today.shape[0]
        ia_ori_len = ia_today.shape[0]
        caveat.append([identity, date.strftime('%Y/%m/%d'), target, message, ba_ori_len, ba_match_len, ia_ori_len, ia_match_len])
        ###########################################################
    return bank_account_output, internal_account_output, caveat


def jedi_helper(array, method):
    optimal_dict = {}
    length = len(array)

    def tco_factorial(n, cum):
        if n == 0:
            return cum
        else:
            return tco_factorial(n - 1, n * cum)

    def factorial(num):
        global fact_dict
        if num in fact_dict.keys():
            return fact_dict[num]
        else:
            fact_dict[num] = tco_factorial(num, 1)
            return fact_dict[num]

    def combination_calculator(n, c):
        return factorial(n) / (factorial(c) * factorial(n - c))

    def optimal_selection(num, limit=1000):
        global optimal_dict
        if num in optimal_dict.keys():
            return optimal_dict[num]
        else:
            cum_combs_num = 0
            optimal_choose = num
            for selection in range(1, num + 1):
                cum_combs_num += combination_calculator(num + 1, selection)
                if cum_combs_num >= limit:
                    optimal_choose = max(selection - 1, 1)
                    break
            optimal_dict[num] = optimal_choose
            return optimal_choose

    if method == 'drop_duplicates':
        combination = [list(x) for x in list(itertools.combinations(array, 1))]
        return combination
    elif method == 'merge_match':
        cum_combs = []
        optimal_choose = optimal_selection(length, 5000)
        for choose in range(1, optimal_choose + 1):
            cache_combs = [list(x) for x in list(itertools.combinations(array, choose))]
            cum_combs += cache_combs
        return cum_combs
    elif method == 'forward_match':
        cum_combs = []
        optimal_choose = optimal_selection(length)
        for choose in range(1, optimal_choose + 1):
            cache_combs = [list(x) for x in list(itertools.combinations(array, choose))]
            cum_combs += cache_combs
        return cum_combs
    elif method == 'backward_eliminate':
        cum_combs = []
        optimal_choose = optimal_selection(length)
        for choose in range(length, max(0, length - optimal_choose), -1):
            cache_combs = [list(x) for x in list(itertools.combinations(array, choose))]
            cum_combs += cache_combs
        return cum_combs


def reduce_duplicates(ba_today, ia_today):
    ba_today_combs = jedi_helper(ba_today.iloc[:, -1], 'drop_duplicates')
    ia_today_combs = jedi_helper(ia_today.iloc[:, -1], 'drop_duplicates')
    ba_today_combs = [x[0] for x in ba_today_combs]
    ia_today_combs = [x[0] for x in ia_today_combs]
    while True:
        outer = False
        for ba_combs_index, ba_trans_index in enumerate(ba_today_combs):
            for ia_combs_index, ia_trans_index in enumerate(ia_today_combs):
                ba_numbers = ba_today[ba_today.iloc[:, -1] == ba_trans_index].iloc[0, 0]
                ia_numbers = ia_today[ia_today.iloc[:, -1] == ia_trans_index].iloc[0, 0]
                if ba_numbers == ia_numbers:
                    outer = True
                    ba_today_combs.pop(ba_combs_index)
                    ia_today_combs.pop(ia_combs_index)
                    break
            if outer:
                break
        if not outer:
            break
    ba_survivor = ba_today[ba_today.iloc[:, -1].isin(ba_today_combs)]
    ia_survivor = ia_today[ia_today.iloc[:, -1].isin(ia_today_combs)]
    return ba_survivor, ia_survivor


def merge_transactions(ba_today, ia_today):
    ia_today.reset_index(drop=True, inplace=True)
    ba_today.reset_index(drop=True, inplace=True)
    while True:
        match_condition = False
        ba_today['Cum Amount'] = ba_today.iloc[:, 0].cumsum()
        ia_today['Cum Amount'] = ia_today.iloc[:, 0].cumsum()
        for amount_idx, amount in enumerate(ba_today.iloc[:, -1]):
            for cum_idx, cum_amount in enumerate(ia_today.iloc[:, -1]):
                if amount == cum_amount:
                    match_condition = True
                    ba_today = ba_today.iloc[(amount_idx + 1):, :]
                    ba_today.reset_index(drop=True, inplace=True)
                    ia_today = ia_today.iloc[(cum_idx + 1):, :]
                    ia_today.reset_index(drop=True, inplace=True)
                    break
                elif amount < cum_amount:
                    break
            if match_condition:
                break
        if not match_condition:
            break
    ba_today.drop('Cum Amount', axis=1, inplace=True)
    ia_today.drop('Cum Amount', axis=1, inplace=True)
    return ba_today, ia_today


def match(ba_today, ia_today, target, method):
    def one_way_match(combs_list, transactions):
        nonlocal temp
        nonlocal total
        for combination in combs_list:
            temp += 1
            print('\r' + '         >>>[Progress]:[%s%s]%.2f%%;' % ('█' * int(temp * 20 / total), ' ' * (20 - int(temp * 20 / total)), float(temp / total * 100)), end='')
            combination_array = transactions[transactions.iloc[:, -1].isin(combination)].iloc[:, 0]
            combination_sum = np.round(combination_array.sum(), 0)
            if combination_sum == np.abs(target):
                print('\n             >>>> Find 1 Possible Combination')
                return combination
        return []

    def two_way_match(combs_list1, transactions1, combs_list2, transactions2):
        nonlocal temp
        nonlocal total
        collector = []
        match_pair = [[], []]
        for combination1 in combs_list1:
            combination_array1 = transactions1[transactions1.iloc[:, -1].isin(combination1)].iloc[:, 0]
            combination_sum1 = np.round(combination_array1.sum(), 2)
            for combination2 in combs_list2:
                temp += 1
                print('\r' + '         >>>[Progress]:[%s%s]%.2f%%;' % ('█' * int(temp * 20 / total), ' ' * (20 - int(temp * 20 / total)), float(temp / total * 100)), end='')
                combination_array2 = transactions2[transactions2.iloc[:, -1].isin(combination2)].iloc[:, 0]
                combination_sum2 = np.round(combination_array2.sum(), 2)
                diff = np.round(combination_sum1 - combination_sum2, 0)
                if diff == target:
                    collector.append([combination1, combination2])
        print('\n             >>>> Find {} Possible Combination'.format(len(collector)))
        shortest = np.inf
        for comb_pair in collector:
            leng = len(comb_pair[0]) + len(comb_pair[1])
            if leng < shortest:
                shortest = leng
                match_pair = comb_pair
        return match_pair[0], match_pair[1]
    ##############################################################
    print('         >>> Target {}'.format(target))
    print('         >>> Bank Account Trans Num: {} / Internal Account Trans Num: {}'.format(ba_today.shape[0], ia_today.shape[0]))
    if not ba_today.empty:
        ba_combs = jedi_helper(ba_today.iloc[:, -1], method)  # get combination
    else:
        ba_combs = []
    if not ia_today.empty:
        ia_combs = jedi_helper(ia_today.iloc[:, -1], method)  # get combination
    else:
        ia_combs = []
    total = len(ba_combs) * len(ia_combs) + len(ba_combs) * int(target > 0) + len(ia_combs) * int(target < 0)
    temp = 0
    print('         >>> Total Number of Combination to Be Tried: {}'.format(total))
    ##############################################################
    # Scenario1: Only bank account
    if ba_combs and (target > 0):
        ba_match_combination = one_way_match(ba_combs, ba_today)
        if ba_match_combination:
            ba_match_transaction = ba_today[ba_today.iloc[:, -1].isin(ba_match_combination)]
            return ba_match_transaction, pd.DataFrame()
    # Scenario2: Only internal account
    if ia_combs and (target < 0):
        ia_match_combination = one_way_match(ia_combs, ia_today)
        if ia_match_combination:
            ia_match_transaction = ia_today[ia_today.iloc[:, -1].isin(ia_match_combination)]
            return pd.DataFrame(), ia_match_transaction
    # Scenario2: Both accounts
    if ba_combs and ia_combs:
        ba_match_combination, ia_match_combination = two_way_match(ba_combs, ba_today, ia_combs, ia_today)
        if ba_match_combination and ia_match_combination:
            ia_match_transaction = ia_today[ia_today.iloc[:, -1].isin(ia_match_combination)]
            ba_match_transaction = ba_today[ba_today.iloc[:, -1].isin(ba_match_combination)]
            return ba_match_transaction, ia_match_transaction
    return pd.DataFrame(), pd.DataFrame()


def backward_eliminate(ba_today, ia_today, target):
    while True:
        ba_today_temp, ia_today_temp = match(ba_today, ia_today, target, 'backward_eliminate')
        if (ba_today_temp.shape[0] == ba_today.shape[0]) and (ia_today_temp.shape[0] == ia_today.shape[0]):
            return ba_today_temp, ia_today_temp
        if ba_today_temp.empty or ia_today_temp.empty:
            return ba_today_temp, ia_today_temp
        elif ba_today_temp.empty and ia_today_temp.empty:
            raise ValueError('Stuck In An Infinite Loop')
        ba_today, ia_today = ba_today_temp, ia_today_temp


def forward_match(ba_today, ia_today, target):
    return match(ba_today, ia_today, target, 'forward_match')