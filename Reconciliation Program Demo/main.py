import function2
import function1
import datetime
import warnings
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

'''
##################################################################
Import File
##################################################################
'''
mother_ship = function1.MotherShip('config.xlsx', 'Open')        # This is to locate the file path
mother_ship.summon_child()       # # This is to import Bank/Internal/Cover files
'''
####################################################### ###########
Timing the processing
##################################################################
'''
progress_counter = 0       # This is to record the progress
era_start = datetime.datetime.now()
master1 = list()
master2 = list()
'''
##################################################################
Reconciliation
##################################################################
'''
for yoda in mother_ship.Onboard:
    # try:
    yoda.BattleBegins = datetime.datetime.now()
    mother_ship_capacity = len(mother_ship.Onboard)     # This is to get how many files we are currently processing
    progress_counter = progress_counter + 1     # This is to record the progress by ADDING 1 to the counter

    print('************************************************')
    print('Now Process - File Name: {} in {}/{}'.format(yoda.Identity, progress_counter, mother_ship_capacity))
    print('************************************************')
    try:
        yoda.CombineDailyAmount, yoda.DifferenceDailyAmount = function2.light_saber(
            yoda.BankAccountManager['ba_daily_amount_only'], yoda.InternalAccountManager['ia_daily_amount_only'])
    except:
        mother_ship.LightSaberLost.append(yoda)
        print('{} Combine Daily Amount or Difference Daily Amount Failed'.format(yoda.Identity))
        continue
    try:
        yoda.ImbalanceDatesValues = function2.wisdom(yoda.DifferenceDailyAmount)
    except:
        mother_ship.WisdomLost.append(yoda)
        print('{} Get Imbalance Dates and Values Failed'.format(yoda.Identity))
        continue
    # parse credit and debit of both account respectively
    ba_credit = yoda.BankAccountManager['ba_formatted'].iloc[:, [0, -1]]
    ba_debit = yoda.BankAccountManager['ba_formatted'].iloc[:, [1, -1]]
    ia_credit = yoda.InternalAccountManager['ia_formatted'].iloc[:, [0, -1]]
    ia_debit = yoda.InternalAccountManager['ia_formatted'].iloc[:, [1, -1]]
    # parse bank account credit and internal account debit diff
    bac_iad = yoda.ImbalanceDatesValues.iloc[:, [0]]
    # parse bank account debit and internal account credit diff
    bad_iac = yoda.ImbalanceDatesValues.iloc[:, [1]]
    ba_match_credit, ia_match_debit, yoda.CaveatBankCreditInternalDebit = function2.force(ba_credit, ia_debit, bac_iad, yoda.Identity)
    ba_match_debit, ia_match_credit, yoda.CaveatBankDebitInternalCredit = function2.force(ba_debit, ia_credit, bad_iac, yoda.Identity)
    if not ba_match_credit.empty:
        ba_match_credit_idx = list(ba_match_credit.iloc[:, 1].values)
    else:
        ba_match_credit_idx = []
    if not ba_match_debit.empty:
        ba_match_debit_idx = list(ba_match_debit.iloc[:, 1].values)
    else:
        ba_match_debit_idx = []
    if not ia_match_credit.empty:
        ia_match_credit_idx = list(ia_match_credit.iloc[:, 1].values)
    else:
        ia_match_credit_idx = []
    if not ia_match_debit.empty:
        ia_match_debit_idx = list(ia_match_debit.iloc[:, 1].values)
    else:
        ia_match_debit_idx = []
    ba_dark_side_idx = sorted(list(set(ba_match_credit_idx + ba_match_debit_idx)))
    ia_dark_side_idx = sorted(list(set(ia_match_credit_idx + ia_match_debit_idx)))
    yoda.BankAccountManager['ba_match'] = yoda.BankAccountManager['ba_formatted'][yoda.BankAccountManager['ba_formatted'].iloc[:, -1].isin(ba_dark_side_idx)].sort_index()
    yoda.InternalAccountManager['ia_match'] = yoda.InternalAccountManager['ia_formatted'][yoda.InternalAccountManager['ia_formatted'].iloc[:, -1].isin(ia_dark_side_idx)].sort_index()
    # collect caveat
    if yoda.CaveatBankDebitInternalCredit:
        for x in yoda.CaveatBankDebitInternalCredit:
            mother_ship.MasterCaveatBankDebitInternalCredit.append(x)
    if yoda.CaveatBankCreditInternalDebit:
        for x in yoda.CaveatBankCreditInternalDebit:
            mother_ship.MasterCaveatBankCreditInternalDebit.append(x)
    # export result
    yoda.BattleEnds = datetime.datetime.now()
    yoda.BattleDuration = yoda.BattleEnds - yoda.BattleBegins
    yoda.Residual_Diff = yoda.a_new_hope(mother_ship.Mars)
    master2.append([yoda.Identity,
                   np.round(yoda.BattleDuration.total_seconds(), 2),
                   yoda.BankAccountManager['ba_balance'],
                   yoda.InternalAccountManager['ia_balance'],
                   yoda.Residual_Diff])
    mother_ship.Success.append(yoda)
    print('\n************************************************')
    print('Success - File Name: {} in {}/{}'.format(yoda.Identity, progress_counter, mother_ship_capacity))
    print('************************************************')
    # except:
    #    continue
# finally:
era_end = datetime.datetime.now()
duration = np.round((era_end - era_start).total_seconds() / 60, 2)
ct = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')

master1.append(['Process Start At', era_start])
master1.append(['Process End At', era_end])
master1.append(['Total Time to Process (Minutes)', duration])
master1.append(['Number of Call to Process Account', len(mother_ship.Generation1SnapShot)])
master1.append(['Number of Import Account', mother_ship_capacity])
master1.append(['Number of Successfully Process Account', len(mother_ship.Success)])
master_df1 = pd.DataFrame(master1)
master_df1.columns = ['Item', 'Figure']

master_df2 = pd.DataFrame(master2)
master_df2.columns = ['Account Code', 'Time to Process (Seconds)', 'Bank Account Balance', 'Internal Account Balance', 'Residual Difference']

with pd.ExcelWriter('Process Master Summary.xlsx', engine='xlsxwriter') as writer:
    master_df1.to_excel(writer, sheet_name='Process Master Summary', startrow=1, index=None)
    master_df2.to_excel(writer, sheet_name='Process Master Summary', startrow=10, index=None)

    # create worksheet
    worksheet1 = writer.sheets['Process Master Summary']
    workbook = writer.book
    outer = workbook.add_format({'top': 1, 'bottom': 1, 'left': 1, 'right': 1})
    bold = workbook.add_format(
        {'bold': True, 'align': 'center', 'border': 1, 'valign': 'vcenter', 'num_format': '#,###'})
    num = workbook.add_format({'num_format': '#,###'})

    # page format
    worksheet1.merge_range('A1:B1', 'Process Master Summary', bold)
    worksheet1.conditional_format('A1:B8', {'type': 'no_errors', 'format': outer})
    worksheet1.conditional_format('B8:B500', {'type': 'no_errors', 'format': num})
    worksheet1.set_column(0, 0, 40)
    worksheet1.set_column(1, 1, 30)
    worksheet1.set_column(2, 4, 25)
    worksheet1.autofilter('A11:E11')

    # add caveat
    col_cum = 0
    if mother_ship.MasterCaveatBankDebitInternalCredit:
        bd_ic = pd.DataFrame(mother_ship.MasterCaveatBankDebitInternalCredit)
        bd_ic.columns = ['Account Code', 'Date', 'Imbalance Value', 'Message', 'Original Bank Trans #', 'Match Bank Trans #',
                         'Original Internal Trans #', 'Match Internal Trans #']
        bd_ic.to_excel(writer, sheet_name='Caveat Summary', index=None, startrow=1)
        col_cum += bd_ic.shape[0] + 4
        worksheet2 = writer.sheets['Caveat Summary']
        worksheet2.merge_range('A1:H1', 'Caveat :: Bank Debit - Internal Credit', bold)
        worksheet2.set_column(0, 7, 25)
        worksheet2.autofilter('A2:H2')
    if mother_ship.MasterCaveatBankCreditInternalDebit:
        bc_id = pd.DataFrame(mother_ship.MasterCaveatBankCreditInternalDebit)
        bc_id.columns = ['Account Code', 'Date', 'Imbalance Value', 'Message', 'Original Bank Trans #', 'Match Bank Trans #',
                         'Original Internal Trans #', 'Match Internal Trans #']
        bc_id.to_excel(writer, sheet_name='Caveat Summary', index=None, startrow=1 + col_cum)
        worksheet2 = writer.sheets['Caveat Summary']
        worksheet2.merge_range('A{}:H{}'.format(col_cum + 1, col_cum + 1), 'Caveat :: Bank Credit - Internal Debit', bold)
        worksheet2.set_column(0, 7, 25)
        worksheet2.autofilter('A2:H2')
