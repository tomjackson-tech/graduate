import pandas as pd
import numpy as np
import datetime

if __name__ != '__main__':
    print('IMPORT MODULE:', __name__)

'''
███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
Node Object: Yoda
███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
'''


class Yoda:
    def __init__(self):
        self.Identity = None
        self.CombineDailyAmount = None
        self.DifferenceDailyAmount = None
        self.ImbalanceDatesValues = None
        self.BattleBegins = None
        self.BattleEnds = None
        self.BattleDuration = None
        self.Status = None
        self.Summoned = False
        self.Era = None
        self.Residual_Diff = False
        self.CaveatBankCreditInternalDebit = list()
        self.CaveatBankDebitInternalCredit = list()
        
        self.CoverPageManager = {
            'account_owner': None,
            'done_by': None,
            'validated_by': None,
            'entity': None,
            'bank_code': None,
            'analized': None,
            'regularized': None,
            'currency': None,
            'banking_acct_number': None,
            'end_of_month': None
        }
        self.BankAccountManager = {
            'FormatManager': None,
            'ba_balance': 0,
            'ba_raw': pd.DataFrame(),
            'ba_formatted': pd.DataFrame(),
            'ba_daily_amount_only': pd.DataFrame(),
            'ba_match': pd.DataFrame(),
        }
        self.InternalAccountManager = {
            'ia_balance': 0,
            'ia_raw': pd.DataFrame(),
            'ia_formatted': pd.DataFrame(),
            'ia_daily_amount_only': pd.DataFrame(),
            'ia_match': pd.DataFrame(),
        }
    '''
    ##################################################################
    Function to Import Cover Page
    ##################################################################
    '''
    def import_cover(self, cover_commander, apocalypse):
        """
        This is to import cover information of yoda
        :param cover_commander: DataFrame, all cover information
        :param apocalypse: str, end of the month
        :return:
        cover_page_manager: dict, cover information of yoda
        """
        filter_net = (cover_commander[cover_commander['Account Code'].astype('str') == str(self.Identity)])
        cover_page_manager = {}
        cover_page_manager['banking_acct_number'] = filter_net['Bank Account Number'].values[0]
        cover_page_manager['account_owner'] = filter_net['Account Owner'].values[0]
        cover_page_manager['done_by'] = filter_net['Done by'].values[0]
        cover_page_manager['validated_by'] = filter_net['Validated by'].values[0]
        cover_page_manager['entity'] = filter_net['Entity'].values[0]
        cover_page_manager['bank_code'] = filter_net['Bank Code'].values[0]
        cover_page_manager['currency'] = filter_net['Currency'].values[0]
        cover_page_manager['analized'] = filter_net['All are analized ? (Yes/No)'].values[0]
        cover_page_manager['regularized'] = filter_net['All are regularized ? (Yes/No)'].values[0]
        cover_page_manager['end_of_month'] = apocalypse
        return cover_page_manager
    '''
    ##################################################################
    Function to Import Internal Account
    ##################################################################
    '''
    def parse_internal_account_transaction(self, internal_account_commander):
        """
        This is to get original internal account
        :param internal_account_commander: str, all bank account transactions
        :return:
        bank_account_raw: DataFrame, original bank account
        """
        return internal_account_commander[internal_account_commander.iloc[:, 1] == str(self.Identity)]

    def format_internal_account_transaction(self, internal_account_raw):
        """
        This is to get the clean and formatted internal account
        :param internal_account_raw: DataFrame, original internal account
        :return:
        bank_account_formatted: DataFrame, trimmed, formatted, datetime converted internal account
        << index >> Date (freq=Intra-day)
        << col-order >> Credit(+, float), Debit(+, float), Subject Name, Trans Number, Description, Index
        """
        # try:
        if self.CoverPageManager['currency'] == 'TWD':
            ia_abstract = internal_account_raw.iloc[:, [2, 4, 5, 6, 8]]
        else:
            ia_abstract = internal_account_raw.iloc[:, [2, 4, 5, 6, 10]]
        ia_abstract['Credit'] = [x if x >= 0 else 0 for x in ia_abstract.iloc[:, 4]]
        ia_abstract['Debit'] = [-x if x < 0 else 0 for x in ia_abstract.iloc[:, 4]]
        internal_account_formatted = ia_abstract.iloc[:, [1, 5, 6, 0, 2, 3]]
        internal_account_formatted.iloc[:, [1, 2]] = internal_account_formatted.iloc[:, [1, 2]].apply(pd.to_numeric, errors='coerce').fillna(0)
        internal_account_formatted['Index'] = [str(i).zfill(2) for i in range(1, internal_account_formatted.shape[0] + 1)]
        internal_account_formatted.columns = ['Date', 'Credit', 'Debit', 'Subject Name', 'Trans Number', 'Description', 'Index']
        internal_account_formatted.iloc[:, 0] = pd.to_datetime(internal_account_formatted.iloc[:, 0])
        internal_account_formatted.index = internal_account_formatted.iloc[:, 0]
        internal_account_formatted = internal_account_formatted.iloc[:, 1:]
        # except:
        #     raise ValueError('{} Internal Account Transactions Format Is Incorrect'.format(self.Identity))
        return internal_account_formatted

    def parse_internal_account_balance(self, internal_account_balance_sergeant):
        """
        This is to get internal balance
        :param internal_account_balance_sergeant: DataFrame, all balance information
        :return:
        balance: float, internal account balance of yoda
        * Error Raised
        ValueError('{} Internal Account Balance Not A Number'.format(self.Identity))
        """
        if self.Identity in list(internal_account_balance_sergeant.iloc[:, 1].values):
            balance = internal_account_balance_sergeant[internal_account_balance_sergeant.iloc[:, 1] == self.Identity]
            try:
                return float(balance.iat[0, -1])
            except:
                raise ValueError('{} Internal Account Balance Not A Number'.format(self.Identity))
        else:
            return False

    @staticmethod
    def resample_internal_account_sum(internal_account_formatted):
        """
        This is to get resampled internal account
        :param internal_account_formatted: DataFrame, clean and formatted internal account
        :return:
        internal_account_sum: internal account with only two columns (Debit, Credit), frequency daily
        << index >> Date (freq=Day)
        << col-order >> Credit(+, float), Debit(+, float)
        """
        internal_account_sum = internal_account_formatted.iloc[:, [0, 1]]
        internal_account_sum = internal_account_sum.resample('d').sum()
        internal_account_sum.columns = ['Credit', 'Debit']
        internal_account_sum.astype('float').fillna(0)
        return internal_account_sum
    '''
    ##################################################################
    Function to Import Bank Account
    ##################################################################
    '''
    def import_bank_account_transaction(self, bank_account_planet):
        """
        This is to get original bank account
        :param bank_account_planet: str, all bank account directory
        :return:
        bank_account_raw: DataFrame, original bank account
        * Error Raised
        ValueError('Bank Account Balance Does Not Exist')
        """
        yoda_village = bank_account_planet + '//#' + str(self.Identity) + '.xlsx'
        try:
            bank_account_raw = pd.read_excel(yoda_village, header=None).dropna(axis=0, how='all').dropna(axis=1, how='all')
        except FileNotFoundError:
            raise ValueError('Bank Account Balance Does Not Exist')
        return bank_account_raw

    def format_bank_account_transaction(self, bank_account_raw):
        """
        This is to get the clean and formatted bank account
        :param bank_account_raw: DataFrame, original bank account
        :return:
        bank_account_formatted: DataFrame, trimmed, formatted, datetime converted bank account
        << col-order >> Credit, Debit, (Description), Balance, Index
        << index >> Date (freq=Intra-day)
        """
        # the necessary columns we want
        use_col = list(self.BankAccountManager['FormatManager'].loc[:, ['Date', 'Credit', 'Debit', 'Amount', 'C/D', 'Description','Balance']].values[0])
        use_col = [x for x in use_col if str(x) != 'nan']
        use_col = [int(x) for x in use_col if str(x) != 'None']
        if (len(use_col) != 4) and (len(use_col) != 5):
            raise ValueError('The Number of Formation Must Be 4 Or 5')

        # trim the bank account, only necessary columns remain
        skip_row = int(self.BankAccountManager['FormatManager']['Skip Row'])
        bank_account_raw.columns = bank_account_raw.iloc[skip_row, :]
        bank_account_formatted = pd.DataFrame(bank_account_raw.iloc[skip_row + 1:, use_col])

        # determine the method to format the bank account
        try:
            method = int(self.BankAccountManager['FormatManager']['Method'].values[0])
        except:
            raise ValueError('{} Bank Account Formatting Method Is Not Found'.format(self.Identity))
        bank_account_formatted = self.format_bank_account_columns(bank_account_formatted, method)

        # transfer datetime
        date_format = int(self.BankAccountManager['FormatManager']['Date Format'])
        bank_account_formatted = bank_account_formatted.replace(' ', np.nan).dropna(how='all', axis=0)
        bank_account_formatted = self.format_date(bank_account_formatted, date_format)
        try:
            bank_account_formatted.index = pd.to_datetime(bank_account_formatted.iloc[:, 0])
            bank_account_formatted = bank_account_formatted.iloc[:, 1:]
        except:
            raise ValueError('{} Bank Account Datetime Conversion Failed'.format(self.Identity))
        
        # check transaction period
        this_month_end = datetime.datetime.strptime(self.CoverPageManager['end_of_month'][:10], '%Y-%m-%d')
        last_month_end = this_month_end.replace(day=1) - datetime.timedelta(days=1)
        if bank_account_formatted.shape[0] > 1:
            if (bank_account_formatted.index[0] > this_month_end) or (bank_account_formatted.index[0] < last_month_end):
                    raise ValueError('{} Bank Account Has Transactions Occur After {} or Before {}'.format(self.Identity, this_month_end, last_month_end))
        bank_account_formatted['Index'] = [str(i).zfill(2) for i in range(1, bank_account_formatted.shape[0] + 1)]
        bank_account_formatted = bank_account_formatted.loc[:, ['Credit', 'Debit', 'Description', 'Balance', 'Index']]
        return bank_account_formatted

    def parse_bank_account_balance(self, bank_account_formatted):
        """
        This is to get bank balance
        :param bank_account_formatted: DataFrame, completely formatted bank account
        :return:
        balance: float, bank account balance of yoda
        * Warning Message
        print('{} Bank Account Balance Parse Fail'.format(self.Identity))
        """
        try:
            balance = float(bank_account_formatted.iloc[:, -2].ffill()[-1])
        except:
            print('{} Bank Account Balance Parse Fail'.format(self.Identity))
            balance = 0
        return balance

    @staticmethod
    def resample_bank_account_sum(bank_account_formatted):
        """
        This is to get resampled bank account
        :param bank_account_formatted: DataFrame, clean and formatted bank account
        :return:
        bank_account_sum: bank account with only two columns (Credit, Debit), frequency daily
        """
        bank_account_sum = bank_account_formatted.iloc[:, [0, 1]]
        if not bank_account_sum.empty:
            bank_account_sum = bank_account_sum.resample('d').sum()
            bank_account_sum.columns = ['Credit', 'Debit']
        return bank_account_sum

    def format_bank_account_columns(self, bank_account_formatted, method):
        """
        This is to adjust the order of columns of bank account and handle some special cases
        :param bank_account_formatted: DataFrame, trimmed bank account, but column information is
        still messy and date is messy too
        :param method:  int, type of format method, CD/Amount or Credit/Debit
        :return:
        bank_account_formatted: DataFrame, formatted bank account but date column is still messy


        * Error Raised:
        ValueError('{} Using Unspecified Bank Account Formation Method'.format(self.Identity))
        """
        # credit and debit method to format the bank account
        if method == 0:
            if str(self.BankAccountManager['FormatManager']['Description'].values[0]) == 'None':
                bank_account_formatted.iloc[:, [1, 2, -1]] = bank_account_formatted.iloc[:, [1, 2, -1]].apply(pd.to_numeric, errors='coerce').fillna(0)
                bank_account_formatted['Description'] = ['No Description'] * bank_account_formatted.shape[0]
                bank_account_formatted = bank_account_formatted.iloc[:, [0, 1, 2, 4, 3]]
                bank_account_formatted.columns = ['Date', 'Credit', 'Debit', 'Description', 'Balance']

            else:
                bank_account_formatted.iloc[:, [1, 2, -1]] = bank_account_formatted.iloc[:, [1, 2, -1]].apply(pd.to_numeric, errors='coerce').fillna(0)
                bank_account_formatted.columns = ['Date', 'Credit', 'Debit', 'Description', 'Balance']

        # Credit and Debit Method to format the bank account
        elif method == 1:
            if str(self.BankAccountManager['FormatManager']['Description'].values[0]) == 'None':
                bank_account_formatted.iloc[:, [1, -1]] = bank_account_formatted.iloc[:, [1, -1]].apply(pd.to_numeric, errors='coerce').fillna(0)
                bank_account_formatted['Description'] = ['No Description'] * bank_account_formatted.shape[0]
                bank_account_formatted = bank_account_formatted.iloc[:, [0, 1, 2, 4, 3]]
                bank_account_formatted.columns = ['Date', 'Amount', 'C/D', 'Description', 'Balance']
            else:
                bank_account_formatted.iloc[:, [1, -1]] = bank_account_formatted.iloc[:, [1, -1]].apply(pd.to_numeric, errors='coerce').fillna(0)
                bank_account_formatted.columns = ['Date', 'Amount', 'C/D', 'Description', 'Balance']
            bank_account_formatted['Credit'] = [1 if (x == '-') or (x == '提出') or (x == 'D') else 0 for x in bank_account_formatted['C/D']]
            bank_account_formatted['Debit'] = [1 if (x == '+') or (x == '存入') or (x == 'C') else 0 for x in bank_account_formatted['C/D']]
            bank_account_formatted['Credit'] = (bank_account_formatted['Amount'] * bank_account_formatted['Credit']).apply(np.abs)
            bank_account_formatted['Debit'] = (bank_account_formatted['Amount'] * bank_account_formatted['Debit']).apply(np.abs)
            bank_account_formatted.drop(['Amount', 'C/D'], axis=1, inplace=True)
            bank_account_formatted_temp = bank_account_formatted.iloc[:, [0, -2, -1]]
            bank_account_formatted = pd.concat([bank_account_formatted_temp, bank_account_formatted.iloc[:, 1:-2]],axis=1)

        # Special Case for 800251
        elif method == 2:
            bank_account_formatted.iloc[:, [1, -1]] = bank_account_formatted.iloc[:, [1, -1]].apply(pd.to_numeric,errors='coerce').fillna(0)
            bank_account_formatted.columns = ['Date', 'Amount', 'Description', 'Balance']
            bank_account_formatted['Credit'] = [1 if x < 0 else 0 for x in bank_account_formatted['Amount']]
            bank_account_formatted['Debit'] = [1 if x > 0 else 0 for x in bank_account_formatted['Amount']]
            bank_account_formatted['Credit'] = (bank_account_formatted['Amount'] * bank_account_formatted['Credit']).apply(np.abs)
            bank_account_formatted['Debit'] = (bank_account_formatted['Amount'] * bank_account_formatted['Debit']).apply(np.abs)
            bank_account_formatted.drop(['Amount'], axis=1, inplace=True)
            bank_account_formatted_temp = bank_account_formatted.iloc[:, [0, -2, -1]]
            bank_account_formatted = pd.concat([bank_account_formatted_temp, bank_account_formatted.iloc[:, 1:-2]],axis=1)

            # other unspecified methods
        else:
            raise ValueError('{} Using Unspecified Bank Account Formation Method'.format(self.Identity))
        bank_account_formatted.iloc[:, [1, 2]] = bank_account_formatted.iloc[:, [1, 2]].apply(pd.to_numeric, errors='coerce').fillna(0)
        return bank_account_formatted

    def format_date(self, ba_partial_formatted, format_num):
        """
        This is to format the date column of bank account

        :param ba_partial_formatted: DataFrame, formatted bank account but date column is still messy
        :param format_num: int, type of datetime format used to format the account

        :return:
        ba_partial_formatted: DataFrame, formatted bank account with date column converted datetime object


        * Error Raised:
        raise ValueError('{} Bank Account Incorrect Data Format　{}'.format(self.Identity, format_num))
        """
        ba_partial_formatted.iloc[:, 0] = ba_partial_formatted.iloc[:, 0].ffill()
        ba_partial_formatted.iloc[:, 0] = ba_partial_formatted.iloc[:, 0].bfill()
        ba_partial_formatted_notna = ba_partial_formatted[ba_partial_formatted.iloc[:, 0].notna()]
        ba_partial_formatted_isna = ba_partial_formatted[ba_partial_formatted.iloc[:, 0].isna()]
        ba_partial_formatted_isna.iloc[:, 0] = ba_partial_formatted_isna.iloc[:, 0].fillna(pd.to_datetime(self.Era))
        ba_partial_formatted_isna.index = ba_partial_formatted_isna.iloc[:, 0]
        try:
            # 正常的
            if format_num == 0:
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0], errors='coerce')
            # 連在一起的 20204020
            elif format_num == 1:
                new_time = ['-'.join([str(x)[:4], str(x)[4:6], str(x)[6:8]]) for x in ba_partial_formatted_notna.iloc[:, 0]]
                ba_partial_formatted_notna.iloc[:, 0] = new_time
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
            # 正常的加時間
            elif format_num == 2:
                ba_partial_formatted_notna.iloc[:, 0] = [str(x)[:10] if len(str(x)) > 8 else x
                                   for x in ba_partial_formatted_notna.iloc[:, 0]]
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
            # 逗點 加連在一起的 '20204020'
            elif format_num == 3:
                ba_partial_formatted_notna.iloc[:, 0] = ['-'.join([str(x)[1:5], str(x)[5:7], str(x)[7:-1]])
                                    for x in ba_partial_formatted_notna.iloc[:, 0] if len(str(x)) == 10]
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
            # 正常的加時間然後往後移一格
            elif format_num == 4:
                ba_partial_formatted_notna.iloc[:, 0] = [str(x)[1:11] for x in ba_partial_formatted_notna.iloc[:, 0]]
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
            # 民國 109-04-01 或 109-4-01 或 109/4/01
            elif format_num == 5:
                ba_partial_formatted_notna.iloc[:, 0] = ['-'.join([str(int(str(x)[:3]) + 1911), str(x)[4:6], str(x)[7:]]) if len(str(x)) > 8
                                   else '-'.join([str(int(str(x)[:3]) + 1911), str(x)[4:5], str(x)[6:]])
                                   for x in ba_partial_formatted_notna.iloc[:, 0]]
                ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
            # 民國但沒有 dash 1090401
            elif format_num == 6:
                ba_partial_formatted_notna.iloc[:, 0] = ['-'.join([str(int(str(x)[:3]) + 1911), str(x)[3:5], str(x)[5:7]]) if len(str(x)) > 5
                                   else str(x)
                                   for x in ba_partial_formatted_notna.iloc[:, 0]]
            ba_partial_formatted_notna.index = pd.to_datetime(ba_partial_formatted_notna.iloc[:, 0])
        except:
            raise ValueError('{} Bank Account Incorrect Data Format　{}'.format(self.Identity, format_num))

        ba_partial_formatted = pd.concat([ba_partial_formatted_notna, ba_partial_formatted_isna], axis=0).sort_index()

        return ba_partial_formatted

    def a_new_hope(self, destination):
        path = destination + '//' + str(self.Identity) + '.xlsx'
        with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
            residual_diff = self.the_empire_strikes_back(writer)
            self.return_of_the_jedi(writer)
            self.the_phantom_menace(writer)
            self.attack_of_the_clones(writer)
            self.revenge_of_the_sith(writer)
        return residual_diff

    def the_empire_strikes_back(self, writer):
        ba_match = self.BankAccountManager['ba_match']
        ba_balance = self.BankAccountManager['ba_balance']
        if not ba_match.empty:
            ba_match = ba_match.loc[:, ['Description', 'Debit', 'Credit']].fillna('N/A')
            ba_match = pd.concat([ba_match['Description'].resample('d').first(), ba_match[['Debit', 'Credit']].resample('d').sum()], axis=1)
            ba_match = ba_match.replace(0, np.nan).dropna(axis=0, how='all')
            ba_match['Comment'] = [None] * ba_match.shape[0]
            ba_match.index = [x.strftime('%Y/%m/%d') for x in list(ba_match.index.to_pydatetime())]
            ba_match.to_excel(writer, sheet_name='Result', startrow=15)
            ba_match_credit_sum = np.sum(ba_match.loc[:, 'Credit'])
            ba_match_debit_sum = np.sum(ba_match.loc[:, 'Debit'])
        else:
            pd.DataFrame().to_excel(writer, sheet_name='Result', startrow=15)
            ba_match_credit_sum = 0
            ba_match_debit_sum = 0
        ba_formatted = self.BankAccountManager['ba_formatted']
        if not ba_formatted.empty:
            ba_formatted_credit_sum = ba_formatted.loc[:, 'Credit'].sum()
            ba_formatted_debit_sum = ba_formatted.loc[:, 'Debit'].sum()
        else:
            ba_formatted_credit_sum = 0
            ba_formatted_debit_sum = 0

        ia_match = self.InternalAccountManager['ia_match']
        ia_balance = self.InternalAccountManager['ia_balance']
        if not ia_match.empty:
            ia_match = ia_match.loc[:, ['Description', 'Debit', 'Credit']].fillna('N/A')
            ia_match = pd.concat([ia_match['Description'].resample('d').first(), ia_match[['Debit', 'Credit']].resample('d').sum()], axis=1)
            ia_match = ia_match.replace(0, np.nan).dropna(axis=0, how='all')
            ia_match['Comment'] = [None] * ia_match.shape[0]
            ia_match.index = [x.strftime('%Y/%m/%d') for x in list(ia_match.index.to_pydatetime())]
            ia_match.to_excel(writer, sheet_name='Result', startrow=15, startcol=6)
            ia_match_credit_sum = ia_match.loc[:, 'Credit'].sum()
            ia_match_debit_sum = ia_match.loc[:, 'Debit'].sum()
        else:
            pd.DataFrame().to_excel(writer, sheet_name='Result', startrow=15, startcol=6)
            ia_match_credit_sum = 0
            ia_match_debit_sum = 0

        ia_formatted = self.InternalAccountManager['ia_formatted']
        if not ia_formatted.empty:
            ia_formatted_credit_sum = ia_formatted.loc[:, 'Credit'].sum()
            ia_formatted_debit_sum = ia_formatted.loc[:, 'Debit'].sum()
        else:
            ia_formatted_credit_sum = 0
            ia_formatted_debit_sum = 0
        b_pending_and_balance = (float(ba_match_debit_sum) + float(ia_balance)) - float(ba_match_credit_sum)
        a_pending_and_balance = (float(ia_match_credit_sum) + float(ba_balance)) - float(ia_match_debit_sum)
        residual_badebit_iacredit = np.round((ba_formatted_debit_sum - ba_match_debit_sum) - (
                    ia_formatted_credit_sum - ia_match_credit_sum), 0)
        residual_bacredit_iadebit = np.round((ba_formatted_credit_sum - ba_match_credit_sum) - (
                    ia_formatted_debit_sum - ia_match_debit_sum), 0)
        residual_diff = np.round(a_pending_and_balance - b_pending_and_balance, 0)

        # create worksheet
        worksheet = writer.sheets['Result']
        workbook = writer.book
        bold = workbook.add_format(
            {'bold': True, 'align': 'center', 'border': 1, 'valign': 'vcenter', 'num_format': '#,###'})
        grid = workbook.add_format({'border': 1, 'num_format': '#,###'})
        to_right = workbook.add_format({'align': 'right', 'border': 1, 'num_format': '#,###'})
        to_center = workbook.add_format({'align': 'center', 'border': 1, 'num_format': '#,###'})
        update = workbook.add_format({'bold': True, 'bg_color': 'yellow', 'border': 1, 'num_format': '#,###'})
        green_light = workbook.add_format({'bold': True, 'bg_color': 'green', 'border': 1, 'num_format': '#,###'})

        # upper left side information
        worksheet.autofilter('A16:K16')
        worksheet.write(0, 1, 'Account Owner')
        worksheet.write(0, 2, self.CoverPageManager['account_owner'])
        worksheet.write(1, 1, 'Done by')
        worksheet.write(1, 2, self.CoverPageManager['done_by'])
        worksheet.write(2, 1, 'Validated by')
        worksheet.write(2, 2, self.CoverPageManager['validated_by'])
        worksheet.write(3, 1, 'Entity')
        worksheet.write(3, 2, self.CoverPageManager['entity'])
        worksheet.write(7, 1, 'Closing date')
        era = datetime.datetime.strptime(self.Era[:10], '%Y-%m-%d').strftime('%Y/%m/%d')
        worksheet.write(7, 2, era, update)
        worksheet.write(9, 2, 'Debit')
        worksheet.write(9, 3, 'Credit')
        worksheet.write(10, 1, 'Accounting movements')
        worksheet.write(10, 2, ia_formatted_debit_sum)
        worksheet.write(10, 3, ia_formatted_credit_sum)
        worksheet.write(11, 1, 'Accounting balance')
        if ia_balance >= 0:
            worksheet.write(11, 2, ia_balance)
        else:
            worksheet.write(11, 3, ia_balance)
        worksheet.write(9, 4, 'Accounting Account Number')
        worksheet.write(10, 4, str(self.Identity))

        # upper right side information
        worksheet.write(0, 7, 'Name of  the bank')
        worksheet.write(0, 8, self.CoverPageManager['bank_code'])
        worksheet.write(2, 7, 'Currency')
        worksheet.write(2, 8, self.CoverPageManager['currency'])
        worksheet.write(3, 10, 'All are analized ? (Yes/No)')
        worksheet.write(4, 10, self.CoverPageManager['analized'])
        worksheet.write(6, 10, 'All are regularized ? (Yes/No)')
        worksheet.write(7, 10, self.CoverPageManager['regularized'])
        worksheet.write(7, 7, 'Residual Difference')
        worksheet.write(7, 8, residual_diff, update)
        worksheet.write(8, 7, residual_badebit_iacredit, update)
        worksheet.write(8, 8, residual_bacredit_iadebit, update)
        worksheet.write(9, 8, 'Debit')
        worksheet.write(9, 9, 'Credit')
        worksheet.write(10, 7, 'Bank movements')
        worksheet.write(10, 8, ba_formatted_debit_sum)
        worksheet.write(10, 9, ba_formatted_credit_sum)
        worksheet.write(11, 7, 'Bank balance')
        if ba_balance >= 0:
            worksheet.write(11, 9, ba_balance)
        else:
            worksheet.write(11, 8, ba_balance)
        worksheet.write(9, 10, 'Banking Account Number')
        worksheet.write(10, 10, self.CoverPageManager['banking_acct_number'])

        # upper information format
        worksheet.conditional_format('C1:E12', {'type': 'no_blanks', 'format': to_center})
        worksheet.conditional_format('I1:K12', {'type': 'no_blanks', 'format': to_center})
        worksheet.conditional_format('B1:B12', {'type': 'no_blanks', 'format': to_right})
        worksheet.conditional_format('H1:H12', {'type': 'no_blanks', 'format': to_right})

        # main frame header
        worksheet.write(15, 0, 'Date', bold)
        worksheet.write(15, 1, 'Description', bold)
        worksheet.write(15, 2, 'Debit', bold)
        worksheet.write(15, 3, 'Credit', bold)
        worksheet.write(15, 4, 'Comment', bold)
        
        worksheet.write(15, 6, 'Date', bold)
        worksheet.write(15, 7, 'Description', bold)
        worksheet.write(15, 8, 'Debit', bold)
        worksheet.write(15, 9, 'Credit', bold)
        worksheet.write(15, 10, 'Comment', bold)
        worksheet.merge_range('A14:E15', 'Amounts in bank statement and not in the accounting', bold)
        worksheet.merge_range('G14:K15', 'Amounts in the accounting and not in the bank statement', bold)

        # main frame footer
        worksheet.merge_range('A51:B51', 'Total', bold)
        worksheet.write(50, 2, ba_match_debit_sum, bold)
        worksheet.write(50, 3, ba_match_credit_sum, bold)
        
        worksheet.merge_range('G51:H51', 'Total', bold)
        worksheet.write(50, 8, ia_match_debit_sum, bold)
        worksheet.write(50, 9, ia_match_credit_sum, bold)

        worksheet.write(53, 0, '(b)', grid)
        worksheet.write(53, 1, 'Pending amounts + balance :', grid)
        if b_pending_and_balance >= 0:
            worksheet.write(53, 2, b_pending_and_balance, update)
            worksheet.write(53, 3, 0, update)
        else:
            worksheet.write(53, 2, 0, update)
            worksheet.write(53, 3, b_pending_and_balance * -1, update)

        worksheet.write(53, 6, '(a)', grid)
        worksheet.write(53, 7, 'Pending amounts + balance :', grid)
        if a_pending_and_balance < 0:
            worksheet.write(53, 8, a_pending_and_balance * -1, update)
            worksheet.write(53, 9, 0, update)
        else:
            worksheet.write(53, 8, 0, update)
            worksheet.write(53, 9, a_pending_and_balance, update)

        # main frame format
        worksheet.conditional_format('A16:E51', {'type': 'no_errors', 'format': grid})
        worksheet.conditional_format('G16:K51', {'type': 'no_errors', 'format': grid})

        # highlight the backward elimination
        if self.CaveatBankDebitInternalCredit:
            for info in self.CaveatBankDebitInternalCredit:
                date = info[1]
                if date in list(ba_match.index):
                    ba_debit_idx = int(list(ba_match.index).index(date))
                    worksheet.conditional_format('C{}:C{}'.format(ba_debit_idx + 17, ba_debit_idx + 17),
                                                  {'type': 'no_errors', 'format': green_light})
                if date in list(ia_match.index):
                    ia_credit_idx = int(list(ia_match.index).index(date))
                    worksheet.conditional_format('J{}:J{}'.format(ia_credit_idx + 17, ia_credit_idx + 17),
                                                  {'type': 'no_errors', 'format': green_light})
        if self.CaveatBankCreditInternalDebit:
            for info in self.CaveatBankCreditInternalDebit:
                date = info[1]
                if date[1] in list(ba_match.index):
                    ba_credit_idx = int(list(ba_match.index).index(date))
                    worksheet.conditional_format('D{}:D{}'.format(ba_credit_idx + 17, ba_credit_idx + 17),
                                                  {'type': 'no_errors', 'format': green_light})
                if date in list(ia_match.index):
                    ia_debit_idx = int(list(ia_match.index).index(date))
                    worksheet.conditional_format('I{}:I{}'.format(ia_debit_idx + 17, ia_debit_idx + 17),
                                                  {'type': 'no_errors', 'format': green_light})

        # page format
        worksheet.set_column(0, 0, 13)
        worksheet.set_column(1, 1, 30)
        worksheet.set_column(2, 3, 15)
        worksheet.set_column(4, 4, 30)
        worksheet.set_column(6, 6, 13)
        worksheet.set_column(7, 7, 30)
        worksheet.set_column(8, 9, 15)
        worksheet.set_column(10, 10, 30)

        worksheet.set_tab_color('green')
        worksheet.set_landscape()
        worksheet.center_horizontally()
        worksheet.print_area('A1:K54')
        worksheet.fit_to_pages(1, 1)
        worksheet.set_paper(9)
        return residual_diff

    def return_of_the_jedi(self, writer):
        diff = self.DifferenceDailyAmount
        diff.index = [x.strftime('%Y/%m/%d') for x in list(diff.index.to_pydatetime())]
        diff.to_excel(writer, sheet_name='Check', startrow=2)
        combine = self.CombineDailyAmount
        combine.index = [x.strftime('%Y/%m/%d') for x in list(combine.index.to_pydatetime())]
        combine.to_excel(writer, sheet_name='Check', startcol=4, startrow=2)

        # create worksheet
        worksheet = writer.sheets['Check']
        workbook = writer.book
        bold = workbook.add_format(
            {'bold': True, 'align': 'center', 'border': 1, 'valign': 'vcenter', 'num_format': '#,###'})
        grid = workbook.add_format({'border': 1, 'num_format': '#,###'})
        # main frame header
        worksheet.write(2, 0, 'Date', bold)
        worksheet.write(2, 4, 'Date', bold)
        worksheet.merge_range('A1:C2', 'Difference between Bank and Account', bold)
        worksheet.merge_range('E1:E2', 'Daily Balance', bold)
        worksheet.merge_range('F1:G2', 'Bank Statement', bold)
        worksheet.merge_range('H1:I2', 'Account', bold)

        # format page
        worksheet.set_column(0, 0, 20)
        worksheet.set_column(1, 2, 30)
        worksheet.set_column(4, 4, 20)
        worksheet.set_column(5, 8, 20)
        worksheet.conditional_format('A1:I100', {'type': 'no_blanks', 'format': grid})

    def the_phantom_menace(self, writer):
        ba_match = self.BankAccountManager['ba_match']
        if not ba_match.empty:
            ba_match.index = [x.strftime('%Y/%m/%d') for x in list(ba_match.index.to_pydatetime())]
        ba_match.to_excel(writer, sheet_name='Match Transactions', startrow=1)

        ia_match = self.InternalAccountManager['ia_match']
        ia_match.index = [x.strftime('%Y/%m/%d') for x in list(ia_match.index.to_pydatetime())]
        ia_match = ia_match.iloc[:, [1, 0, 2, 3, 4, 5]]
        ia_match.to_excel(writer, sheet_name='Match Transactions', startrow=1, startcol=7)

        # create worksheet
        worksheet = writer.sheets['Match Transactions']
        workbook = writer.book
        bold = workbook.add_format(
            {'bold': True, 'align': 'center', 'border': 1, 'valign': 'vcenter', 'num_format': '#,###'})
        grid = workbook.add_format({'border': 1, 'num_format': '#,###'})

        # main frame header
        worksheet.merge_range('A1:F1', 'Amounts in bank statement and not in the accounting', bold)
        worksheet.merge_range('H1:N1', 'Amounts in the accounting and not in the bank statement', bold)
        worksheet.write(1, 0, 'Date', bold)
        worksheet.write(1, 7, 'Date', bold)

        # main frame format
        row_num_all = max(ba_match.shape[0], ia_match.shape[0]) + 2
        worksheet.conditional_format('A3:F{}'.format(row_num_all), {'type': 'no_errors', 'format': grid})
        worksheet.conditional_format('H3:N{}'.format(row_num_all), {'type': 'no_errors', 'format': grid})

        # page format
        worksheet.set_column(0, 0, 15)
        worksheet.set_column(1, 2, 20)
        worksheet.set_column(3, 3, 30)
        worksheet.set_column(4, 5, 20)
        worksheet.set_column(7, 7, 15)
        worksheet.set_column(8, 9, 20)
        worksheet.set_column(10, 12, 30)
        worksheet.set_column(13, 13, 20)
        worksheet.autofilter('A2:N2')

    def attack_of_the_clones(self, writer):
        summary = list()
        # summary info
        summary.append(['Account Code', self.Identity])
        summary.append(['Process Start At', self.BattleBegins.strftime('%Y-%d-%m %H:%M:%S')])
        summary.append(['Process End At', self.BattleEnds.strftime('%Y-%d-%m %H:%M:%S')])
        summary.append(['Time to Process (Seconds)', np.round(self.BattleDuration.total_seconds(), 4)])
        ba_balance = self.BankAccountManager['ba_balance']

        # account info
        summary.append(['Number of Transaction in Bank Account', self.BankAccountManager['ba_formatted'].shape[0]])
        summary.append(['Bank Account Balance', ba_balance])
        ia_balance = self.InternalAccountManager['ia_balance']
        summary.append(['Number of Transaction in Internal Account', self.InternalAccountManager['ia_formatted'].shape[0]])
        summary.append(['Internal Account Balance', ia_balance])
        summary.append(['', ''])

        # residual info
        ba_match = self.BankAccountManager['ba_match']
        if not ba_match.empty:
            ba_match_credit_sum = np.sum(ba_match.loc[:, 'Credit'])
            ba_match_debit_sum = np.sum(ba_match.loc[:, 'Debit'])
        else:
            ba_match_credit_sum = 0
            ba_match_debit_sum = 0
        ba_formatted = self.BankAccountManager['ba_formatted']
        if not ba_formatted.empty:
            ba_formatted_credit_sum = ba_formatted.loc[:, 'Credit'].sum()
            ba_formatted_debit_sum = ba_formatted.loc[:, 'Debit'].sum()
        else:
            ba_formatted_credit_sum = 0
            ba_formatted_debit_sum = 0

        ia_match = self.InternalAccountManager['ia_match']
        if not ia_match.empty:
            ia_match_credit_sum = ia_match.loc[:, 'Credit'].sum()
            ia_match_debit_sum = ia_match.loc[:, 'Debit'].sum()
        else:
            ia_match_credit_sum = 0
            ia_match_debit_sum = 0
        
        ia_formatted = self.InternalAccountManager['ia_formatted']
        if not ia_formatted.empty:
            ia_formatted_credit_sum = ia_formatted.loc[:, 'Credit'].sum()
            ia_formatted_debit_sum = ia_formatted.loc[:, 'Debit'].sum()
        else:
            ia_formatted_credit_sum = 0
            ia_formatted_debit_sum = 0
        summary.append(['Bank Account Credit Movement', ba_formatted_credit_sum])
        summary.append(['Bank Account Credit Other Movement', ba_match_credit_sum])
        summary.append(['Internal Account Credit Movement', ia_formatted_credit_sum])
        summary.append(['Internal Account Credit Other Movement', ia_match_credit_sum])
        summary.append(['', ''])
        summary.append(['Bank Account Debit Movement', ba_formatted_debit_sum])
        summary.append(['Bank Account Debit Other Movement', ba_match_debit_sum])
        summary.append(['Internal Account Debit Movement', ia_formatted_debit_sum])
        summary.append(['Internal Account Debit Other Movement', ia_match_debit_sum])

        b_pending_and_balance = (ba_match_debit_sum + ia_balance) - ba_match_credit_sum
        a_pending_and_balance = (ia_match_credit_sum + ba_balance) - ia_match_debit_sum
        residual_diff = np.round(a_pending_and_balance - b_pending_and_balance, 0)
        summary.append(['', ''])
        summary.append(['Residual Difference (Should be 0)', residual_diff])

        # convert to table
        summary_df = pd.DataFrame(summary)
        summary_df.columns = ['Item', 'Figure']
        summary_df.to_excel(writer, sheet_name='Process Summary', index=None, startrow=1)

        # create worksheet
        worksheet = writer.sheets['Process Summary']
        workbook = writer.book
        bold = workbook.add_format(
            {'bold': True, 'align': 'center', 'border': 1, 'valign': 'vcenter', 'num_format': '#,###'})
        outer_num = workbook.add_format({'top': 1, 'bottom': 1, 'left': 1, 'right': 1, 'num_format': '#,###'})
        outer = workbook.add_format({'top': 1, 'bottom': 1, 'left': 1, 'right': 1})

        # main frame format
        worksheet.merge_range('A1:B1', 'Process Summary', bold)
        worksheet.conditional_format('A3:B6', {'type': 'no_errors', 'format': outer})
        worksheet.conditional_format('A7:B11', {'type': 'no_errors', 'format': outer_num})
        worksheet.conditional_format('A12:B22', {'type': 'no_errors', 'format': outer_num})

        # page format
        worksheet.set_tab_color('red')
        worksheet.set_column(0, 0, 40)
        worksheet.set_column(1, 1, 25)

        # add caveat
        col_cum = 0
        if self.CaveatBankDebitInternalCredit:
            bd_ic = pd.DataFrame(self.CaveatBankDebitInternalCredit).iloc[:, 1:]
            bd_ic.columns = ['Date', 'Imbalance Value', 'Message', 'Original Bank Trans #', 'Match Bank Trans #',
                             'Original Internal Trans #', 'Match Internal Trans #']
            bd_ic.to_excel(writer, sheet_name='Process Summary', index=None, startrow=1, startcol=3)
            col_cum += bd_ic.shape[0] + 4
            worksheet.merge_range('D1:J1', 'Caveat :: Bank Debit - Internal Credit', bold)
            worksheet.set_column(3, 9, 25)
        if self.CaveatBankCreditInternalDebit:
            bc_id = pd.DataFrame(self.CaveatBankCreditInternalDebit).iloc[:, 1:]
            bc_id.columns = ['Date', 'Imbalance Value', 'Message', 'Original Bank Trans #', 'Match Bank Trans #',
                             'Original Internal Trans #', 'Match Internal Trans #']
            bc_id.to_excel(writer, sheet_name='Process Summary', index=None, startrow=1 + col_cum, startcol=3)
            worksheet.merge_range('D{}:J{}'.format(col_cum + 1, col_cum + 1), 'Caveat :: Bank Credit - Internal Debit', bold)
            worksheet.set_column(3, 9, 25)

    def revenge_of_the_sith(self, writer):
        ba_formatted = self.BankAccountManager['ba_formatted']
        if not ba_formatted.empty:
            ba_formatted.index = [x.strftime('%Y/%m/%d') for x in list(ba_formatted.index.to_pydatetime())]
        ba_formatted.to_excel(writer, sheet_name='Bank Account')
        ia_formatted = self.InternalAccountManager['ia_formatted']
        ia_formatted.index = [x.strftime('%Y/%m/%d') for x in list(ia_formatted.index.to_pydatetime())]
        ia_formatted.to_excel(writer, sheet_name='Internal Account')

        ba_raw = self.BankAccountManager['ba_raw']
        ba_raw.to_excel(writer, sheet_name='Bank Account Raw', header=None, index=None)
        ia_raw = self.InternalAccountManager['ia_raw']
        ia_raw.to_excel(writer, sheet_name='Internal Account Raw', index=None)
        # create worksheet
        worksheet1 = writer.sheets['Bank Account']
        worksheet2 = writer.sheets['Internal Account']
        worksheet3 = writer.sheets['Bank Account Raw']
        worksheet4 = writer.sheets['Internal Account Raw']
        workbook = writer.book
        grid = workbook.add_format({'border': 1, 'num_format': '#,###'})

        # page format
        worksheet1.conditional_format('A1:F{}'.format(ba_formatted.shape[0] + 2), {'type': 'no_errors', 'format': grid})
        worksheet1.set_column(0, 0, 15)
        worksheet1.set_column(1, 2, 20)
        worksheet1.set_column(3, 3, 30)
        worksheet1.set_column(4, 5, 20)
        worksheet1.autofilter('A1:F1')

        worksheet2.conditional_format('A1:G{}'.format(ia_formatted.shape[0] + 2), {'type': 'no_errors', 'format': grid})
        worksheet2.set_column(0, 0, 15)
        worksheet2.set_column(1, 2, 20)
        worksheet2.set_column(3, 5, 30)
        worksheet2.set_column(6, 6, 20)
        worksheet2.autofilter('A1:G1')

        worksheet3.set_column(0, 20, 10)

        worksheet4.set_column(0, 20, 10)

'''
███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
Aggregate Object: MotherShip
███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
'''


class MotherShip:
    def __init__(self, config_path, open_switch):
        # attributes
        self.Body = None
        self.ConfigPath = config_path
        self.OpenSwitch = open_switch
        self.Apocalypse = None
        # commander room
        self.General = None
        self.ConfigCommander = None
        self.CoverCommander = None
        self.InternalAccountCommander = None
        self.BAFormatSergeant = None
        self.IABalanceSergeant = None
        # information to import
        self.InternalAccountPlanet = None
        self.BankAccountPlanet = None
        # information to output
        self.Mars = None
        # cabin
        self.Onboard = []
        self.Generation1SnapShot = []
        self.Success = []
        # caveat
        self.MasterCaveatBankCreditInternalDebit = list()
        self.MasterCaveatBankDebitInternalCredit = list()
        # graveyard
        self.CoverDead = []
        self.IABalanceDead = []
        self.IATransactionDead = []
        self.IAResampleDead = []
        self.MissingYodaBA = []
        self.BAFormatOrBalanceDead = []
        self.LightSaberLost = []
        self.WisdomLost = []
        self.CreditCrush = []
        self.DebitCrush = []
        self.BAResampleDead = []

    '''
    ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    Function to Call All the Functions
    ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    '''
    def summon_child(self):
        """
        This is to conclude all the functions of the MotherShip, to be called in once

        :return
        None


        * Get Assigned:
       self.Onboard: list, all current yoda onboard
        """
        print('* Step 1 - Roll Call')
        self.roll_call()
        print('>>>>>Successfully Loaded {} Files in Step 1 - Configuration Setup\n'.format(len(self.Onboard)))
        print('* Step 2 - Cover Page')
        self.Onboard = self.get_cover()
        print('>>>>>Successfully Loaded {} Files in  Step 2 - Cover Page\n'.format(len(self.Onboard)))
        print('* Step 3 - Internal Account')
        self.Onboard = self.get_internal_account()
        print('>>>>>Successfully Loaded {} Files in Step 3 - Internal Account\n'.format(len(self.Onboard)))
        print('* Step 4 - Bank Account')
        self.Onboard = self.get_bank_account()
        print('>>>>>Successfully Loaded {} Files in Step 4 - Bank Account\n'.format(len(self.Onboard)))
    '''
    ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    All the Functions
    ███████████████████████████████████████████████████████████████████████████████████████████████████████████████████
    '''
    ##################################################################################################################
    def roll_call(self):
        """
        This is to decide which account to be call and import the config.xlsx

        :return
        None


        * Get Assigned:
        self.Body: list, all sheets of Config.xlsx,
        self.ConfigCommander: DataFrame, Configuration Sheet of Config.xlsx,
        self.Mars: str, Output Destination Directory,
        self.Apocalypse: str, End of the Month,
        self.General: DataFrame, Order Sheet of Config.xlsx,
        self.Onboard: list, Current Yoda onboard,
        self.Generation1SnapShot: list, Initial Yoda identity

        yoda.Identity: str, account code of yoda
        yoda.Status: str, yoda is dead or alive
        yoda.Summoned: bool, to be called or not
        """
        # get account directory and folder
        self.Body = pd.read_excel(self.ConfigPath, sheet_name=None)
        self.ConfigCommander = self.Body['Configuration']
        self.Mars = self.ConfigCommander.loc[0, 'Output Folder']
        self.Apocalypse = str(self.ConfigCommander.loc[0, 'End of Month'])
        # get bank account open
        self.General = self.Body['Order']
        self.General.columns = self.General.iloc[0, :]
        self.General = self.General[1:]
        self.General.loc[:, 'Account Code'] = self.General.loc[:, 'Account Code'].astype('int')
        self.General.set_index('Account Code', inplace=True)
        # instantiate yoda
        generals_order = self.General[self.General[self.OpenSwitch] == 'V']
        for identity, order in generals_order.iterrows():
            yoda = Yoda()
            yoda.Summoned = True
            yoda.Identity = identity
            yoda.Status = 'Alive'
            yoda.Era = self.Apocalypse
            self.Onboard.append(yoda)
            self.Generation1SnapShot.append(yoda)

    ##################################################################################################################
    def get_cover(self):
        """
        This function is to parse cover page information for all yoda

        :return:
        new_board: Survived Yoda onboard after cover battle


        * Get Assigned:
        yoda.CoverPageManager: dict, all cover page information of yoda

        self.CoverDead: Cover Import Failed
        """
        self.CoverCommander = self.Body['Cover Information']    # get all cover information
        new_board = []
        for yoda in self.Onboard:
            try:
                yoda.CoverPageManager = yoda.import_cover(self.CoverCommander, self.Apocalypse)
            except:
                self.CoverDead.append(yoda)
                yoda.Status = 'Dead'
                continue
            new_board.append(yoda)
        return new_board

    ##################################################################################################################
    def get_internal_account(self):
        """
        This is to import internal account for all yoda, ready to be processed

        :return:
        new_board: Survived Yoda onboard after internal account battle


        * Get Assigned:
        self.InternalAccountPlanet: the file address of internal account
        self.IABalanceSergeant: balance sheet of internal account
        yoda.InternalAccountManager['ia_balance']: int, balance of the internal account of yoda
        self.InternalAccountCommander: all internal account transactions
        yoda.InternalAccountManager['ia_raw']: original internal account transactions of yoda
        yoda.InternalAccountManager['ia_formatted']: formatted internal account transactions of yoda
        yoda.InternalAccountManager['ia_daily_amount_only']: resampled daily amount of internal account of yoda

        self.IABalanceDead: Balance Parse Failed
        self.IATransactionDead: Transactions Import Failed
        self.IAResampleDead: Transactions Resample Failed


        * Error Raised:
        ValueError('Internal Account Balance Does Not Exist')
        ValueError('Internal Account Balance Table Formation is Incorrect')
        ValueError('{} Internal Account Balance Parse Fail'.format(yoda.Identity))
        ValueError('Internal Account Transaction Does Not Exist')
        ValueError('Internal Account Transaction Table Format is Incorrect')
        """
        # directory to the internal account file & internal account file
        internal_galaxy = self.ConfigCommander.loc[0, 'Internal Account Folder']
        internal_system = self.ConfigCommander.loc[0, 'Internal Account File Name']
        self.InternalAccountPlanet = internal_galaxy + '//' + internal_system
        # all transactions to the internal account
        try:
            self.InternalAccountCommander = pd.read_excel(self.InternalAccountPlanet, sheet_name='Transaction')
        except FileNotFoundError:
            raise ValueError('Internal Account Transaction Does Not Exist')
        try:
            self.InternalAccountCommander = self.InternalAccountCommander[:-1]      # This is to skip the last row
            self.InternalAccountCommander.iloc[:, 1] = self.InternalAccountCommander.iloc[:, 1].astype('int').astype('str')
        except:
            raise ValueError('Internal Account Transaction Table Format is Incorrect')
        ###################################################################################################
        # transactions to yoda
        new_board = []
        for yoda in self.Onboard:
            yoda.InternalAccountManager['ia_raw'] = yoda.parse_internal_account_transaction(self.InternalAccountCommander)
            if yoda.InternalAccountManager['ia_raw'].empty:
                yoda.InternalAccountManager['ia_formatted'] = pd.DataFrame(columns=['Credit', 'Debit', 'Subject Name', 'Trans Number', 'Description','Index'], index=[pd.to_datetime(self.Apocalypse)]*2)
            else:
                yoda.InternalAccountManager['ia_formatted'] = yoda.format_internal_account_transaction(yoda.InternalAccountManager['ia_raw'])
            yoda.InternalAccountManager['ia_daily_amount_only'] = yoda.resample_internal_account_sum(yoda.InternalAccountManager['ia_formatted'])
            # except:
            #     self.IATransactionDead.append(yoda)
            #     yoda.Status = 'Dead'
            #     continue
            # try:
            
            # except:
            #     self.IAResampleDead.append(yoda)
            #     yoda.Status = 'Dead'
            #     continue
            new_board.append(yoda)

        ###################################################################################################
        # all balance to the internal account
        try:
            self.IABalanceSergeant = pd.read_excel(self.InternalAccountPlanet, sheet_name='Balance')
        except FileNotFoundError:
            raise ValueError('Internal Account Balance Does Not Exist')
        try:
            self.IABalanceSergeant = self.IABalanceSergeant.iloc[:-2]
            self.IABalanceSergeant['Account Code'] = self.IABalanceSergeant.iloc[:, 1].astype('int')
        except:
            raise ValueError('Internal Account Balance Table Formation is Incorrect')
        ###################################################################################################
        # balance to yoda
        new_board2 = []
        for yoda in new_board:
            # try:
            yoda.InternalAccountManager['ia_balance'] = float(yoda.parse_internal_account_balance(self.IABalanceSergeant))
            # except:
            #     raise ValueError('{} Internal Account Balance Parse Fail'.format(yoda.Identity))
            if yoda.InternalAccountManager['ia_balance'] or (yoda.InternalAccountManager['ia_balance'] == 0.0):
                new_board2.append(yoda)
            else:
                self.IABalanceDead.append(yoda)
                yoda.Status = 'Dead'
                continue
        ###################################################################################################
        return new_board2

    ##################################################################################################################
    def get_bank_account(self):
        """
        This is to import bank account for all yoda, ready to be processed

        :return:
        new_board: Survived Yoda onboard after bank account battle

         * Get Assigned:
        self.BankAccountPlanet: str, All bank account directory
        self.BAFormatSergeant: DataFrame, All bank account format
        yoda.BankAccountManager['FormatManager']: DataFrame, Bank account format of yoda

        self.MissingYodaBA: list, empty bank account yoda, it should at least includes header
        self.BAFormatOrBalanceDead: list, yoda whose bank account failed to be formatted or parsed balance
        self.BAResampleDead: list, yoda whose bank account failed to be resampled to day


        * Error Raised:
        ValueError('{} Bank Account Format Is Not Found'.format(yoda.Identity))
        """
        # bank account directory
        self.BankAccountPlanet = self.ConfigCommander['Bank Statement Folder'][0]
        # format to import bank account
        self.BAFormatSergeant = self.Body['Bank Account Format']
        self.BAFormatSergeant.columns = self.BAFormatSergeant.iloc[0, :]
        self.BAFormatSergeant = self.BAFormatSergeant[1:]
        self.BAFormatSergeant.loc[:, 'Account Code'] = self.BAFormatSergeant.loc[:, 'Account Code'].astype('int')
        new_board = []
        for yoda in self.Onboard:
            print(yoda.Identity)
            yoda.BankAccountManager['FormatManager'] = self.BAFormatSergeant[self.BAFormatSergeant['Account Code'] == yoda.Identity]
            if yoda.BankAccountManager['FormatManager'].empty:
                raise ValueError('{} Bank Account Format Is Not Found'.format(yoda.Identity))
            yoda.BankAccountManager['ba_raw'] = yoda.import_bank_account_transaction(self.BankAccountPlanet)
            if yoda.BankAccountManager['ba_raw'].empty:
                self.MissingYodaBA.append(yoda)
                yoda.Status = 'Missing'
                continue
            # try:
            yoda.BankAccountManager['ba_formatted'] = yoda.format_bank_account_transaction(yoda.BankAccountManager['ba_raw'])
            yoda.BankAccountManager['ba_balance'] = float(yoda.parse_bank_account_balance(yoda.BankAccountManager['ba_formatted']))
            # except:
            #     self.BAFormatOrBalanceDead.append(yoda)
            #     yoda.Status = 'Dead'
            #     continue
            # try:
            yoda.BankAccountManager['ba_daily_amount_only'] = yoda.resample_bank_account_sum(yoda.BankAccountManager['ba_formatted'])
            # except:
            #     self.BAResampleDead.append(yoda)
            #     yoda.Status = 'Dead'
            #     continue
            new_board.append(yoda)
        return new_board
