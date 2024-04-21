import csv
import pandas as pd
import os

class FileCompare:
    def __init__(self, path, lfile, rfile, commonkey=None, separator=','):
        self.path = path
        if not check_files(path, lfile):
            raise FileNotFoundError(f'Looks like either path {self.path} or {lfile} is missing.')
        self.lfile = lfile
        if not check_files(path, rfile):
            raise FileNotFoundError(f'Looks like either path {self.path} or {rfile} is missing.')
        self.rfile = rfile
        if not commonkey:
            raise Exception('Please enter a common field that can be used for comparison.')
        elif len(commonkey) == 1:
            self.keys = ','.join(commonkey)
        else:
            self.keys = commonkey
        self.separator = separator

    def __str__(self):
        return f'{self.path}\n{self.lfile}\n{self.rfile}\n{self.keys}\n{self.separator}'

    def validate_files(self):
        CompLeft = os.path.join(self.path, self.lfile)
        CompRight = os.path.join(self.path, self.rfile)
        dfLeft = pd.read_csv(CompLeft, sep=self.separator, dtype=str)
        dfRight = pd.read_csv(CompRight, sep=self.separator, dtype=str)
        headerLeft = dfLeft.columns.to_list()
        headerRight = dfRight.columns.to_list()
        FinalHeader = []
        if headerLeft == headerRight:
            for val in headerRight:
                FinalHeader.append(f'{val.strip()}.[{self.lfile.split('/')[-1].split('.')[0]}]')
                FinalHeader.append(f'{val.strip()}.[{self.rfile.split('/')[-1].split('.')[0]}]')
            return dfLeft, dfRight, FinalHeader
        else:
            raise KeyError('File headers are not matching')

    def common_records(self):
        df1, df2, fheader = self.validate_files()
        common_df = pd.merge(df1, df2, how='inner', on=self.keys)
        if len(common_df)>1:
            common_df.to_csv(f'{self.path}/common_records_{self.lfile.split('/')[-1].split('.')[0]}_'
                             f'{self.rfile.split('/')[-1].split('.')[0]}.csv', sep=',', quoting=csv.QUOTE_ALL, index=False)
        else:
            print(f"No common records found based on the given key, {self.keys}")

    # for difference records including unqiue records
    def difference_records(self):
        df1, df2, header = self.validate_files()
        lName = self.lfile.split('/')[-1].split('.')[0]
        rName = self.rfile.split('/')[-1].split('.')[0]
        merge_df = pd.merge(df1, df2, how='outer', on=self.keys,indicator=True, suffixes=[f'.[{lName}]',f'.[{rName}]'])
        # records only in left file
        left_df = merge_df[merge_df['_merge'] == 'left_only'].drop(columns='_merge')
        if len(left_df) > 0:
            left_df.to_csv( f'{self.path}/{lName}_only_records.csv', index=False, sep=',', quoting=csv.QUOTE_ALL)
        else:
            print(f'No distinct records found in {self.lfile.split('/')[-1]}')
        # records only in right file
        right_df = merge_df[merge_df['_merge'] == 'right_only'].drop(columns='_merge')
        if len(right_df) > 0:
            right_df.to_csv(f'{self.path}/{rName}_only_records.csv', index=False, quoting=csv.QUOTE_ALL)
        else:
            print(f'No distinct records found in {self.rfile.split('/')[-1]}')
        # common records
        common_keys = set(df1[self.keys]).intersection(df2[self.keys])
        diff_df = pd.DataFrame()
        # Iterate over common key values and compare corresponding rows
        for key in common_keys:
            df1_row = df1[df1[self.keys] == key].iloc[0]
            df2_row = df2[df2[self.keys] == key].iloc[0]
            if not df1_row.equals(df2_row):
                diff_row = {col + f'.[{lName}]': df1_row[col] for col in df1.columns}
                diff_row.update({col + f'.[{rName}]': df2_row[col] for col in df2.columns})
                diff_row = {k: [v] for k, v in diff_row.items()}
                diff_df = pd.concat([diff_df, pd.DataFrame(diff_row)], ignore_index=True)
                diff_df = diff_df[header]
        if len(diff_df) > 0:
            diff_df.to_csv(f'{self.path}/diference_in_{lName}_{rName}.csv', index=False, quoting=csv.QUOTE_ALL, sep=',')
        else:
            print(f"Files {lName} and {rName} are identical")

    def full_compare(self):
        self.common_records()
        self.difference_records()


def check_files(path, file):
    return os.path.isfile(f'{path}/{file}')

def main():
    """
    path  --> provide the path where files are present and also output files will be created here
    lfile  --> file that needs to be compared
    rfile --> another file that needs to be compared
    commonkey --> one key column that is common in both the file that can be used for comparison
    separator --> file delimiter
    """
    configdetails = {
        'path': '/example/',
        'lfile': 'exp1.csv',
        'rfile': 'exp2.csv',
        'commonkey': ['key1'],
        'separator': '|'
    }
    comparefiles  = FileCompare(**configdetails)
    # for printing the class FileCompare
    print(comparefiles)
    # for extracting only common records
    comparefiles.common_records()
    # for extracting distinct or diff records
    comparefiles.difference_records()
    # for end to end
    comparefiles.full_compare()



if __name__ == '__main__':
    main()
