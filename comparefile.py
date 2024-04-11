import pandas as pd

# for common records
def common_records(df1, df2, path, keys):
    #common recrods
    common_df = pd.merge(df1, df2, how='inner', on=keys)
    common_df.to_csv(path + '/' + 'common_elements.csv', index=False)

#for difference records including unqiue records
def difference_records(df1, df2, path, keys, header):
    merge_df = pd.merge(df1, df2, how='outer', on=keys, indicator=True)
    #records only in left file
    left_df = merge_df[merge_df['_merge'] == 'left_only'].drop(columns='_merge')
    left_df.to_csv(path + '/' + 'left_only.csv', index=False)
    # records only in right file
    right_df = merge_df[merge_df['_merge'] == 'right_only'].drop(columns='_merge')
    right_df.to_csv(path + '/' + 'right_only.csv', index=False)
    #common records
    common_keys = set(df1[keys]).intersection(df2[keys])
    diff_df = pd.DataFrame()
    # Iterate over common key values and compare corresponding rows
    for key in common_keys:
        df1_row = df1[df1[keys] == key].iloc[0]
        df2_row = df2[df2[keys] == key].iloc[0]
        if not df1_row.equals(df2_row):
            diff_row = {col + '_file1': df1_row[col] for col in df1.columns}
            diff_row.update({col + '_file2': df2_row[col] for col in df2.columns})
            diff_row = {k: [v] for k, v in diff_row.items()}
            diff_df = pd.concat([diff_df, pd.DataFrame(diff_row)], ignore_index=True)
            diff_df = diff_df[header]
            diff_df.to_csv(path + '/' + 'differences.csv', index=False)


#Enter file path
path = ''
#File to compare
CompLeft = f'{path}/employees1.csv'
#File to compare with
CompRight = f'{path}/employees.csv'
#key columns to compare using
KeyColumn = ['emp_no', 'emp_id']
CommonCol = ','.join(KeyColumn)
# Read the two files into pandas DataFrames with file seperator as '|
dfLeft = pd.read_csv(CompLeft, sep='|')
dfRight = pd.read_csv(CompRight, sep='|')

headerLeft = dfRight.columns.to_list()
headerRight = dfRight.columns.to_list()
FinalHeader = []
if headerLeft == headerRight:
    for val in headerRight:
        FinalHeader.append(f'{val.strip()}_file1')
        FinalHeader.append(f'{val.strip()}_file2')
    common_records(dfLeft, dfRight, path, CommonCol)
    difference_records(dfLeft, dfRight, path, CommonCol, FinalHeader)

