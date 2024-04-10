import pandas as pd

#Enter you file path
path = ''
#File to compare
CompFile1 = f'{path}/employees1.csv'
#File to compare with
CompFile2 = f'{path}/employees.csv'
#key columns to compare using
CommonColumn = ['emp_no']
# Read the two files into pandas DataFrames with file seperator as '|
dfFile1 = pd.read_csv(CompFile1, sep='|')
dfFile2 = pd.read_csv(CompFile2, sep='|')

listheader1 = list(dfFile1.columns.values)
listheader2 = list(dfFile2.columns.values)
listheader = []
if listheader2 == listheader1:
    for val in listheader1:
        listheader.append(f'{val}_file1')
        listheader.append(f'{val}_file2')
    # Merge the DataFrames to find common elements
    common_df = pd.merge(dfFile1, dfFile2, how='inner', on='emp_no')

    # Find differences

    # Get common key values
    common_keys = set(dfFile1['emp_no']).intersection(dfFile2['emp_no'])

    # Create an empty DataFrame to store the differences
    diff_df = pd.DataFrame()

    # Iterate over common key values and compare corresponding rows
    for key in common_keys:
        df1_row = dfFile1[dfFile1['emp_no'] == key].iloc[0]
        df2_row = dfFile2[dfFile2['emp_no'] == key].iloc[0]
        if not df1_row.equals(df2_row):
            diff_row = {col+'_file1': df1_row[col] for col in dfFile1.columns}
            diff_row.update({col+'_file2': df2_row[col] for col in dfFile2.columns})
            diff_row = {k: [v] for k, v in diff_row.items()}
            diff_df = pd.concat([diff_df, pd.DataFrame(diff_row)], ignore_index=True)


    diff_df = diff_df[listheader]
    diff_df.to_csv(path + '/' +'differences.csv', index=False)
    common_df.to_csv(path + '/' + 'common_elements.csv', index=False)
