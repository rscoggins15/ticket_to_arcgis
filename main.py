import pandas as pd

data = 'C:\\Users\\RSCOGGIN\\Documents\\Projects\\_python\\emc_summary\\spreadsheets\\reduced_item_history_report.xlsx'
df = pd.read_excel(data)

# Pull specific columns
df = df[['COUNTY', 'HIGHWAY INFO.', 'BEGIN STA.', 'END STA.', 'ITEM DESCRIPTION', 'DWR REPORT QTY.', 'UNIT PRICE']]


# Convert reference markers to decimal format
def decimal_trm(df):
    def sum_rm(cell):
        parts = cell.split(' +')
        return float(parts[0]) + float(parts[1])

    df['TRM BEG'] = df['BEGIN STA.'].apply(sum_rm)
    df['TRM END'] = df['END STA.'].apply(sum_rm)

    # Calculate total price by multiplying the quantity by the unit price
    df['TOTAL PRICE'] = df['DWR REPORT QTY.'] * df['UNIT PRICE']

    # Sort the rows by County, roadway, and the item description
    df = df.sort_values(by=['COUNTY', 'HIGHWAY INFO.', 'ITEM DESCRIPTION'])
    return df

# Convert to DFO function
def add_dfo(df1):
    file2_path = 'C:\\Users\\RSCOGGIN\\Documents\\Projects\\_python\\emc_summary\\spreadsheets\\filtered_ref.xlsx'
    df2 = pd.read_excel(file2_path)

    # Create a dictionary to store the second file's data for quick lookup
    highway_data = df2.set_index('Highway').to_dict('index')

    # Initialize new columns in the first DataFrame
    df1['BEG DFO'] = None
    df1['END DFO'] = None

    # Iterate through the rows of the first DataFrame and perform the calculations
    for index, row in df1.iterrows():
        highway_info = row['HIGHWAY INFO.']
        trm_beg_first = row['TRM BEG']
        trm_end_first = row['TRM END']

        if highway_info in highway_data:
            trm_beg_second = highway_data[highway_info]['TRM BEG']
            trm_end_second = highway_data[highway_info]['TRM END']
            end_dfo_second = highway_data[highway_info]['End DFO']

            beg_dfo = trm_beg_first - trm_beg_second
            end_dfo = (trm_end_first - trm_end_second) + end_dfo_second

            df1.at[index, 'BEG DFO'] = beg_dfo
            df1.at[index, 'END DFO'] = end_dfo
    return df1


# Separate out by county
def separate_counties(df):
    counties = df['COUNTY'].unique()

    for county in counties:
        county_df = df[df['COUNTY'] == county]
        county_df.to_csv(f'.\\all_in_one\\{county}_spreadsheet.csv', index=False)

# Consolidate TRMs and add a count for each ticket
def add_count(df):
    df = df.groupby(['COUNTY', 'HIGHWAY INFO.', 'ITEM DESCRIPTION', 'TRM BEG', 'TRM END', 'BEG DFO', 'END DFO']).agg(
        total_qty=('DWR REPORT QTY.', 'sum'),
        count=('DWR REPORT QTY.','size')
    ).reset_index()
    return df

# Generate the files
the_df = add_dfo(decimal_trm(df))
the_df.to_csv('all_tickets_report.csv', index=False)
add_count_df = add_count(the_df)
add_count_df.to_csv('grouped_report.csv', index=False)
separate_counties(the_df)
