import pandas as pd

data1 = 'C:\\Users\\RSCOGGIN\\Documents\\Projects\\_python\\emc_summary\\reduced_item_history_report.xlsx'

data2 = 'C:\\Users\\RSCOGGIN\\Documents\\Projects\\_python\\emc_summary\\Valid Reference Markers by Highway.xlsx'
df1 = pd.read_excel(data1)
df2 = pd.read_excel(data2)

df2_matched = df2[df2['Highway'].isin(df1['HIGHWAY INFO.'])]

# Consolidate 1 row per highway
df2_matched = df2_matched.groupby('Highway').agg({
    'Begin RM': 'first',
    'Begin Displacement': 'first',
    'Ending Ref Marker': 'last',
    'End Displacement': 'last',
    'Start DFO': 'first',
    'End DFO': 'last'
}).reset_index()

df2_matched['BEG TRM'] = df2_matched['Begin RM'] + df2_matched['Begin Displacement']
df2_matched['END TRM'] = df2_matched['Ending Ref Marker'] + df2_matched['End Displacement']

df2_matched = df2_matched.drop(columns=[
    'Begin RM',
    'Begin Displacement',
    'Ending Ref Marker',
    'End Displacement'
    ])

output_path = 'filtered_ref.xlsx'
df2_matched.to_excel(output_path, index=False)
print('Successfully exported file')