import os
import pandas as pd

# Specify the folder containing CSV files
folder_path = '/Users/'

# Specify the output folder for processed CSV files
output_folder_path = '/Users/'


# Function to clean and process a DataFrame
def process_dataframe(df):
    # We want to keep schools that keep track of SAT or ACT scores
    # These are mor eliekly to be 4 year schools
    columns_to_check = df.columns
    selected_columns = [col for col in columns_to_check if col.startswith('SAT') or col.startswith('ACT')]
    new_dataframe = df[df[selected_columns].notna().any(axis=1)]
    # Remove non-main colleges as well as inactives colleges
    new_dataframe = new_dataframe[new_dataframe['MAIN'] == 1]
    new_dataframe = new_dataframe[new_dataframe['CURROPER'] != 0]
    result_data = new_dataframe.drop('CURROPER', axis=1)

    columns = [
        "UNITID",
    "INSTNM",
    "CITY",
    "STABBR",
    "ZIP",
    "ACCREDAGENCY",
    "PREDDEG",
    "HIGHDEG",
    "ST_FIPS",
    "REGION",
    "LOCALE",
    "LOCALE2",
    "LATITUDE",
    "LONGITUDE",
    "CCBASIC",
    "CCUGPROF",
    "CCSIZSET",
    "HBCU",
    "PBI",
    "ANNHI",
    "TRIBAL",
    "AANAPII",
    "HSI",
    "NANTI",
    "MENONLY",
    "WOMENONLY",
    "RELAFFIL",
    "ADM_RATE",
    "ADM_RATE_ALL",
    "SATVRMID",
    "SATMTMID",
    "SATWRMID",
    "ACTCMMID",
    "ACTENMID",
    "ACTMTMID",
    "ACTWRMID",
    "SAT_AVG",
    "SAT_AVG_ALL",
    "UGDS",
    "UG",
    "UGDS_WHITE",
    "UGDS_BLACK",
    "UGDS_HISP",
    "UGDS_ASIAN",
    "UGDS_AIAN",
    "UGDS_NHPI",
    "UGDS_2MOR",
    "UGDS_NRA",
    "UGDS_UNKN",
    "PPTUG_EF",
    "PPTUG_EF2",
    "NPT4_PUB",
    "NPT4_PRIV",
    "NPT4_PROG",
    "NPT4_OTHER",
    "NPT41_PUB",
    "NPT42_PUB",
    "NPT43_PUB",
    "NPT44_PUB",
    "NPT45_PUB",
    "NPT41_PRIV",
    "NPT42_PRIV",
    "NPT43_PRIV",
    "NPT44_PRIV",
    "NPT45_PRIV",
    "COSTT4_A",
    "COSTT4_P",
    "TUITIONFEE_IN",
    "TUITIONFEE_OUT",
    "TUITIONFEE_PROG",
    "TUITFTE",
    "INEXPFTE",
    "AVGFACSAL",
    "PFTFAC",
    "PCTPELL",
    "PCTFLOAN",
    "UG25ABV",
    "DEBT_MDN",
    "GRAD_DEBT_MDN",
    "WDRAW_DEBT_MDN",
    "LO_INC_DEBT_MDN",
    "MD_INC_DEBT_MDN",
    "HI_INC_DEBT_MDN",
    "DEP_DEBT_MDN",
    "IND_DEBT_MDN",
    "PELL_DEBT_MDN",
    "NOPELL_DEBT_MDN",
    "FEMALE_DEBT_MDN",
    "MALE_DEBT_MDN",
    "FIRSTGEN_DEBT_MDN",
    "NOTFIRSTGEN_DEBT_MDN",
    "COUNT_NWNE_P10",
    "COUNT_WNE_P10",
    "COUNT_NWNE_P10",
    "COUNT_WNE_P10"
    ]

    filtered_df = result_data[columns]
    return filtered_df

# Initialize an empty list to store processed DataFrames
processed_dfs = []

# Loop through each additional CSV file
for csv_file in os.listdir(folder_path):
    if csv_file.endswith('.csv'):
        file_path = os.path.join(folder_path, csv_file)

        additional_df = pd.read_csv(file_path)
        processed_additional_df = process_dataframe(additional_df)

        # Extract the year from the CSV file name
        year = int(csv_file.split('_')[0].replace('MERGED', ''))

        # Add a new column 'Year' to the processed DataFrame
        processed_additional_df['Year'] = year

        # Append the processed DataFrame to the list
        processed_dfs.append(processed_additional_df)

# Merge all processed DataFrames into a single DataFrame
merged_df = pd.concat(processed_dfs, ignore_index=True)
#output_excel_path = '/Users/'
#merged_df.to_excel(output_excel_path, index=False)
#print(f'Merged file saved as Excel: {output_excel_path}')

################# Cleaning Merged dataframe ##################
# Removing rows if all the rows have the same value
final_df = merged_df.loc[:, merged_df.nunique() > 1]
final_df

threshold = 0.8
thresh_value = int(threshold * len(final_df))
df = final_df.dropna(axis=1, thresh=thresh_value)
# Adjust Path
output_excel_path = '/Users/'
df.to_excel(output_excel_path, index=False)

