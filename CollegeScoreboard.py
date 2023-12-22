import os
import pandas as pd
from zipfile import ZipFile


zip_file_path = r'C:\Users\domin\PycharmProjects\CollegeScoreboardProject\CollegeScorecard_Raw_Data.zip'


extracted_dir = r'C:\Users\domin\PycharmProjects\CollegeScoreboardProject\ExtractedFiles'


exclude_words = ['Community', 'beauty', 'trade', 'hair', 'Cosmetology', 'Esthetics', 'Seminary',
                 'Medical', 'Funeral', 'technical', 'nursing']


os.makedirs(extracted_dir, exist_ok=True)


with ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_dir)


combined_df = pd.DataFrame()


all_files = os.listdir(os.path.join(extracted_dir, 'data'))
print(f"All files in the subdirectory: {all_files}")


files_list = [file for file in all_files if file.startswith('MERGED') and file.endswith('.csv')]
if files_list:
    first_file_path = os.path.join(extracted_dir, 'data', files_list[0])
    first_file_columns = pd.read_csv(first_file_path, nrows=0).columns


    for file_name in files_list:
        file_path = os.path.join(extracted_dir, 'data', file_name)


        df = pd.read_csv(file_path, usecols=first_file_columns)


        df = df[~df['INSTNM'].str.contains('|'.join(exclude_words), case=False)]

        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Display the combined DataFrame
    print(combined_df.head())

    combined_df.to_csv('combined_data.csv', index=False)

else:
    print("No files found with the specified pattern.")

output_filename = 'combined_data.csv'
combined_df.to_csv(output_filename, index=False)
print(f"\nCombined data saved to {output_filename}")