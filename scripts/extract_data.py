import pandas as pd
import os
import glob

def remove_old_and_save(filename, df, dir_path):
    file_path = os.path.join(dir_path, filename)
    
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{filename} was deleted.")

    df.to_csv(file_path, index=False)
    print(f"{filename} was saved.")

def create_dimension_table():
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir,"data")
    output_dir = os.path.join(data_dir,"zone")

    os.makedirs(output_dir,exist_ok=True)
    
    #Select Zone
    north_america = ['US', 'CA', 'MX']
    europe = ['GB', 'DE', 'FR', 'RU']
    asia = ['KR', 'IN', 'JP']

    NA_data, EU_data, AS_data = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for file_path in glob.glob(os.path.join(data_dir, '*.csv')):
        temp_df = pd.read_csv(file_path, low_memory=False, encoding='latin')
        print(f"{file_path}")
        if any(country in file_path for country in north_america):
            NA_data = pd.concat([NA_data, temp_df], ignore_index=True)
        elif any(country in file_path for country in europe):
            EU_data = pd.concat([EU_data, temp_df], ignore_index=True)
        elif any(country in file_path for country in asia):
            AS_data = pd.concat([AS_data, temp_df], ignore_index=True)

    remove_old_and_save("NA_data.csv", NA_data, output_dir)
    remove_old_and_save("EU_data.csv", EU_data, output_dir)
    remove_old_and_save("AS_data.csv", AS_data, output_dir)
