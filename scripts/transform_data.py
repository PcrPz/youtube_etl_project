import pandas as pd
import os
import random


def handle_null_and_dup_values(df):
     df.drop_duplicates(subset='video_id',inplace=True)
     df=df.reset_index(drop=True)
     return df

def clear_nonalphabet(df):
    df.replace(to_replace=[r"\\t|\\n|\\r", "\t|\n|\r",r' +',r'\.+'], value=["",""," ","."], regex=True, inplace=True)
    return df

def remove_old_and_save(filename, df, dir_path):
    file_path = os.path.join(dir_path, filename)
    
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{filename} was deleted.")

    df.to_csv(file_path, encoding='gbk',index=False,mode='a')
    print(f"{filename} was saved.")

def generate_surrogate_key(df,lower_limit,upper_limit):
    df["video_key"] = random.sample(range(lower_limit, upper_limit), df.shape[0])
    return df

def clean_data():
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir,"data")
    output_dir = os.path.join(data_dir,"zone")

    NA_data = pd.read_csv(os.path.join(output_dir,"NA_data.csv"),encoding='gbk', low_memory=False)
    EU_data = pd.read_csv(os.path.join(output_dir,"EU_data.csv"),encoding='gbk', low_memory=False)
    AS_data = pd.read_csv(os.path.join(output_dir,"AS_data.csv"),encoding='gbk', low_memory=False)
    print(os.path.join(output_dir,"NA_data.csv"))

    zone_df =[NA_data,EU_data,AS_data]
    title_name = ["Clean_NA_data.csv","Clean_EU_data.csv","Clean_AS_data.csv"]

    for df,title in zip(zone_df,title_name):
        df = handle_null_and_dup_values(df)
        df = clear_nonalphabet(df)
        remove_old_and_save(title, df, output_dir)
