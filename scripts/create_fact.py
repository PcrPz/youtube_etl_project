import pandas as pd
import os
import random

def remove_old_and_save(filename, df, dir_path):
    file_path = os.path.join(dir_path, filename)
    
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"{filename} was deleted.")

    df.to_csv(file_path, encoding='gbk', index=False, mode='w') 
    print(f"{filename} was saved.")

def generate_surrogate_key(df, lower_limit, upper_limit):
    df["video_key"] = random.sample(range(lower_limit,upper_limit), df.shape[0])
    df["video_key"] = df["video_key"].astype(int)
    return df

def video_interaction_percentage(dest_df, source_df, col_name):
    dest_df[col_name] = round(
        (source_df['likes'] + source_df['dislikes'] + source_df['comment_count']) / 
        source_df['views'].replace(0, 1) * 100, 2)
    dest_df[col_name].fillna(0, inplace=True)

def create_fact_table():
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir,"data")
    output_dir = os.path.join(data_dir,"zone")


    NA_data = pd.read_csv(os.path.join(output_dir,"NA_data.csv"), encoding='gbk', low_memory=False)
    EU_data = pd.read_csv(os.path.join(output_dir,"EU_data.csv"), encoding='gbk', low_memory=False)
    AS_data = pd.read_csv(os.path.join(output_dir,"AS_data.csv"), encoding='gbk', low_memory=False)

    dim_table = [NA_data, EU_data, AS_data]
    title_name = ["Clean_NA_data.csv", "Clean_EU_data.csv", "Clean_AS_data.csv"]
    columnfact = ['na_video_interaction_rate', 'eu_video_interaction_rate', 'as_video_interaction_rate']


    NA_data = generate_surrogate_key(NA_data, 1000000, 2000000)
    EU_data = generate_surrogate_key(EU_data, 2000001, 3000000)
    AS_data = generate_surrogate_key(AS_data, 1, 1000000)


    fact_table = pd.DataFrame()
     # test Type
    print(NA_data['video_key'].dtype) 
    print(EU_data['video_key'].dtype)
    print(AS_data['video_key'].dtype)
    fact_table['AS_key'] = AS_data['video_key']
    fact_table['NA_key'] = NA_data['video_key']
    fact_table['EU_key'] = EU_data['video_key']
    fact_table['AS_key'] = fact_table['AS_key'].fillna(random.randint(9000000, 9999999))



    for df, column, title in zip(dim_table, columnfact, title_name):
        video_interaction_percentage(fact_table, df, column)
        remove_old_and_save(title, df, output_dir) 

    fact_table.to_csv(os.path.join(output_dir, "fact_table.csv"), index=False)
    print("fact_table.csv was saved.")
