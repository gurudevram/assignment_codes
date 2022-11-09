import sys
sys.path.append('../')

import pandas as pd

import math

import random

import env

def get_data_df(input_file_path):
  # Reads file as dataframe
  return pd.read_csv(input_file_path, sep=" ", header = None)

def get_required_var_for_iterations(data_df_row_size, rows_to_be_populated):
  # Gives inputs required for iteration
  df_row_size = data_df_row_size
  loop_iterations_to_df = math.ceil(rows_to_be_populated/df_row_size)
  rows_to_pick = list(filter(lambda val: val%10==0 , [*range(0, df_row_size)]))[1:]
  rows_to_avoid = [0] + list(filter(lambda val: val%10!=0 , [*range(0, df_row_size)]))
  return loop_iterations_to_df, rows_to_pick, rows_to_avoid

def increase_ip(ip_val):
  # Increases ip value by 1
  ip_len = ip_val.split(".")
  return ".".join([str(int(val)+1) if (index == len(ip_len)-1) else val for index, val in enumerate(ip_len)])

def clean_request(request_val):
  # Cleans the reuest column
  remove_char_list = ['%', '-', '|', ","]
  method_variations_list = ["GET", "POST", "HEAD"]
  # " ".join(["GET", "POST", "HEAD", "new"]).replace("HEAD", "").replace("  "," ").strip().split(" ")
  #values should not contain spaces
  splitted_request_val = request_val.split(" ")
  splitted_request_val[0] = random.choice(method_variations_list)
  splitted_request_val = " ".join(splitted_request_val)
  return splitted_request_val
  # return "".join(["" if request_char in remove_char_list else request_char for request_char in splitted_request_val])

def clean_referer(referer_val):
  remove_char_list = ['%', '-', '|', ","]
  return "".join(["" if request_char in remove_char_list else request_char for request_char in referer_val])

def replace_rows(df):
  # Replaces the original rows
  df[0] = df[0].apply(lambda x: increase_ip(x))
  df[3] = pd.to_datetime(df[3], format="[%d/%b/%Y:%H:%M:%S") + pd.Timedelta(hours=1)
  df[3] = df[3].dt.strftime('[%d/%b/%Y:%H:%M:%S')
  df[5] = df[5].apply(lambda x: clean_request(x))
  # df[8] = df[8].apply(lambda x: clean_referer(x))
  return df

def populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path):
  # Populates the ouput file 
  modified_df = data_df
  df_row_count = 0
  for file_index in range(0,loop_iterations_to_df):
    if file_index>0:
      shuffled_df = modified_df.sample(frac=1)
      replaced_df = replace_rows(shuffled_df.iloc[rows_to_pick])
      unchanged_shuffled_df = shuffled_df.iloc[rows_to_avoid]
      modified_df = pd.concat([unchanged_shuffled_df, replaced_df], axis=0).sample(frac=1)
      modified_df.to_csv(save_to_path, header=None, index=None, sep=" ", mode = "a")
      df_row_count += modified_df.shape[0]
      print("{} iterations completed and {} rows populated".format(file_index, df_row_count))
    else:
      modified_df.to_csv(save_to_path, header=None, index=None, sep=" ", mode = "a")
      df_row_count += modified_df.shape[0]
      print("{} iterations completed and {} rows populated".format(file_index, df_row_count))

if __name__ == "__main__":
  # input log file path
  input_file_path = r"{}/{}.{}".format(env.log_file_path, env.log_file_name, env.log_file_extension)
  # rows required here
  rows_to_be_populated = env.increased_text_file_rows
  # output log file path
  save_to_path = r"{}/{}.{}".format(env.increased_text_file_path, env.increased_text_file_name.format(rows_to_be_populated), env.increased_text_file_extension)
  data_df = get_data_df(input_file_path)
  loop_iterations_to_df, rows_to_pick, rows_to_avoid = get_required_var_for_iterations(data_df.shape[0], rows_to_be_populated)
  populate_file(loop_iterations_to_df, rows_to_pick, rows_to_avoid, data_df, save_to_path)
