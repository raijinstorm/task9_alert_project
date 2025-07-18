import pandas as pd
import logging
from src import config
from src.rules import SlidingWindowRule


def process_csv(csv_file_path: str, chunk_size: int, rule_list):
  batched_df = pd.read_csv(csv_file_path, chunksize=chunk_size)

  for batch in batched_df:
    batch.columns = ['error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']
    batch["date"] = pd.to_datetime(batch["date"], unit="s")

    for row_tuple in  batch.itertuples(index=False):
      for rule in rule_list:
        rule.check(row_tuple)
      
    
def main():
    col_names = [ 'error_code', 'error_message', 'severity', 'log_location', 'mode', 'model', 'graphics', 'session_id', 'sdkv', 'test_mode', 'flow_id', 'flow_type', 'sdk_date', 'publisher_id', 'game_id', 'bundle_id', 'appv', 'language', 'os', 'adv_id', 'gdpr', 'ccpa', 'country_code', 'date']

    col_names_dict = {}
    for i, row in enumerate(col_names):
        col_names_dict[row] = i

    rule1 = SlidingWindowRule(limit = 10, time_delta_seconds=60, col_names_dict=col_names_dict, groups=[])
    rule2 = SlidingWindowRule(limit = 10, time_delta_seconds=600, col_names_dict=col_names_dict, groups=["bundle_id"])
    rule_list = [rule1, rule2]
    
    process_csv(config.CSV_FILE_PATH, config.CHUNK_SIZE, rule_list)
    
if __name__ == "__main__":
    main()