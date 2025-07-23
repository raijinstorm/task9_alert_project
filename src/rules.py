from abc import ABC, abstractmethod
from collections import deque
from datetime import timedelta
import logging

class Rule(ABC):
  def __init__(self, limit: int, time_delta_seconds: int, col_names_dict, groups):
    self.limit = limit
    self.time_delta = timedelta(seconds=time_delta_seconds)
    self.col_names_dict = col_names_dict
    self.window_dict = {}
    self.groups = groups

  @abstractmethod
  def check(self, row_tuple):
    pass

class SlidingWindowRule(Rule):

  def check(self, row_tuple):
    current_time = row_tuple[self.col_names_dict["date"]]
    filtered_row = [row_tuple[self.col_names_dict[col_name]] for col_name in self.groups]
    filtered_row_str = ["" if s is None else str(s) for s in filtered_row]
    group_name = "_".join(filtered_row_str) 
    
    if group_name not in self.window_dict.keys():
      self.window_dict[group_name] =  {"window": deque(), "alert_is_active":False}
   
    self.window_dict[group_name]["window"].append(current_time)
    while current_time - self.window_dict[group_name]["window"][0] > self.time_delta:
        self.window_dict[group_name]["window"].popleft()


    if len(self.window_dict[group_name]["window"]) > self.limit and not self.window_dict[group_name]["alert_is_active"]:
      if group_name:
        logging.warning(f"ALERT. Error limit exceded for {group_name} at: {current_time}")
      else:
        logging.warning(f"ALERT. Error limit exceded at: {current_time}")
      self.window_dict[group_name]["alert_is_active"] = True
    elif len(self.window_dict[group_name]["window"]) <= self.limit and self.window_dict[group_name]["alert_is_active"]:
      if group_name:
        logging.info(f"Condition resolved for group {group_name} at: {current_time}")
      else:
        logging.info(f"Condition resolved at: {current_time}")
      self.window_dict[group_name]["alert_is_active"] = False