import pandas as pd
from datetime import datetime, timedelta
from collections import deque
import logging
import pytest
from src.rules import SlidingWindowRule 


@pytest.fixture(autouse=True)
def setup_logging(caplog):
    caplog.set_level(logging.INFO) 
    caplog.clear()


@pytest.fixture
def col_names_dict():
    col_names = ['error_code',
                'error_message',
                'severity',
                'log_location',
                'mode',
                'model',
                'graphics',
                'session_id',
                'sdkv',
                'test_mode',
                'flow_id',
                'flow_type',
                'sdk_date',
                'publisher_id',
                'game_id',
                'bundle_id',
                'appv',
                'language',
                'os',
                'adv_id',
                'gdpr',
                'ccpa',
                'country_code',
                'date']
    return {name: i for i, name in enumerate(col_names)}


def generate_row(col_names_dict, **kwargs):
    row = [None] * len(col_names_dict)
    for col_name, value in kwargs.items():
        row[col_names_dict[col_name]] = value
    return row


def test_sliding_window_rule_no_group(caplog, col_names_dict):
    rule = SlidingWindowRule(2, 5, col_names_dict, groups=[])
    
    timestamps = [
        datetime(2023, 1, 1, 10, 0, 1),
        datetime(2023, 1, 1, 10, 0, 4),
        datetime(2023, 1, 1, 10, 0, 5),
        datetime(2023, 1, 1, 10, 2, 10)
    ]
    
    rows = []
    for curr_timestamp in timestamps:
        rows.append(generate_row(col_names_dict, date=curr_timestamp))  
             
    rule.check(rows[0])
    assert len(rule.window_dict[""]["window"]) == 1
    assert not rule.window_dict[""]["alert_is_active"]
    assert "ALERT" not in caplog.text
    
    rule.check(rows[1])
    assert len(rule.window_dict[""]["window"]) == 2
    assert not rule.window_dict[""]["alert_is_active"]
    assert "ALERT" not in caplog.text
    
    rule.check(rows[2])
    assert len(rule.window_dict[""]["window"]) == 3
    assert rule.window_dict[""]["alert_is_active"]
    assert "ALERT. Error limit exceded at: 2023-01-01 10:00:05" in caplog.text
    assert "Condition resolved" not in caplog.text
    
    rule.check(rows[3])
    logging.warning("glagnn")
    assert len(rule.window_dict[""]["window"]) == 1
    assert not rule.window_dict[""]["alert_is_active"]
    assert "Condition resolved" in caplog.text
    

def test_sliding_window_rule_group(caplog, col_names_dict):
    rule = SlidingWindowRule(1, 5, col_names_dict, groups=["bundle_id"])
    
    rule.check(generate_row(col_names_dict, date=datetime(2023, 1, 1, 10, 0, 1), bundle_id = "bundleA"))
    assert len(rule.window_dict["bundleA"]["window"]) == 1
    assert not rule.window_dict["bundleA"]["alert_is_active"]
    assert "ALERT" not in caplog.text
    
    
    rule.check(generate_row(col_names_dict, date=datetime(2023, 1, 1, 10, 0, 2), bundle_id = "bundleB"))
    assert len(rule.window_dict["bundleA"]["window"]) == 1
    assert not rule.window_dict["bundleA"]["alert_is_active"]
    assert len(rule.window_dict["bundleB"]["window"]) == 1
    assert not rule.window_dict["bundleB"]["alert_is_active"]
    assert "ALERT" not in caplog.text
    
    rule.check(generate_row(col_names_dict, date=datetime(2023, 1, 1, 10, 0, 3), bundle_id = "bundleA"))
    assert len(rule.window_dict["bundleA"]["window"]) == 2
    assert rule.window_dict["bundleA"]["alert_is_active"]
    assert len(rule.window_dict["bundleB"]["window"]) == 1
    assert not rule.window_dict["bundleB"]["alert_is_active"]
    assert "ALERT. Error limit exceded for bundleA at: 2023-01-01 10:00:03" in caplog.text
