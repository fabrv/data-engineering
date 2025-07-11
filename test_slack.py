#!/usr/bin/env python3
"""
Test script for Slack notifications
"""
import sys
sys.path.append('/app/citibike_project')
from utils.slack_notifier import notify_failure, notify_success, notify_completion

def test_notifications():
    print("Testing Slack notifications...")
    
    # Test success notification
    notify_success("Test Step", {
        "records_processed": 1000,
        "duration": "30 seconds"
    })
    
    # Test failure notification
    notify_failure("Test Step", "Simulated test error", {
        "error_code": "TEST_001",
        "timestamp": "2024-01-01 12:00:00"
    })
    
    # Test completion notification
    notify_completion(5, "2 minutes 30 seconds")
    
    print("Slack notification tests completed!")

if __name__ == "__main__":
    test_notifications()