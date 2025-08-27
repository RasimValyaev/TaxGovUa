# see rkengler.com for related blog post
# https://www.rkengler.com/how-to-capture-network-traffic-when-scraping-with-selenium-and-python/

import json
import re

from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

capabilities = DesiredCapabilities.CHROME
# capabilities["loggingPrefs"] = {"performance": "ALL"}  # chromedriver < ~75
capabilities["goog:loggingPrefs"] = {"performance": "ALL"}  # chromedriver 75+


def process_browser_logs_for_network_events(logs):
    """
    Return only logs which have a method that start with "Network.response", "Network.request", or "Network.webSocket"
    since we're interested in the network events specifically.
    """
    for entry in logs:
        log = json.loads(entry["message"])["message"]
        if (
                "Network.response" in log["method"]
                or "Network.request" in log["method"]
                or "Network.webSocket" in log["method"]
        ):
            yield log


def get_token(logs):
    logs_str = str(logs)
    pattern = r'"Authorization":"Bearer\s(?!undefined)[\w-]+"'
    matches = re.findall(pattern, logs_str)
    if matches:
        return matches[-1].split(":")[-1].replace('"', '')
    return None


def get_bearer_token(driver):
    logs = driver.get_log("performance")
    network_logs = process_browser_logs_for_network_events(logs)
    token = get_token(logs)
    if token:
        return token

    for log in network_logs:
        try:
            auth = log.get('params').get('headers').get('Authorization')
            if auth is not None and auth != "Bearer undefined" and 'Bearer' in auth:
                return auth
        except:
            continue
    auth = find_authorization_dict(logs)
    if auth:
        return auth
    return None


def find_authorization_dict(data):
    if not isinstance(data, (dict, list)):
        data = json.loads(data)

    if isinstance(data, dict):
        for key, value in data.items():
            if key == 'Authorization' and value != "Bearer undefined" and 'Bearer' in value:
                return data
            elif isinstance(json.dumps(value), (dict, list)):
                result = find_authorization_dict(value)
                if result:
                    return result
    elif isinstance(data, list):
        for item in data:
            result = find_authorization_dict(item)
            if result:
                return result
    return None
