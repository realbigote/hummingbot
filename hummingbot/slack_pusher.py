import json
import logging
import requests

def SlackPusher(hook, msg):
  #Pushes message to slack channel based on hook endpoint provided.

  try:
    data = json.dumps({"text":msg})
    res = requests.post(url=hook, data=data)
    return res.status_code
  except Exception as e:
    logging.error(f"{e} ::: {e.__cause__}")
    return None