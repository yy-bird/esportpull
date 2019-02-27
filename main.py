import time, requests, re
from matchsocket import MatchSocket
from oddssocket import OddsSocket
from threading import Thread
def match():
    for i in range(2, 100000):
        ms.send(i)
        time.sleep(60)

if __name__ == "__main__":
    resp = requests.get("https://ggbet.com/en/betting")
    token = re.search('token: "(.+?)",', resp.text)[1]
    print(f"Token is {token}")
    # ms = MatchSocket(token)
    # os = OddsSocket(token)
    # # ms.start()
    os.start()
    Thread(target=match, daemon=True)
    
    while True:
        time.sleep(100000)