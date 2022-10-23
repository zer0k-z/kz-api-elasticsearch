# kz-api-elasticsearch
Continuously upload kz records to an elastic node. Bad code!
Usage: python kzcontinue.py <ip> <port> <index> <start_id>
(single threaded, no proper rate limit, no auth, no handling of non existent future runs)
