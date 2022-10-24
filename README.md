# kz-api-elasticsearch
Continuously upload kz records to an elastic node. Bad code!

Clone this repo then
`cd kz-api-elasticsearch`
`pip install -e .` or `pip3 install -e .`

Usage: `kzcontinue <ip> <port> <index> <start_id>`

(single threaded, no proper rate limit, no auth, no handling of non existent future runs)
