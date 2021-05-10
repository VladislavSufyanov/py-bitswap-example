# py-bitswap-example
Example of using the py-bitswap project

Download submodules:
-------------------------
Download the required submodules:

    git submodule init
    git submodule update

Install requirements:
-------------------------

    pip3 install -r requirements.txt
    pip3 install -r bitswap/requirements.txt

Running:
-------------------------
Help (show options):

    python3 main.py -h

Running example (2 local peers):
    
    mkdir storage_1 storage_2
    python3.8 main.py --peer-name first_peer --storage-path storage_1
    python3.8 main.py --kad-port 21301 --web-socket-port 10101 --connect-port 10101 --peer-name second_peer --storage-path storage_2 --peers-adr 127.0.0.1:21300
