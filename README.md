![DDJBlocks](images/logo.png)

A Data-Driven Journalism tool to analyze blockchain data

DDJBlocks is a simple but unique tool that is intended to be useful for data journalists and researchers to extract transaction records from the bitcoin Main blockchain for further analysis. Since the learning curve on how to read and extract data from public permissionless blockchains is steep for many, I decided to create this tool to save time and energy of those interested to dig deeper into this often mysterious arena. With all the hype around bitcoin and blockchain, it is high time data and investigative journalists have some way to study bitcoin transactions.

The tool is in development mode and is under continuous testing to find bugs and make improvements.

# Installation

To install the tool, simply download or clone this repo into a folder on your website.

Then you will need to install dependencies and do some necessary steps as follows:

1) Install the [blockchain.info Python package](https://github.com/blockchain/api-v1-client-python) since it is the most important library needed in this tool
2) I recommend you [request an API key](https://api.blockchain.info/customer/signup) from blockchain.info to use it without limits
3) Get a free public key and secret from [Bitcoin Average] (https://bitcoinaverage.com/) to access daily rates for conversion to USD
4) Install the [Geolocation Postgres](https://github.com/tvondra/geoip) package which facilitates translating IP information to the corresponding geolocation. Part of the installation requires also downloading data from [MaxMind](https://dev.maxmind.com/geoip/geoip2/geolite2/)
5) Double check to see that you have all the needed libraries (all available via pip install <module>)
6) Make sure you have postgresql server running (you will need the credentials handy)
7) Once you have done all the above, go into config_empty.py and enter the required information
8) If all goes well, then you will be ready to start using the tool.
  
# Usage:
```sh
python ddjblocks.py <file> 
```
where <file> is a CSV file containing a list of addresses (starting with the header *address*) to analyze for incoming and outgoing transactions. There is a preloaded file called wannacry.csv (containing three addresses known to belong to that ransomware) in the cases folder. To analyze that file, you can run the this command in the terminal:

```sh
$ python ddjblocks.py wannacry.csv

```

The script should then access the blockchain explorer, retreive the transactions and connect addresses with each other. Additionally, it uses the relayed IP parameters to detect the city (using geolocation data obtained from MaxMind) and adds a USD converted value based on the time of the particular transaction. The USD value of the transaction needs to be assessed carefully depending on whether the transaction was spent or not. 

Once done, the operation will generate by default the following three CSV files in the output folder:
- <case_name>_all_addresses.csv: this contains all addresses that either received or send funds to the given addresses. It also includes the analyzed addresses themselves
- <case_name>_change_addresses.csv: this will contain the list of addresses that are suspected to be change addresses belonging to the one of the addresses given in the original file. Knowing those addresses may be useful to use as new input to go deeper into analyzing the account if needed. 
- <case_name>_transactions.csv: This CSV file will contain the transactions themselves. Note that in cases where multiple input addresses were used to transmit a fund to one of the analyzed addresses, only one address (the one with the highest amount of BTC) is used as the 'sender' of the amount. The other accounts are assumed to be change addresses and hence they belong to the same wallet. This was a necessary measure to be able to map out relationships between two addresses.

Below are the results one finds in the transaction CSV file:

* **from**: address sending the fund
* **to**: address receiving it
* **time**: the time the transaction was mined
* **value_sent_btc**: the value received (in BTC) received
* **value_sent_usd**: the value received (in USD) 
* **fee**: The transaction fee in btc
* **city**: The city where the relayed IP is located
* **is_utxo**: A flag indicating whether the output transaction was spent
* **messages**: Any OP_RETURN messages that were found in the transaction
* **tx_hash**: the hash of the transaction, which is useful to cross-validate using blockchain explorers

Once you have this data, you have many ways to analyze it. For example, you can answer questions like:
- How frequently do funds get sent and from/to whom
- Using pivot tables, one can know the addresses that give or take bigger amounts in comparison to others
- How long do they stay before they are spent (no longer unspent transactions)
- Where are the transactions taking place (cities when geolocation data is available)
- What embedded messages appear (OP_RETURN)
- To follow a specific address, it is possible to rerun the script with that address using the "go_forward" or "move_backward" parameters in the configuration file. Rerunning the tool multiple times can create a very complex and multilayered network
- What networks emerge over time (this could lead to using social network analysis and visualisaiton tools such as [Kumu](https://kumu.io))

# Future work:
We plan to have journalists start using the tool and help them generate questions and give feedback on how it was useful (or not) and what improvements could be made. If you want to be one of those journalits, please drop me an email on walid@al-saqaf.se




