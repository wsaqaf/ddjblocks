##### Configure those variables so that you don't need to mess with the other files

# Postgres DB config
dbhost="localhost"
dbname="ddjblocks"
dbuser="ddjblocks"
dbpw="datajournalism"

# If set, the original input file is appended with any discovered change addresses
change_addresses=0

# How many addresses should the script be able to handle at once
max_addresses=10000 

# 1 means incoming only, 2 means outgoing only, 0 means both are allowed
direction=0 

# by move forward, you ask ddjblocks to recursively add new addresses that emerge in the "To" column of the transaction. The value is how many iterations/hops you want to go.
move_forward=0  

# by move forward, you ask ddjblocks to recursively add new addresses that emerge in the "To" column of the transaction. 
go_backward=0

# This is where the daily average rates for BTCUSD would be kept
rates_file="rates/btcusd_daily_rates.csv" 

# This is the blockchain.info API code that will prevent limits from being set

api_code='' 

# The public key and private key are important for BTCUSD conversions
btcavg_key="" 
btcavg_secret=""

