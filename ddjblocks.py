### Steps needed to get this running: 
## 1. Install Blockchain API Python repo : https://github.com/blockchain/api-v1-client-python
## 2. Install Geolocation Postgres: https://github.com/tvondra/geoip
## 3. Download the geolocation data needed for the above repo. MaxMind GeoLite recommended: http://www.maxmind.com/app/geolitecountry
## 4. Load BTC daily rates bitcionaverage.com. API access to daily rates is free for upto 5000 requests per month https://bitcoinaverage.com/en/plans

########## ###
# Created by Walid Al-Saqaf, 2018 - Please do not use for illegal purposes and do keep this credit note ###
# for questions or feedback, email me on walid@al-saqaf.se ###
######### ###

########### IMPORT LIBRARIES AND SETTINGS  #############

import sys
import csv
import os.path
import time
import string
import pandas as pd
import psycopg2
import requests
from bitcoin import *
from datetime import datetime, timedelta
from calendar import timegm
from blockchain import blockexplorer
import urllib2
import codecs
import hashlib
import hmac
import linecache
import json
from builtins import bytes
from bitcoinaverage import RestfulClient

import config

################### FUNCTIONS #########################

def save_tx_csv(fn):

        global filename
	global cur

        with open(fn, 'w') as csvfile:
               csvfile.write("from,to,time_sent,value_sent_btc,value_sent_usd,fee,city,is_utxo,notes,tx_hash,messages\n");

        query="SELECT from_addr,to_addr,time_sent,amount_sent*0.00000001,(select amount_sent*0.00000001*(select rate_usd from ddjblocks.btc_rates where i_date=time_sent::date)),fee*0.00000001,(select city FROM geoip_city(relayed_ip::inet)),is_utxo,notes,tx_i,messages FROM ddjblocks."+filename+"transactions"
        outputquery = "COPY ({0}) TO STDOUT WITH CSV ".format(query)

        with open(fn, 'a') as f:
            cur.copy_expert(outputquery, f)

        return

###

def save_addr_csv(fn,mode):

        global filename
	global all_addresses
	global cur

	if mode==1:
        	with open(fn, 'w') as csvfile:
                	csvfile.write("address\n");

        	with open(fn, 'a') as csvfile:
                	for a in all_addresses:
                        	csvfile.write(a+"\n");
	else:
                with open(fn, 'w') as csvfile:
                        csvfile.write("Label\n");

		cur.execute("SELECT distinct from_addr FROM "+filename+"transactions UNION SELECT distinct to_addr FROM "+filename+"transactions;")
		
		distinct_addresses=cur.fetchall()
		
                with open(fn, 'a') as csvfile:
                        for a in distinct_addresses:
                                csvfile.write(str(a[0])+"\n");

        return

###

def update_rates_file():

	timestamp = int(time.time())
	payload = '{}.{}'.format(timestamp, config.btcavg_key)
	hex_hash = hmac.new(config.btcavg_secret.encode(), msg=payload.encode(), digestmod=hashlib.sha256).hexdigest()
	signature = '{}.{}'.format(payload, hex_hash)

	url='https://apiv2.bitcoinaverage.com/indices/global/history/BTCUSD?period=alltime&format=csv'
	headers = {'X-Signature': signature}
	result = requests.get(url=url, headers=headers)
	
	result.raise_for_status()

	today=time.strftime("%Y-%m-%d", time.gmtime())
	if (not linecache.getline(config.rates_file, 1).startswith(today)):
		url='https://apiv2.bitcoinaverage.com/convert/global?from=BTC&to=USD&amount=1'
		result2 = requests.get(url=url, headers=headers)
		usd_rate=json.loads(result2.content)['price']
		result=result.content.replace("BTC","BTC\n"+today+" 00:00:00,"+str(usd_rate)+","+str(usd_rate)+","+str(usd_rate)+",0")
	with open(config.rates_file, 'w') as handle:
        	handle.write(result)

	return
###

def cleanexit():
	global conn
	global cur

	cur.close()
	conn.close()
	sys.exit(0)

	return
###

def create_address_table(tble):
	global cur

        try:
                cur.execute("CREATE TABLE "+tble+" (address VARCHAR(255) PRIMARY KEY, updated TIMESTAMP, tx_count INTEGER)")
		cur.execute("CREATE INDEX ON "+tble+" (address);")

                print "New address table "+tble+" created."

        except psycopg2.OperationalError as e:
                print('Unable to create address table!\n{0}').format(e)

	return
###

def create_tx_table(tble):
        global cur

      	try:
        	cur.execute("CREATE TABLE "+tble+" (tx_i VARCHAR(255) PRIMARY KEY, from_addr VARCHAR(255), to_addr VARCHAR(255), time_sent TIMESTAMP, "
			    "amount_sent real, fee real, relayed_IP VARCHAR(255), is_utxo boolean, notes VARCHAR(255), messages VARCHAR(255));")
                cur.execute("CREATE INDEX ON "+tble+" (tx_i);")

        	print "New tx table "+tble+" created."

      	except psycopg2.OperationalError as e:
        	print('Unable to create tx table!\n{0}').format(e)

        return
###


def create_rates_table():
        global cur

        try:
                cur.execute("CREATE TABLE ddjblocks.btc_rates (i_date DATE PRIMARY KEY, rate_usd real);")
                cur.execute("CREATE INDEX ON ddjblocks.btc_rates (i_date);")

                print "New rates table created."

        except psycopg2.OperationalError as e:
                print('Unable to create rate table!\n{0}').format(e)

        return
###

def csv_to_array(fl): # use descriptive variable names
    with open(fl, 'r') as myfile:
    	response=myfile.read()
    lines = response.splitlines() # you don't need an open...the data is already loaded
    for line in lines[1:]: # skip first line (has headers)
        el = [i.strip() for i in line.split(',')]
        yield el # don't return, that immediately ends the function

###
def load_addr(addr):
	global filename
	global processed
	global total_addresses
	global addresses
	global new_addresses
	global cur
	global all_addresses
	global current_level
	global in_depth

	tx_count=0

	all_addresses=[addr]+all_addresses
	
	tx_left=1
	offset_id=0
	max_reached=0

	while (1):
	   if max_reached:
		break
	   if (tx_count==0):
	        address = blockexplorer.get_address(addr,api_code=config.api_code)
		cur.execute("SELECT tx_count FROM "+filename+"addresses WHERE address='"+addr+"';")
		try:
			old_n_tx=cur.fetchone()[0]
			if (old_n_tx):
				if old_n_tx<address.n_tx:
					tx_count=old_n_tx
					print "Updating tx record for "+addr
					continue				 
				else:
					print in_depth+str(current_level)+"TX record for "+addr+" up-to-date. Skipping..."
					break	
                except:
                        pass
	   else:
                tx_left=address.n_tx-tx_count
		if (tx_left==0):
			break
		else:
			offset_id=tx_count
			address = blockexplorer.get_address(addr,offset=offset_id,api_code=config.api_code)
	   for transaction in address.transactions:
        	if (transaction.double_spend or transaction.block_height==-1):
			continue
        	tx_count+=1 
		in_count=0
		out_count=0
		total_output=0
		address_output=0
		total_input=0
		main_sending_address=""
		main_receiving_address=""
		max_input=0
		max_output=0
		receiving_tx=0
		sending_tx=0
		received_amount=0
		sent_amount=0
		is_utxo=0
		is_utxo_i=0
		tx_time=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(transaction.time))
		found_change_addresses=0
		change_addresses=[]
		encoded_messages=""

                print in_depth+str(current_level)+"TX:",tx_count,transaction.hash," ("+str(tx_count)+"/"+str(address.n_tx)+")"

        	if (config.direction!=2):
          		for input in transaction.inputs:
            			if (not input.value):
					continue
                                if input.address is None:
                                        if (input.script.startswith("6a")):
                                                decoded_msg=input.script[2:].decode("hex")
                                                decoded_msg=''.join(x for x in decoded_msg if x in string.printable)
                                                decoded_msg=decoded_msg.replace('\r',' ').replace('\n',' ')
                                                encoded_messages=encoded_messages+decoded_msg+"\n"
                                if (input.address==addr):
                                        sent_amount=input.value+sent_amount
                                        sending_tx=1
				in_count+=1
                                total_input=input.value+total_input
				if (input.address==addr):
					found_change_addresses=1
				if (input.value>max_input):
					main_sending_address=input.address
					max_input=input.value	
				change_addresses.append(input.address)
                if (config.direction!=1):
                        for output in transaction.outputs:
				if output.address is None:
					if (output.script.startswith("6a")):
						decoded_msg=output.script[2:].decode("hex")
						decoded_msg=''.join(x for x in decoded_msg if x in string.printable)
						decoded_msg=decoded_msg.replace('\r',' ').replace('\n',' ')
						encoded_messages=encoded_messages+decoded_msg+"\n"
				if (output.address==addr):
					received_amount=output.value+received_amount
					receiving_tx=1
					if (not output.spent):
						is_utxo=1
                                if (output.value>max_output):
                                        main_receiving_address=output.address
					utxo_i=output.spent
                                        max_output=output.value
				out_count+=1
				total_output=output.value+total_output
                fee=(total_input-total_output)
	   	if (receiving_tx):
                	row="'"+transaction.hash+"', '"+main_sending_address+"', '"+addr+"', '"+str(tx_time)+"', "+str(received_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo))+",'To "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                        cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")
                if (sending_tx):
                        row="'"+transaction.hash+"', '"+addr+"', '"+main_receiving_address+"', '"+str(tx_time)+"',"+str(sent_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo_i))+", 'From "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                        cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")
	   	if (found_change_addresses):
			all_addresses=list(set(all_addresses)|set(change_addresses))
			all_addresses.remove(addr)
			all_addresses=[addr]+all_addresses

                if (tx_count>=config.max_transactions):
                        print "Reached max transactions",config.max_transactions,"... skipping this address and moving to next"
                        max_reached=1
			break

	cur.execute("INSERT INTO "+filename+"addresses VALUES('"+addr+"',NOW(),"+str(address.n_tx)+") ON CONFLICT (address) DO UPDATE SET updated=NOW(),tx_count="+str(address.n_tx)+";")

   	processed+=1
	perc=(100*processed)/config.max_addresses;
	print "Processed address ("+str(processed)+"/"+str(config.max_addresses)+") [%"+str(perc)+"]";
	
	return
###

################### END OF FUNCTIONS #########################

# Some global variable initialisations

addresses=pd.DataFrame(['address'])
filename=''
original_file=''
processed=0
total_addresses=0

try:
        conn = psycopg2.connect("dbname='"+config.dbname+"' user='"+config.dbuser+"' host='"+config.dbhost+"' password='"+config.dbpw+"'")
#        conn = psycopg2.connect(dbname=config.dbname user=config.dbuser host=config.dbhost password=config.dbpw)

        conn.autocommit = True
        cur = conn.cursor()
except psycopg2.OperationalError as e:
        print('Unable to connect!\n{0}').format(e)
        sys.exit(1)

if len(sys.argv)>1:
        if os.path.isfile("cases/"+sys.argv[1]):
                print "Getting addresses from "+sys.argv[1]
                filename=os.path.basename("cases/"+sys.argv[1]).split('.')[0]+"_"
		original_file="cases/"+sys.argv[1]
                addresses=pd.read_csv("cases/"+sys.argv[1])
        else:
                print "the input file cases/"+sys.argv[1]+" could not be found"
                cleanexit()
else:
        print "No input CSV provided"
        cleanexit()

try:
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='ddjblocks' AND tablename='"+filename+"addresses');")
        if not cur.fetchone()[0]:
                create_address_table(filename+"addresses")
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='ddjblocks' AND tablename='"+filename+"transactions');")
        if not cur.fetchone()[0]:
                create_tx_table(filename+"transactions")
        cur.execute("SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname='ddjblocks' AND tablename='btc_rates');")
        if not cur.fetchone()[0]:
		create_rates_table()
except psycopg2.OperationalError as e:
        print('Unable to connect!\n{0}').format(e)
        sys.exit(1)

cur.execute("select true from ddjblocks.btc_rates where i_date=DATE 'today';")
if not cur.fetchone():
	try:
		time_modified=os.path.getmtime(config.rates_file)
		if (time_modified-time.time())>43200:
			update_rates_file()
			print "Updated rates file from bitcoinaverage.com!"
		else:
			print "using existing rates file"
	except:
                update_rates_file()
                print "Updated rates file from bitcoinaverage.com!"

	data = csv_to_array(config.rates_file)
	for row in data: # don't use indexes, just iterate over the data
		r="select rate_usd from ddjblocks.btc_rates where i_date='"+row[0]+"';"
		cur.execute("select rate_usd from ddjblocks.btc_rates where i_date='"+row[0]+"';")
		if not cur.fetchone():
			cur.execute("insert into ddjblocks.btc_rates values('"+row[0]+"', "+row[3]+");")

total_addresses=addresses.address.count()
all_addresses=[]

current_level=""
in_depth=""

max_reached=0

for index, row in addresses.iterrows():
        if (processed>=config.max_addresses):
                print "Max addresses ("+str(config.max_addresses)+") processed, exiting...\n"
		max_reached=1
                break;
	load_addr(row['address'])

current_level=0
in_depth="F"
for x in range(0, config.move_forward):
	current_level+=1
	rw="Select distinct to_addr,sum(amount_sent) from "+filename+"transactions where to_addr not in (select address from "+filename+"addresses) group by to_addr order by sum(amount_sent) desc limit "+str(config.addresses_per_level)+";"
	cur.execute(rw)
        new_list=cur.fetchall()
        for ad in new_list:
	        if (processed>=config.max_addresses):
        	        if not max_reached:
				print "Max addresses ("+str(config.max_addresses)+" processed, exiting...\n"
			max_reached=1
			break;
		load_addr(ad[0])

in_depth="B"
for x in range(0, config.go_backward):
	current_level+=1
        cur.execute("Select distinct from_addr,sum(amount_sent) from "+filename+"transactions where from_addr not in (select address from "+filename+"addresses) group by to_addr order by sum(amount_sent) desc limit "+str(config.addresses_per_level)+";")
        new_list=cur.fetchall()
        for ad in new_list:
                if (processed>=config.max_addresses):
                        if not max_reached:
				print "Max addresses ("+str(config.max_addresses)+" processed, exiting...\n"
			max_reached=1
                        break;
                load_addr(ad[0])

save_tx_csv("output/"+filename+"transactions.csv")

if config.change_addresses:
        save_addr_csv(original_file,1)
else:
	save_addr_csv("output/"+filename+"change_addresses.csv",1)

save_addr_csv("output/"+filename+"all_addresses.csv",2)

print "Done!"

