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
		print "Adding rate for today"
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
                cur.execute("CREATE TABLE "+tble+" (address VARCHAR(255) PRIMARY KEY, updated TIMESTAMP, tx_count INTEGER, last_hash VARCHAR(255))")
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
			    "amount_sent bigint, fee bigint, relayed_IP VARCHAR(255), is_utxo boolean, notes VARCHAR(255), messages VARCHAR(255));")
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
	global initial_total_addresses

	tx_count=0

	all_addresses=[addr]+all_addresses
	
	tx_left=1
	valid_address=0
	offset_id=0
	max_reached=0
	processed_count=0

	while (1):
	   if max_reached:
		break
	
	   if (tx_count==0):
	        try:
			address = blockexplorer.get_address(addr,api_code=config.api_code)
			valid_address=1
		except KeyboardInterrupt:
			cleanexit()
		except Exception, e:
			print str(e)+" Could not get address: "+addr+" Skipping..."
			valid_address=0
			break
		cur.execute("SELECT tx_count FROM "+filename+"addresses WHERE address='"+addr+"';")
		try:
			old_n_tx=cur.fetchone()[0]
			if (old_n_tx):
				if old_n_tx<address.n_tx:
					if (config.max_transactions<50 and config.max_transactions>0):
						margin=config.max_transactions
					else:
						margin=50
					tx_count=old_n_tx-margin
			                tx_left=address.n_tx-tx_count
					print "Resuming from tx # "+str(tx_count)+" for "+addr
				else:
					print "All transactions for "+addr+" have already been fetched. Skipping..."
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

                print in_depth+str(current_level)+"TX:",tx_count,transaction.hash,"at"+str(tx_time)+" ("+str(tx_count)+"/"+str(address.n_tx)+")"

        	if (config.direction!=2):
          		for input in transaction.inputs:
				try:
                                   if input.address is None:
                                        if (input.script.startswith("6a")):
                                                decoded_msg=input.script[2:].decode("hex")
                                                decoded_msg=''.join(x for x in decoded_msg if x in string.printable)
                                                decoded_msg=decoded_msg.replace('\r',' ').replace('\n',' ')
                                                encoded_messages=encoded_messages+decoded_msg+"\n"
					else:
						continue
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
				except:
				   pass
                if (config.direction!=1):
                        for output in transaction.outputs:
				if output.address is None:
					if (output.script.startswith("6a")):
						decoded_msg=output.script[2:].decode("hex")
						decoded_msg=''.join(x for x in decoded_msg if x in string.printable)
						decoded_msg=decoded_msg.replace('\r',' ').replace('\n',' ')
						encoded_messages=encoded_messages+decoded_msg+"\n"
					else:
						continue
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

#		if transaction.hash=="f614779da7278f5d04684d97dd09d4471be4ba50a32a37006d906f427626a93d":
#			print "rtx:"+str(receiving_tx)+" msa:("+main_sending_address+") '"+addr+transaction.hash+"', '"+main_sending_address+"', '"+addr+"', '"+str(tx_time)+"', "+str(received_amount-sent_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo))+",'To "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
#			cleanexit()
	   	if (receiving_tx):
			if not main_sending_address:
				main_sending_address="Miner reward"
				fee=0
			if main_sending_address!=addr:
                		row="'"+addr+transaction.hash+"', '"+main_sending_address+"', '"+addr+"', '"+str(tx_time)+"', "+str(received_amount-sent_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo))+",'To "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                        	cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")
			elif sent_amount<received_amount:
                                row="'"+addr+transaction.hash+"', '"+main_sending_address+"', '"+addr+"', '"+str(tx_time)+"', "+str(received_amount-sent_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo))+",'To "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                                cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")
				
                if (sending_tx):
			if main_receiving_address!=addr:
                        	row="'"+addr+transaction.hash+"', '"+addr+"', '"+main_receiving_address+"', '"+str(tx_time)+"',"+str(sent_amount-received_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo_i))+", 'From "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                        	cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")
			elif sent_amount>received_amount:
                                row="'"+addr+transaction.hash+"', '"+addr+"', '"+main_receiving_address+"', '"+str(tx_time)+"',"+str(sent_amount-received_amount)+", "+str(fee)+", '"+transaction.relayed_by+"', "+str(bool(is_utxo_i))+", 'From "+filename[:-1]+"', '"+encoded_messages.replace("'","''")+"'"
                                cur.execute("INSERT INTO "+filename+"transactions VALUES("+row+") ON CONFLICT (tx_i) DO NOTHING;")

                cur.execute("INSERT INTO "+filename+"addresses VALUES('"+addr+"',NOW(),"+str(tx_count)+",'"+transaction.hash+"') ON CONFLICT (address) DO UPDATE SET updated=NOW(),tx_count="+str(tx_count)+", last_hash='"+transaction.hash+"';")
	   	if (found_change_addresses):
			all_addresses=list(set(all_addresses)|set(change_addresses))
			all_addresses.remove(addr)
			all_addresses=[addr]+all_addresses
		processed_count+=1
                if (config.max_transactions!=0 and processed_count>=config.max_transactions):
                        print "Reached max transactions",config.max_transactions,"... skipping"
                        max_reached=1
			break

	if valid_address:
   		processed+=1
		if (config.max_addresses!=0 and initial_total_addresses>=config.max_addresses):
			initial_total_addresses=config.max_addresses	
		perc=(100*processed)/initial_total_addresses;
		print "Processed address ("+str(processed)+"/"+str(initial_total_addresses)+") [%"+str(perc)+"]";
	
	return
###

################### END OF FUNCTIONS #########################

# Some global variable initialisations

addresses=pd.DataFrame([0])
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
		with open("cases/"+sys.argv[1]) as f:
		    addresses = [line.rstrip() for line in f]        
		initial_total_addresses=len(addresses)
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
		if time.time()-time_modified>43200: #more than 12 hours have passed since update
                        print "Updatng rates file from bitcoinaverage.com!"
			update_rates_file()
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

total_addresses=len(addresses)
all_addresses=[]

current_level=""
in_depth=""

max_reached=0

for row in addresses:
        if (config.max_addresses!=0 and processed>=config.max_addresses):
                print "Max addresses ("+str(config.max_addresses)+") processed, exiting...\n"
		max_reached=1
                break;
	load_addr(row)

current_level=0
in_depth="F"
for x in range(0, config.move_forward):
	current_level+=1
	rw="Select to_addr,sum(amount_sent) from "+filename+"transactions where to_addr not in (select address from "+filename+"addresses) group by to_addr order by sum(amount_sent) desc limit "+str(config.addresses_per_level)+";"
	cur.execute(rw)
        new_list=cur.fetchall()
        for ad in new_list:
	        if (config.max_addresses!=0 and processed>=config.max_addresses):
        	        if not max_reached:
				print "Max addresses ("+str(config.max_addresses)+" processed, exiting...\n"
			max_reached=1
			break;
		load_addr(ad[0])

in_depth="B"
current_level=0
for x in range(0, config.go_backward):
	current_level+=1
        cur.execute("Select from_addr,sum(amount_sent) from "+filename+"transactions where from_addr not in (select address from "+filename+"addresses) group by from_addr order by sum(amount_sent) desc limit "+str(config.addresses_per_level)+";")
        new_list=cur.fetchall()
        for ad in new_list:
                if (config.max_addresses!=0 and processed>=config.max_addresses):
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

