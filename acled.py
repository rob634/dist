from datetime import datetime
import requests
import pandas as pd

from databases import PostgreSQL #database connection class
from logger import log
from acled_lib import acled_attrs, acled_df_blank, acled_df_dtype, new_acled_sqlexp

requests.packages.urllib3.disable_warnings() 

class ACLED(object):
#Takes a PostgreSQL instance and uploads ACLED data from their http api

    def __new__(cls, *args, **kwargs):

        return super().__new__(cls)
    
    def __init__(self,**db_params):
        
        self.db = db_params['db'] #PostgreSQL instance
        self.schema = db_params['schema']
        self.creds = db_params['creds']
        self.table_name = db_params['table_name']
        self.attrs = acled_attrs#column names and corresponding data types and attributes
        self.dtypes = acled_df_dtype#dtype dictionary for DataFrame.to_sql()

        log('ACLED object instantiated with database connection to {}'.format(self.db.database),'green')
        if self.table_exists():
            self.latest_in_db()
        else:
            log('No ACLED table found in {}.{}'.format(self.db.database,self.schema))
    
    def table_exists(self,schema=None,table_name=None):
        #Checks to see if acled table with table_name exists in database returns boolean
        if not isinstance(schema,str):
            schema = self.schema
        if not isinstance(table_name,str):#by default this function updates the instance attribute boolean has_table
            table_name = self.table_name
            instance = True
        exp = "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_catalog='{}' AND table_schema='{}' AND table_name='{}');".format(self.db.database,schema,table_name)
        has_table = self.db.query(exp).fetchone()[0]
        if instance:
            self.has_table = has_table
        return has_table
    
    def latest_in_db(self,schema=None,table_name=None):
        #update instance attributes describing acled table size and most recent records
        if not isinstance(schema,str):
            schema = self.schema
        if not isinstance(table_name,str):
            table_name = self.table_name
        self.latest_timestamp = self.db.query('''select max(timestamp) from {}.{};'''.format(schema,table_name),v=True).fetchall()[0][0]
        self.db_count = self.db.query('''select count(event_id_cnty) from {}.{};'''.format(schema,table_name),v=True).fetchall()[0][0]
        log('{:,} records in {} database - most recent timestamp {}'.format(self.db_count,self.db.database,datetime.fromtimestamp(self.latest_timestamp).strftime('%m/%d/%Y, %H:%M'),'blue'))

    def update_date(self,schema=None,table_name=None,v=False):
        #update the date field in the database dable
        if not isinstance(schema,str):
            schema = self.schema
        if not isinstance(table_name,str):
            table_name = self.table_name
        if v:
            log('Updating date field')
        try:
            r = self.db.query('''update {}.{} set "date" = TO_DATE(event_date,'YYYY-MM-DD');'''.format(schema,table_name),v=v)
            if v:
                log('Date field updated','green')
        except Exception as e:
            log(str(e),'red')
        return r

    def get_page_as_df(self,limit=5000,page=1,v=False):
        #Queries ACLED URL and returns results as DataFrame
        url_base = '{}read?key={}&email={}&limit={}&page={}'.format(self.creds['url_base'], self.creds['key'], self.creds['username'], limit, page)
        log('Querying {:,} records from ACLED page {}'.format(limit,page))
        if v:
            log('{}'.format(url_base))
        response = requests.get(url_base,verify=False).json()
        if response['success']:
            result_count = response['count']
            if result_count == 0:
                log('ACLED query returned no records','yellow')
                return None
            log('ACLED query succesful - {:,} records returned'.format(result_count),'green')
            df_results = pd.json_normalize(response['data'])
            log('Query converted to data frame','green')
            if v:
                log('Updating attribute lengths')            
            #Check the length of all varchar values in df to update instance max length attributes
            for field in self.attrs.keys():
                if 'max_len' in self.attrs[field].keys():
                    m = max([len(i) for i in list(df_results[field])])
                    if m > self.attrs[field]['max_len']:
                        self.attrs[field]['max_len'] = m
            return df_results
        else:
            log('ACLED query failure',color='red')
            log(str(response))

    def update_acled_table(self,schema=None,table_name=None,page_start=1,page_end=None,copy=None,v=False):
        '''iterates through pages in ACLED API and builds a dataframe of records 
        with ids not currently in the database then appends that dataframe to the
          database table 
          if no page end is specified, function continues until no records are returned
            from ACLED API call'''
        if not isinstance(schema,str):
            schema = self.schema
        if not isinstance(table_name,str):
            table_name = self.table_name
        if v:
            log('Querying ACLED ids from {}.{}.{}'.format(self.db.database,schema,table_name))
        #df_ids = pd.DataFrame(self.db.query('''select event_id_cnty from {}.{};'''.format(schema,table_name),v=v).fetchall())
        df_ids = self.db.query_to_DF('''select event_id_cnty from {}.{};'''.format(schema,table_name),v=v)
        if 'event_id_cnty' in df_ids.axes[1]:
            db_ids_set = list(df_ids['event_id_cnty'])
        else:
            log('No ACLED data found in {} {}.{}'.format(self.db.database,schema,table_name))
            return None
        if v:
            log('{:,} ACLED ids found in {}.{}.{}'.format(len(db_ids_set),self.db.database,schema,table_name),'blue')
        df_update = acled_df_blank#create blank df with acled columns
        p = page_start
        repeat = True
        new_count = None
        ends = isinstance(page_end,int)
        while repeat:#iteration through pages in ACLED API
            df_loop = self.get_page_as_df(limit=5000,page=p,v=True)
            if not isinstance(df_loop,pd.DataFrame):#get_page_as_df returns None when no records are returned from ACLED API triggering end of loop
                if isinstance(new_count,int):#if new_count is an integer, it means that new ACLED records have been retrieved
                    log('End of ACLED Pages {}'.format(p),'blue')
                else:
                    log('No DataFrame returned','red')
                repeat = False
                break
            acled_ids = list(df_loop['event_id_cnty'])
            new_acled_ids = list(set(acled_ids).difference(db_ids_set))
            df_new = df_loop.loc[df_loop['event_id_cnty'].isin(new_acled_ids)]
            if v:
                log('-----{:,} ACLED records on page {} found not in database'.format(len(new_acled_ids),p),'blue')
            df_update = pd.concat([df_update,df_new])
            new_count = len(df_update['event_id_cnty'])
            if v:
                log('{:,} total new records found after {} pages'.format(new_count,p), 'blue')
            p += 1
            if ends:
                if p>page_end:
                    repeat = False
        if isinstance(new_count,int):
            if new_count > 0:
                log('      {:,} new records found in {} pages'.format(new_count,p-1),'blue')
                log('Updating {}.{}.{}'.format(self.db.database,schema,table_name))
                #df_update.to_csv('{}.csv'.format('update20nov'))
                r = df_update.to_sql(name=table_name,con=self.db.engine,schema=schema,dtype=self.dtypes,if_exists='append',index=False,chunksize=10000)
                if isinstance(copy,list):#to handle additional schema but not yet tested, will likely violate unique ID when uploading to db
                    log('Updating {} additional schemas'.format(len(copy)),'yellow')
                    for s in copy:
                        log('    Updating {}.{}'.format(self.db.database,s,table_name))
                        r = df_update.to_sql(name=table_name,con=self.db.engine,schema=s,dtype=self.dtypes,if_exists='append',index=False,chunksize=10000)
                self.update_date(v=v)
                self.latest_in_db()
                log('Update of acled table complete','green')
                log('------------------------------','green')
                return r
        else:
            log('No new records found in {} pages'.format(p),'yellow')
                
    def __del__(self):
        self.db.__del__() 
