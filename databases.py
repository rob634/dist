import traceback

from pandas import DataFrame
import psycopg2
from sqlalchemy import create_engine, sql, types

from logger import log

class PostgreSQL(object):


    def __new__(cls, *args, **kwargs):
        return super().__new__(cls)
    
    def __init__(self,**db_params):
        if len(db_params.keys()) > 0:
            self.driver = 'psycopg2'
            self.username = db_params['username']
            self.password = db_params['password']
            self.host = db_params['host']
            self.port = db_params['port']
            self.database = db_params['database']
            self.dialect = 'postgresql'
            self.conn_str = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(self.username,self.password,self.host,self.port,self.database)
            self.engine = create_engine(self.conn_str,pool_pre_ping=True)
            log('Database connection to {} established'.format(self.database),'green')
        else:
            log('Incorrect parameters passed','red')
            self.__del__
    
    def __del__(self):
        if hasattr(self,'host') and hasattr(self,'conn'):
            log('Closing down {}'.format(self.host)) 
            self.conn.close()
        log('Closed down')  
        
    @classmethod
    def convert_dbapi(self,x,L=None):
        #converts Python data types to DBAPI data types used by SQLalchemy
        if x == str:
            return types.VARCHAR(L)
        elif x == int:
            return types.INTEGER()
        elif x == float:
            return types.NUMERIC()

    def query(self,exp,v=False):
        #executes query in exp and returns sqlalchemy cursor result
        try:
            if v:
                log('Executing SQL: {}'.format(exp[:1500]))
            with self.engine.connect() as conn:
                self.result = conn.execute(sql.text(exp))
                conn.commit()
            if v:
                log('Query executed','green')
            return self.result
        except Exception as e:
            log('Query error: {}'.format(str(e)),'red')
            log(traceback.format_exc())
            return None
        
    def query_to_DF(self,exp,v=False):
        #executes the query method and creates a DataFrame from the results
        self.result = self.query(exp,v)
        log('Building DataFrame')
        self.df = DataFrame(self.result.fetchall())
        log('DataFrame complete','green')
        return self.df
    
    def df_to_db(self,table_name,s,d,e=None,df=None,v=False):
        #uploads DataFrame parameter or instance attribute
        if not e:
            e = self.engine
        if not isinstance(df,DataFrame) and hasattr(self,'df'):
            log('DB Using instance DataFrame','yellow')
            df = self.df
        log('DB Uploading dataframe length {} to {}.{}.{}'.format(len(df),self.database,s,table_name))
        try:
            r = df.to_sql(name=table_name,con=c,schema=s,dtype=d,if_exists='append',index=False,chunksize=10000)
            log('DB Upload complete {}'.format(str(r)))
            return r
        except Exception as e:
            log('Upload to database error for table {} : {}'.format(table_name,str(e)),'red')
            log(traceback.format_exc(),'red')