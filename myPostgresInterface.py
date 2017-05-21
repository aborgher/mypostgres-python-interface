import psycopg2,csv,sys
import timeit
import datetime
# import osgeo.ogr
# import shapefile
import pandas as pd

class myPostgresInterface:
    def __init__(self,dbname,user,host,psw):
        self.dbname = dbname
        self.user = user
        self.host = host
        self.psw = psw
        self.conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbname,user,host,psw))
        self.cur = self.conn.cursor()
        self.out = ""

    def close_connection(self):
        self.conn.close()

    def reconnect(self):
        self.close_connection()
        self.conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(
                self.dbname,self.user,self.host,self.psw))
        self.cur = self.conn.cursor()
        
    def get_dbname(self):
        return self.dbname

    def get_output(self,verbose=True):
        out_ret = ""
        try: 
            self.out = self.cur.fetchall()
            if verbose: [print(k) for k in self.out]
        except psycopg2.ProgrammingError: 
            print(end="")
        out_ret = self.out
        return out_ret
    
    def send_psql(self,psql="",verbose=True):
        start = timeit.default_timer()
        special_psql = ['create table ', 'create or replace function']
        if( not (sum([f in psql.lower() for f in special_psql]) > 0) and ('select ' in psql.lower()) ): 
            r = pd.read_sql_query(psql, self.conn)
        else:
            self.cur.execute(psql)
            self.conn.commit()
            r=''            
        stop = timeit.default_timer()
        if(verbose):
            print("time elapsed", str(datetime.timedelta(seconds=stop - start)), "sec")
        return r
    
    def change_col_name(table, old_col, new_col):
        send_psql("""ALTER TABLE {0} RENAME {1} TO {2}""".format(table, old_col, new_col), False)
    
    def explain_psql(self,psql):
        return(self.send_psql("explain "+psql))

    def kill_pid_process(self,pid):
        self.dbname = self.get_dbname()
        self.send_psql('''SELECT pg_terminate_backend({0}) FROM pg_stat_activity 
        WHERE pid <> pg_backend_pid() AND datname = '{1}';'''.format(pid,self.dbname))
        print("Process",pid,"killed")
        sys.stdout.flush()

    def get_status_db(self, summary = True):
        self.reconnect()
        st_act = {"datid":0,"datname":1,"pid":2,"usesyid":3,"username":4,"application_name":5,"client_addr":6,
                  "client_hostname":7,"client_port":8,"backend_start":9,"xact_start":10,"query_start":11,"state_change":12,
                  "wait_event_type":13,"wait_event":14,"state":15,"backend_xid":16,"backend_xmin":17,"query":18}
        a = self.send_psql('''select * from pg_stat_activity where datname = '{0}' '''.format(self.dbname),False)
        sys.stdout.flush()
        if summary: return a[ ['pid', 'state', 'query'] ]
        else: return a

    def get_column_name(self, tablename,verbose=True):
        return(self.send_psql('''select column_name from information_schema.columns where
                              table_name='{0}';'''.format(tablename),verbose))
    
    def get_first_rows(self, tablename,nrows=5,verbose=True):
        return(self.send_psql('''select * from {0} limit {1};'''.format(tablename,str(nrows)),verbose))

    def get_function_infos(self, funcname,verbose=True):
        return(self.send_psql("select pg_get_functiondef(oid) from pg_proc where proname = '{0}';".format(funcname),verbose)[0][0])
    
    def get_all_table_status(self, schema='public',verbose=True):
        return(self.send_psql("""SELECT relname, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze  
                                 FROM pg_stat_all_tables WHERE schemaname = '{0}'; """.format(schema),verbose))

    def create_csv_from_table(self, tablename, verbose = True):
        return(self.send_psql("""COPY  {0}  TO  'D:\Box_percorrenze\{0}.csv'  
                                 DELIMITER  ',' CSV HEADER""".format(tablename),verbose))

    def get_size_table(self, tablename, verbose=True):
        return(self.send_psql("select pg_size_pretty(pg_relation_size('{0}'))".format(tablename),verbose))

    # def create_table_from_shapefile(self, srcFile, tablename, createTable=True):
    #     sf = shapefile.Reader(srcFile)
    #     map_type = {'C':'character varying({})', 'N': 'NUMERIC({})'}
    #     all_vars = [str(f[0]) + ' '  + map_type[f[1]].format(f[2]) for f in sf.fields[1:]]
    #     if(createTable):
    #         self.cur.execute('CREATE TABLE %s (id serial,geom geometry, %s);' % (tablename, ' ,'.join(all_vars)))
    #         self.cur.execute('CREATE INDEX %s_gix on %s USING GIST (geom)' % (tablename, tablename))
    #         self.conn.commit()
    #     return [f[0] for f in sf.fields[1:]]

    # def import_shapefile(self, srcDir, createTable = True):
    #     start = timeit.default_timer()
    #     sfosgeo = osgeo.ogr.Open(srcDir)
    #     layer = sfosgeo.GetLayer(0)   
    
    #     # create table 
    #     tablename = layer.GetName()
    #     names = self.create_table_from_shapefile(srcDir + '\\' + tablename, tablename, createTable)
    #     # fill the table
    #     f = layer.GetFeature(0)
    #     n = f.GetFieldCount()    
    #     count = 0
    #     for i in range(layer.GetFeatureCount()):
    #         count += 1
    #         f = layer.GetFeature(i)
    #         wkt = f.GetGeometryRef().ExportToWkt()
    #         vals = [f.GetField(j) for j in range(0,n)]
    #         text = ''
    #         for j in vals:
    #             if j == None:
    #                 add = 'NULL'
    #             else:
    #                 add = "'" + j.replace("'","") + "'" if (type(j) == str) else j
    #             text += str(add) + ', '
    #         text = text[0:-2]
    #         self.cur.execute("""INSERT INTO {0} (geom, {1})
    #                           VALUES (ST_GeometryFromText('{2}', 4326), {3})
    #                           """.format(tablename, ', '.join(names), wkt, text) )
    #         if count % 200000 == 0: 
    #             self.conn.commit()
    #     self.conn.commit()
    #     stop = timeit.default_timer()
    #     print("time elapsed", str(datetime.timedelta(seconds=stop - start)), "sec")

    def send_vacuum(self, query):
        old_isolation_level = self.conn.isolation_level
        self.conn.set_isolation_level(0)
        self.cur.execute(query)
        self.conn.commit()
        self.conn.set_isolation_level(old_isolation_level)

    def get_size_db(self):
        self.send_psql("SELECT pg_size_pretty(pg_database_size('box'))", False)

    def get_size_all_table(self):
        for i in self.get_all_table_status(verbose=False).relname.values:
            print(i, self.get_size_table(i,False).pg_size_pretty.values)
    
    
    
    

