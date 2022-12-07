from dataclasses import dataclass,field
from abc import ABC
from typing import List,Callable
from datetime import datetime
import csv
from pathlib import Path
import os

_BASE_DIR = Path(__file__).resolve().parent


class dbmodel(ABC):

    def connect():
        ...
    def close():
        ...
    def execute_many():
        ...
    def insert_many_list():
        ...
    def execute():
        ...
    def select():
        ...
    def insert():
        ...
    def update():
        ...
    def dict_insert():
        ...
        
@dataclass
class BackupJobs(ABC):
    
    fromdbname:str
    todbname  :str
    ClassOfJob:str
    ToTable :   str
    FromTable   : str

    pull_query  :   str     =   field(init=False) 
    FromColumn  :   List    =   field(default_factory=list)
    ToColumn    :   List    =   field(default_factory=list)
    pre_pull_querys :   List=   field(init=False,default_factory=list) 
    final_post_push_querys:   List=   field(init=False,default_factory=list) 
    unique_keys :  List=   field(default_factory=list)  
    post_push_error_handle_failed_querys:   List=field(init=False,default_factory=list) 
    post_push_error_handle_success_querys:   List=field(init=False,default_factory=list)
    backup_status_table:str =   'BACKUP_STATUS'
    BACKUPDIR : str = os.path.join(_BASE_DIR, 'FailedJobs')
    is_disabled : bool = False
    debug : bool = False

    _registry = {}
    
    @staticmethod
    def register_class(cls):
        if not cls.ClassOfJob in cls._registry.keys():
            cls._registry[cls.ClassOfJob]={}
        cls._registry[cls.ClassOfJob][cls.__name__] = cls
        return
        

    def __init_subclass__(cls,**kwargs):
        super().__init_subclass__()
        cls.register_class(cls)
    
    @classmethod
    def get_acivte_jobs(cls,ClassOfJob:str):
        return {name:job for name,job in cls._registry[ClassOfJob].items() if not job.is_disabled}

    def __post_init__(self):

        self.job_name = 'backup_'+ self.FromTable +'_to_'+self.ToTable
        self.job_report = ''.join(['\tREPORT\n','= ='*10])


        
    def __call__(self,getdb:Callable,debug=False):
        "Need to pass a function 'getdb' which gives the dbconnection"
        self.debug = debug
        self.FromDb:dbmodel = getdb(self.fromdbname)
        self.ToDb:dbmodel =  getdb(self.todbname)
        if not self.FromDb:
            raise Exception('from database is null')
        if not self.ToDb:
            raise Exception('to database is null')

        self.FromDb.connect()
        self.ToDb.connect() 
        status = self.run()
        if self.debug:
            print(self.job_report)
        self.ToDb.close()
        self.FromDb.close()
        return status
        


    def get_error_row_unique_keys(self,error):        
        error_row_unique = []
        for row,msg in error:
            if self.debug:
                print(row,msg)
            error_row_unique.append({key:row[idx] for key,idx in self.indexkey})
        return error_row_unique
    
    def manage_error(self,error):
        """update errored rows deletflag to 3 """
        if not error:
            return 1
        error_row_unique=self.get_error_row_unique_keys(error)

        self.backup_update(1,f"Errors Found:{len(error)}")

        if not self.FromDb.execute_many(self.error_update_query,error_row_unique):
            return 0
            
        return 1
    def update_report(self,*args):
        self.job_report='\n'.join([self.job_report,''.join([str(arg) for arg in args])])

    def getandupdate_unique_key_index(self,head:list=None,keys:list=None):
        if head is None:head = self.FromColumn
        if keys is None:keys = self.unique_keys
        self.indexkey =[(k,head.index(k)) for k in keys]
        return self.indexkey

    def run_query_set(self,queryset:str):
        
        query_list = self.__getattribute__(queryset)
        for query in query_list:
            if not self.FromDb.execute(query):
                return 0
        return 1
    
    def save_to_csv(self,head: list,data:list):
        filename = ''.join([self.BACKUPDIR,'/',self.job_name,datetime.now().strftime('%Y%M%d-%H%M'),'.csv'])
        with open(filename,"w") as logfile:
            csvfile_head = csv.writer(logfile)
            csvfile_head.writerow(head)
            csvfile_head.writerows(data)
        return filename
    
    def backup_update(self,gravity:int=0,remarks:str=''):
        backup_status_table = self.backup_status_table
        status = {'BACKUP_DAY':datetime.now(),
            'NAME':self.job_name[:100],
            'FAILURE_GRAVITY' : gravity,
            'REMARK' :remarks[:127]}
        print(status)
        self.ToDb.dict_insert(status,table=backup_status_table)
        print(self.ToDb.error)
        self.FromDb.dict_insert(status,table=backup_status_table)
        print(self.FromDb.error)


    def run(self) -> bool:

        if not self.run_query_set('pre_pull_querys'):
            self.job_report='pre_pull_query_execution failed'
            self.update_report('ErrorDeatils : ',self.FromDb.error)
            return 0
        records,succses,head = self.FromDb.select(self.pull_query,header=True)
        
        if not succses:
            self.update_report('records collection fromdb failed')
            self.update_report('ErrorDeatils : ',self.FromDb.error)
            return 0

        self.update_report(f"Total Records to backup : {len(records)}")

        if not records:
            self.update_report(" No records , Job Completed Successfully\n",'= ='*10)      
            return 1
        self.getandupdate_unique_key_index(head=head)

        errors = self.ToDb.insert_many_list(self.ToTable,self.ToColumn,records,batcherrors=True)

        self.update_report(f"Total backup failed count : {len(errors)}")
        
        # if error managment failed
        if not self.manage_error(errors):
            filename = self.save_to_csv(self.ToColumn,map(lambda row:row[0],errors))
            
            self.update_report(f"managing error failed,error data stored to : {filename}")
            
            self.backup_update(2,f"managing error failed see {filename}")
            
            self.run_query_set('post_push_error_handle_failed_querys')                
        else:
            
            self.update_report("error_handling is succeded")
            
            self.run_query_set('post_push_error_handle_success_querys')
        
        self.update_report("final_post_push_querys is running ...")

        if not self.run_query_set('final_post_push_querys'):
            
            self.update_report("Job Failed at final_post_push_querys ")
            self.backup_update(0,f"Job Failed at final_post_push_querys")
            
            return 0
        
        self.update_report("Job Completed Successfully\n",'='*10)
        
        return 1

            

