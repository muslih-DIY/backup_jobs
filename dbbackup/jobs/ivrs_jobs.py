
from dataclasses import dataclass
from ..dbbackups import BackupJobs
from ..settings import FAILED_BKP_DIR

@dataclass
class ivrs_log_backup(BackupJobs):
    fromdbname:str = 'localpg'
    todbname  :str = 'oraclesdc'    
    FromTable: str  =   'ivrs_basic_log'
    ToTable: str    =   'ivrs_basic_log_test'
    ClassOfJob: str = 'ivrs'
    BACKUPDIR: str = FAILED_BKP_DIR  
    def __post_init__(self):

        self.FromColumn = ['api_date','agi_service','contid','transid','src','inputs','status_code','res_flag','responses']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn
        self.fromcol  =   ','.join(self.FromColumn)
        self.pre_pull_querys = (f'update {self.FromTable} set deleteflag=1',)
        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deleteflag=1"
        self.error_update_query = f'update {self.FromTable} set deleteflag=3'
        # self.final_post_push_querys = (f'delete from {self.FromTable} where deleteflag=1',) 
        self.unique_keys = ['api_date','transid']
        super().__post_init__()

@dataclass
class CDR_IVRS(BackupJobs):
    fromdbname:str = 'localpg'
    todbname  :str = 'oraclesdc'  
    FromTable: str  =   'cdr'
    ToTable: str    =   'cdr_cdr3_test'
    ClassOfJob:str = 'ivrs'  
    BACKUPDIR: str = FAILED_BKP_DIR  
    def __post_init__(self):

        self.FromColumn =[
            'calldate ','src','duration','billsec','disposition','userfield','container_id','transid','main_menu',
            'mains_service','identfy_other','std_code','phoneno','mobileno','other_input','zone','region_no',
            'other ','agi_service','agi_flag','agi_response ','agi_input ','agi_other ','agent_orgtime ',
            'agent_ccgroup ','agent_cat ','agent_other']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = ['agent_orgtime','transid']

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        # self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()

