from dataclasses import dataclass
from ..dbbackups import BackupJobs,dbmodel
from ..settings import FAILED_BKP_DIR

@dataclass
class ivrs_log_backup(BackupJobs):
    fromdbname:str = 'localpg'
    todbname  :str = 'oraclesdc'    
    FromTable: str  =   'ivrs_basic_log'
    ToTable: str    =   'ivrs_basic_log_test'
    ClassOfJob: str = 'test'
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
           
        




        



