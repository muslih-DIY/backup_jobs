from dataclasses import dataclass
from ..dbbackups import BackupJobs,dbmodel
from ..settings import localpg,oraclesdc



@dataclass
class cdr_bkp_198(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'cdr'
    ToTable: str    =   'cdr'
    ClassOfJob:str = '198'    
    def __post_init__(self):

        self.FromColumn =[
            'calldate', 'clid', 'src', 'dst', 'dcontext', 'channel', 'dstchannel', 'lastapp',
            'lastdata', 'duration', 'billsec', 'disposition', 'amaflags','accountcode', 
            'uniqueid', 'userfield', 'phoneno', 'callstage', 'zone', 'region_no', 'std_code',
            'docket_id', 'comp_flag', 'container_id', 'transid']
        self.ToColumn   = [
            'ENTRY_DATE','CALLDATE', 'CLID', 'SRC', 'DST', 'DCONTEXT', 'CHANNEL', 'DSTCHANNEL', 'LASTAPP',
            'LASTDATA', 'DURATION', 'BILLSEC', 'DISPOSITION','AMAFLAGS', 'ACCOUNTCODE',
            'UNIQUEID', 'USERFIELD', 'PHONENO', 'CALLSTAGE', 'ZONE', 'REGION_NO', 'STD_CODE',
            'DOCKET_ID', 'COMP_FLAG', 'CONTAINER_ID','TRANSID']

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = ['uniqueid','transid']

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f"update {self.FromTable} set deletflag=1 where serviceflag='0' and deletflag=0",)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where deletflag=1 and '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 

        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1 and and serviceflag='0'",) 
        
        super().__post_init__()


@dataclass
class cdr_bkp_app(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'cdr'
    ToTable: str    =   'CDR_APPEAL'
    ClassOfJob:str = '198'     
    def __post_init__(self):

        self.FromColumn =[
            'calldate', 'clid', 'src', 'dst', 'dcontext', 'channel', 'dstchannel', 'lastapp', 'lastdata',
            'duration', 'billsec', 'disposition', 'amaflags','accountcode','uniqueid', 'userfield',
            'phoneno', 'callstage', 'zone', 'region_no', 'std_code','container_id', 'transid',
            'app_in_flag', 'complaintid', 'appno','ucomplaintid', 'ubookingdate', 'rcomplaintid',
            'rbookingdate', 'rclosedate', 'appflag']
        self.ToColumn   = [
            'APPSOURCE','ENTRY_DATE','CALLDATE', 'CLID', 'SRC', 'DST', 'DCONTEXT', 'CHANNEL', 'DSTCHANNEL','LASTAPP', 'LASTDATA',
            'DURATION', 'BILLSEC', 'DISPOSITION', 'AMAFLAGS','ACCOUNTCODE', 'UNIQUEID', 'USERFIELD',
            'PHONENO', 'CALLSTAGES','ZONE','REGION_NO', 'STD_CODE','CONTAINER_ID', 'TRANSID','APP_IN_FLAG',
            'COMPLAINTID','APPNO', 'UCOMPLAINTID', 'UBOOKINGDATE', 'RCOMPLAINTID', 'RBOOKINGDATE',
            'RCLOSEDATE', 'APPFLAG']

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = ['uniqueid','transid']

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f"update {self.FromTable} set deletflag=1 where serviceflag='1' and deletflag=0",)

        self.pull_query=f"select '198',now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where deletflag=1 and '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 

        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1 and and serviceflag='1'",) 
        
        super().__post_init__()



@dataclass
class api_bkp_dock(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_api_log'
    ToTable: str    =   'IVRS_API_LOG'
    ClassOfJob:str = '198'     
    def __post_init__(self):

        self.FromColumn =[
            'api_date', 'api_zone', 'api_status', 'input', 'output', 'error', 'userfield',
            'std_code', 'docket_id', 'comp_flag', 'transid']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = ['api_date','transid']

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()

@dataclass
class api_bkp_app(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_appeal_api'
    ToTable: str    =   'APP_API_1500'
    ClassOfJob:str = '198'     
    def __post_init__(self):

        self.FromColumn =[
            'api_date', 'transid', 'zone', 'input_number', 'response', 'input_flag', 'input_compid',
            'resflag', 'appno']

        self.ToColumn   =  ['APPSOURCE','ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = ['api_date','transid']

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select '198',now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()
