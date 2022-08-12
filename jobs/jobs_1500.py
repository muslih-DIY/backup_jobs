
from dataclasses import dataclass
from ..dbbackups import BackupJobs,dbmodel
from ..settings import localpg,oraclesdc


@dataclass
class CDR_1500_RMN(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'cdr'
    ToTable: str    =   'CDR_RMN'
    ClassOfJob:str = '1500'
    def __post_init__(self):

        self.FromColumn = [
            'CALLDATE','CONTAINER_ID','CLID','SRC','DST','DCONTEXT','CHANNEL','DSTCHANNEL','LASTAPP',
            'LASTDATA','DURATION','BILLSEC','DISPOSITION','AMAFLAGS','ACCOUNTCODE','UNIQUEID','USERFIELD',
            'PHONENO','CALLSTAGES','ZONE','REGION_NO','STD_CODE','RMN_FLAG','TRANSID']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['UNIQUEID','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1 and CALLSTAGES like '%RMN'"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 

        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1 and CALLSTAGES like '%RMN'",) 
        
        super().__post_init__()


@dataclass
class CDR_1500_APPEAL(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc
    FromTable: str  =   'cdr'
    ToTable: str    =   'CDR_APPEAL'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =[
            'CALLDATE','CONTAINER_ID','CLID','SRC','DST','DCONTEXT','CHANNEL','DSTCHANNEL','LASTAPP','LASTDATA',
            'DURATION','BILLSEC','DISPOSITION','AMAFLAGS','ACCOUNTCODE','UNIQUEID','USERFIELD','PHONENO','CALLSTAGES',
            'ZONE','REGION_NO','STD_CODE','APP_IN_FLAG','COMPLAINTID','APPNO','UCOMPLAINTID','UBOOKINGDATE','RCOMPLAINTID',
            'RBOOKINGDATE','RCLOSEDATE','APPFLAG','TRANSID']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['UNIQUEID','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1 and CALLSTAGES like '%S-LA-MM-APP%'"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1 and CALLSTAGES like '%S-LA-MM-APP%'",) 
        
        super().__post_init__()

@dataclass
class CDR_1500(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'cdr'
    ToTable: str    =   'CDR_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =[
            'CALLDATE','CONTAINER_ID','CLID','SRC','DST','DCONTEXT','CHANNEL','DSTCHANNEL','LASTAPP','LASTDATA','DURATION',
            'BILLSEC','DISPOSITION','AMAFLAGS','ACCOUNTCODE','UNIQUEID','USERFIELD','PHONENO','CALLSTAGES','ZONE','REGION_NO',
            'STD_CODE','DOCKET_ID','COMP_FLAG','STD_CODE_BILL','PHONENO_BILL','BILL_FLAG','AGENT_REGION','AGENT_LANG',
            'AGENT_SERVICE','AGENT_PURPOSE','AGENT_CUSCAT','AGENT_CLID','AGENT_TELNO','AGENT_ORGTIME','CDRID_AGENT','REPTIME_AGENT',
            'ZONE_BILL','CHANGED_PHONE','TRANSID']
        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['UNIQUEID','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()


@dataclass
class CDR_1500_DOCKET(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_docket_api'
    ToTable: str    =   'DOCKET_API_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =[
            'API_DATE','ZONE','API_STATUS','INPUT','OUTPUT','ERROR',
            'STD_CODE','COMP_FLAG','DOCKET_ID','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()

@dataclass
class CDR_1500_BILL(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_bill_api'
    ToTable: str    =   'BILL_API_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =['API_DATE','ZONE','API_HIT','INPUT_NUMBER','FLAG','RESPONSE','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()

@dataclass
class CDR_1500_VIP(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_vip_api'
    ToTable: str    =   'VIP_API_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =['API_DATE','ZONE','API_HIT','INPUT_NUMBER','FLAG','RESPONSE','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()


@dataclass
class CDR_1500_CHD(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_chd_api'
    ToTable: str    =   'CHD_API_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =['API_DATE','ZONE','API_HIT','INPUT_NUMBER','FLAG','RESPONSE','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()


@dataclass
class CDR_1500_APP(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'ivrs_appeal_api'
    ToTable: str    =   'APP_API_1500'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn =[
            'API_DATE','ZONE','INPUT_NUMBER','RESPONSE','INPUT_FLAG',
            'INPUT_COMPID','RESFLAG','APPNO','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE']+self.FromColumn

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['API_DATE','TRANSID'] ))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()

@dataclass
class CDR_1500_RMN_API(BackupJobs):
    FromDb :dbmodel = localpg
    ToDb :dbmodel = oraclesdc    
    FromTable: str  =   'rmn_numbers'
    ToTable: str    =   'RMN_API'
    ClassOfJob:str = '1500'    
    def __post_init__(self):

        self.FromColumn = ['DATE','PHONENUMBER','CIRCLE','SSA','OTP','IS_SUCCESS','TRANSID']

        self.ToColumn   =   ['ENTRY_DATE','API_DATE']+self.FromColumn[1:]

        self.fromcol  =   ','.join(self.FromColumn)

        self.unique_keys = list(map(lambda x : str.lower(x),['DATE','TRANSID']))

        unique_key_str = ' and '.join([f"{key}=%({key})s" for key in self.unique_keys])

        self.pre_pull_querys = (f'update {self.FromTable} set deletflag=1 where deletflag=0',)

        self.pull_query=f"select now()::timestamp(6),{self.fromcol} from {self.FromTable} where deletflag=1"

        self.error_update_query = ' where '.join([f'update {self.FromTable} set deletflag=3 ',unique_key_str]) 
        
        self.final_post_push_querys = (f"delete from {self.FromTable} where deletflag=1",) 
        
        super().__post_init__()