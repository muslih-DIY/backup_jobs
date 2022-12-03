from .settings import ClassOfJobs
from .dbbackups import BackupJobs

def execute():
    for jobclass in ClassOfJobs:
        jobs = BackupJobs.get_acivte_jobs(jobclass)

    if not jobs:
        print("No bakup Jobs")
        return 1
    for name,job in jobs.items():
        print('class name: ',name)
        x=job()
        x()
    return 1

def details():
    
    for jobclass in ClassOfJobs:
        jobs = BackupJobs.get_acivte_jobs(jobclass)
        count = 1
        if not jobs:
            print("No bakup Jobs")
            return 1
        for name,work in jobs.items():
            job:BackupJobs=work()
            print(f'(count :{count})## job_name = {job.job_name} ##')
            print('='*60)
            print(f'FromTable  = {job.FromTable} ')
            print(f'ToTable  = {job.ToTable} ')
            print(f'FromColumn  = {job.FromColumn} ')
            print(f'ToColumn  = {job.ToColumn} ')
            print(f'unique_keys  = {job.unique_keys} ')
            print(f'pre_pull_querys  = {job.pre_pull_querys} ')
            print(f'pull_query  = {job.pull_query} ')
            print(f'error_update_query  = {job.error_update_query} ')
            print(f'post_push_error_handle_failed_querys  = {job.post_push_error_handle_failed_querys} ')
            print(f'post_push_error_handle_success_querys  = {job.post_push_error_handle_success_querys} ')
            print(f'final_post_push_querys  =  {job.final_post_push_querys} ')
            print('='*60)
            count+=1

        