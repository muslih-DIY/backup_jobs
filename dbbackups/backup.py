from .BackupBase import BackupJobs

def execute():
    jobs = BackupJobs.get_acivte_jobs()
    if not jobs:
        print("No bakup Jobs")
        return 1
    for name,job in jobs.items():
        print('class name: ',name)
        x=job()
        x()
    return 1

def details():
    jobs = BackupJobs.get_acivte_jobs()
    count = 1
    for name,work in jobs.items():
        job:BackupJobs=work()
        print(f'({count = })## {job.job_name = } ##')
        print('='*60)
        print(f'{job.FromTable = } ')
        print(f'{job.ToTable = } ')
        print(f'{job.FromColumn = } ')
        print(f'{job.ToColumn = } ')
        print(f'{job.unique_keys = } ')
        print(f'{job.pre_pull_querys = } ')
        print(f'{job.pull_query = } ')
        print(f'{job.error_update_query = } ')
        print(f'{job.post_push_error_handle_failed_querys = } ')
        print(f'{job.post_push_error_handle_success_querys = } ')
        print(f'{job.final_post_push_querys = } ')
        print('='*60)
        count+=1

        