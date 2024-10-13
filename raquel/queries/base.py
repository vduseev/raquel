class QueriesBase:
    create_jobs_table = NotImplemented
    create_jobs_index_queue = NotImplemented
    create_jobs_index_status = NotImplemented
    create_jobs_index_scheduled_at = NotImplemented
    drop_jobs_table = NotImplemented

    insert_job = NotImplemented
    select_job = NotImplemented
    select_jobs = NotImplemented
    select_jobs_count = NotImplemented
    select_jobs_count_status = NotImplemented
    select_oldest_job = NotImplemented
    select_queues = NotImplemented
    update_job_lock = NotImplemented
    update_job_done = NotImplemented
    update_job_retry = NotImplemented
