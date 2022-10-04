import logging

def set_run_config(ds, **kwargs):
    #Checks to see if runtime config was passed via manual trigger
    manual_trigger_config = kwargs["dag_run"].conf 
    
    try:
        run_start = manual_trigger_config['backfill_start']
        run_end = manual_trigger_config['backfill_end']
        backfill_status = True
        logging.info(f'The backfill status is {backfill_status}')
    
    except KeyError:
        backfill_status = False
        run_start = ds
        run_end = ds
        logging.info(f'The backfill status is {backfill_status}')


    logging.info(f'The run_start and run_end are {run_start} and {run_end}')

    kwargs['ti'].xcom_push(key='run_start',value=run_start)
    kwargs['ti'].xcom_push(key='run_end',value=run_end)
    kwargs["ti"].xcom_push(key='backfill_status', value=backfill_status)