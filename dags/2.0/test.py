def incremental_extract():
    from sqlalchemy import create_engine
    import pymysql
    from dotenv import load_dotenv
    import pandas as pd
    import warehouse_query2
    import os

    load_dotenv()

    pg_user = os.getenv('PG_USER')
    pg_password = os.getenv('PG_PASSWORD')
    pg_host = os.getenv('PG_HOST')
    pg_port = os.getenv('PG_PORT')
    pg_database = os.getenv('PG_DATABASE')

    mysql_user = os.getenv('MY_USER')
    mysql_password = os.getenv('MY_PASSWORD')
    mysql_host = os.getenv('MY_HOST')
    mysql_port = os.getenv('MY_PORT')
    mysql_database = os.getenv('MY_DATABASE')
    
    
    # PostgreSQL 연결
    pg_engine = create_engine(f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}")

    # MySQL 연결
    mysql_engine = create_engine(f"mysql+pymysql://{os.getenv('MY_USER')}:{os.getenv('MY_PASSWORD')}@{os.getenv('MY_HOST')}:{os.getenv('MY_PORT')}/{os.getenv('MY_DATABASE')}")

    before_data = 'select * from raw_data.lecture_video_tutoring'
    today_data = warehouse_query2.lvt_select_query

    df_before = pd.read_sql(before_data, pg_engine)
    print("before data")
    print(df_before)
    df_today = pd.read_sql(today_data, mysql_engine)
    print("today data")
    print(df_today)

    df_union_all = pd.concat([df_before, df_today], ignore_index=True)
    
    print("union data")
    print(df_union_all)
    
    #df_union_all['update_datetime'] = df_union_all['update_datetime'].to_timestamp()
    #df_union_all['update_datetime'] = pd.to_datetime(df_union_all['update_datetime'], errors='coerce')


    df_union_all['row_number'] = df_union_all.sort_values(by = ['update_datetime'], ascending = False).groupby(['lecture_vt_no']).cumcount()+1
    print("row number data")
    print(df_union_all)
    #df_union_all = df_union_all[df_union_all['lecture_vt_no'] == 19091]
    df_union_all = df_union_all[df_union_all['row_number'] == 1]
    
    print("final data")
    print(df_union_all)

    #df_union_all = df_union_all[df_union_all.columns.difference(['row_number'])]
    df_union_all = df_union_all.fillna(0).astype({'payment_item':'int64', 'next_payment_item':'int64', 'current_schedule_no':'int64'})
    #df_union_all = df_union_all.astype({'payment_item':'int64', 'next_payment_item':'int64', 'current_schedule_no':'int64'})
    df_union_all = df_union_all[['lecture_vt_no','student_user_no','lecture_subject_id','student_type','tutoring_state','payment_item','next_payment_item','current_schedule_no','stage_max_cycle_count','stage_free_cycle_count','stage_pre_offer_cycle_count','stage_offer_cycle_count','create_datetime','update_datetime','last_done_datetime','application_datetime','memo','total_subject_done_month','reactive_datetime']]

    print("final data2")
    print(df_union_all)
   
    return df_union_all

incremental_extract()