import pandas as pd # type: ignore
import test_query
from sqlalchemy.engine import create_engine


  

# SQLAlchemy Engine 생성
trino_engine = create_engine(
    "trino://admin@eks-trino.seoltab.com:8080/"
)

# raw_data 불러오기
lvs = pd.read_sql(test_query.lvs_query, trino_engine)
t = pd.read_sql(test_query.t_query, trino_engine)
p = pd.read_sql(test_query.p_query, trino_engine)
mlvt = pd.read_sql(test_query.mlvt_query, trino_engine)
lvc = pd.read_sql(test_query.lvc_query, trino_engine)

# lvt 별 최초 결제
p['rn'] = p.sort_values(by = ['payment_regdate'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
p_1 = p[p['rn'] == 1]
p_result = p_1[p_1['payment_regdate'] >= '2024-11-01 00:00:00']

# lvt 별 최초 매칭
mlvt['rn'] = mlvt.sort_values(by = ['matchedat'], ascending = True).groupby(['lecture_vt_No']).cumcount()+1
mlvt_result = mlvt[mlvt['rn'] == 1]


# schedule & cycle  Inner Join
schedule_list = pd.merge(lvs, lvc, on='lecture_cycle_No', how='inner')

# 최근 결과
df = mlvt_result.merge(schedule_list, on=["lecture_vt_No", "teacher_user_No"], how="inner") \
         .merge(p_result, on="lecture_vt_No", how="inner") \
         .merge(t, left_on="teacher_user_No", right_on='user_No', how="inner") \

meta_data = pd.read_sql(test_query.meta_data_query, trino_engine)

df_meta = df.merge(meta_data, on='lecture_vt_No', how='left') \
    .sort_values(by=["lecture_vt_No"])

print(df_meta)
# 









