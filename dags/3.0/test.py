import random
import pandas as pd
import datetime 
from datetime import datetime, timedelta

# student_id = [random.randrange(1000000,2000000)]



# for i in range(2):
#     random_id = [random.randrange(1000000,2000000),random.randrange(1000000,2000000)]
#     print(random_id)

# for i in range(2):
#     type = ['payment','refund']
#     random_id = [random.randrange(1000000,2000000),random.randrange(1000000,2000000),random.choice(type)]
#     print(random_id)


# for i in range(2):
#     random_id = [random.randrange(1000000,2000000)]
#     print(random_id)

start_date = datetime(2019, 1, 1)
end_date = datetime(2025, 12, 31)

random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))

# 빈 리스트 생성
random_id1 = []

# 값 추가
for i in range(10):
    random_id1.append(random.randrange(1000000, 2000000))

# DataFrame 생성
data = pd.DataFrame(random_id1, columns=['id'])

# 결과 출력
print(data)


random_id2 = []

for i in range(1000):
    random_id2.append([random.randrange(1000000,2000000),random.randrange(300000,2000000), (start_date + timedelta(days=random.randint(0, (end_date - start_date).days)))])

data = pd.DataFrame(random_id2, columns=['id','amount','datetime'])

print(data)

random_id3 = []

for i in range(10):
    type = ['payment','refund']
    random_id3.append([random.randrange(1000000,2000000),random.randrange(1000000,2000000),random.choice(type)])
    
data = pd.DataFrame(random_id3, columns=['id','amount','type'])

print(data)


for i in range(1000):
    random_id1.append(random.randrange(1000000, 2000000))

# DataFrame 생성
data = pd.DataFrame(random_id1, columns=['id'])

print(data)

random_float = round(random.uniform(0, 12),3)
print(random_float)