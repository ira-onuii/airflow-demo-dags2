import random

student_id = [random.randrange(1000000,2000000)]



for i in range(2):
    random_id = [random.randrange(1000000,2000000),random.randrange(1000000,2000000)]
    print(random_id)

for i in range(2):
    type = ['payment','refund']
    random_id = [random.randrange(1000000,2000000),random.randrange(1000000,2000000),random.choice(type)]
    print(random_id)


for i in range(2):
    random_id = [random.randrange(1000000,2000000)]
    print(random_id)