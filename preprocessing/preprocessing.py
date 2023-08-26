import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = 'fastcampus-test-bucket-1/input'
output_path = 'fastcampus-test-bucket-1/output'

# spark에서 파일을 읽을 때는 read 함수를 이용한다.
# greed_df.show(truncate=False)  # truncate파라미터: 컬럼이나 값들이 길어졌을때 줄여지는 부분을 제거
### 각각의 필드들이 어떠한 값으로 저장되어 있는지 알려주는 함수
# yellow_df.printSchema()스키마

new_cols_expr = ['VendorID as vendor_ID', 'RatecodeID as pu_location_id','DOLocationID as do_location_id', 'payment_type',
                 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']


yellow_cols = ['tpep_pickup_datetime as pickup_datetime', 'tpep_dropoff_datetime as dropoff_datetime']
yellow_cols.extend(new_cols_expr) # 공통컬럼 확장
green_cols = ['lpep_pickup_datetime as pickup_datetime', 'lpep_dropoff_datetime as dropoff_datetime']
green_cols.extend(new_cols_expr) # 공통컬럼 확장

# 컬럼 선택 및 컬럼명 변경
# withColumn를 이용해서 taxi_type 컬럼 추가
for ym in ['2022-09', '2022-10']:
    yellow_df = spark.read.parquet(f's3a://{input_path}/yellow_tripdata_{ym}.parquet')\
        .selectExpr(yellow_cols).withColumn('taxi_type', F.lit('yellow'))
    green_df = spark.read.parquet(f's3a://{input_path}/green_tripdata_{ym}.parquet')\
        .selectExpr(green_cols).withColumn('taxi_type', F.lit('green'))

    # 두 데이터 병합
    union_df = yellow_df.union(green_df)
    union_df.show(truncate = False)
    # overwrite 옵션을 활용해 파일 덮어쓰기(즉, 기존의 파일 3개는 삭제)
    # repartition 함수를 사용해 파일 생성 개수 제한
    union_df.repartition(1)\
        .write.option('header', 'true')\
        .mode('overwrite')\
        .csv(f's3a://{output_path}/ym={ym}/') # 기간 값에 따라서 파티셔닝

job.commit()