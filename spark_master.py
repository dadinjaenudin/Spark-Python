
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql import SQLContext

sparkMaster = "spark://129.154.85.159:7078"
hiveMetastore = "thrift://localhost:9083"

def create_spark_session(app_name="Article_Application"):
    spark_session = SparkSession\
    .builder\
    .master(sparkMaster)\
    .appName(app_name)\
    .enableHiveSupport()\
    .getOrCreate()

    spark_session.conf.set("spark.sql.shuffle.partitions", 6)
    spark_session.conf.set("spark.driver.memory", "1g")
    spark_session.conf.set("spark.executor.memory", "1g")
    spark_session.conf.set("hive.metastore.uris", hiveMetastore)

    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session

def main():
    # ss = SparkSession
    SS      = create_spark_session()
    sqlCtx  = SQLContext(SS)


    SS.sql("USE bigdata_prd")

    #===========================================#
    # OverWriter table dim_subclass
    # ===========================================#
    sql = """ SELECT  DISTINCT
                      *
                FROM fs_store
          """
    print sql

    df_select = SS.sql(sql)
    df_select.show(20)


if __name__ == '__main__':
    main()

# run spark on Yarn
# spark-submit --master yarn-cluster connect_hive.py
# spark-submit --master yarn-client connect_hive.py

