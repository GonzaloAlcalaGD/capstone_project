# Dependencies
import directories
import logging
import os 


def close_connection(session, context) -> bool:
    """ Function that closes the current session with Spark """
    try:
        logging.info('Closing spark session...')
        session.stop()
        context.stop()
        logging.info('Spark session succesfully closed')
        return True
    except Exception as e:
        logging.critical('Something went wrong at closing current spark session')
        logging.critical(e.__class__)
        logging.critical(e)
        return False


def load_and_normalize_json_data(json_path: str) -> None:
    """
    Function that returns a dataframe from a valid directory that contains jsonlines files.
    """
    # initialize PySpark Session
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    latest = directories.get_latest_folder(directories.get_subdirectories(json_path))
    path = '/opt/airflow/dags/storage/jsonlines_temp'

    if os.path.exists(os.path.dirname(os.path.join(json_path, latest))):
        df = spark.read.json(os.path.join(json_path, latest))
        df = df.withColumnRenamed('ts', 'timestamp')
        df = df.repartition(2)
        logging.info(f'Working with {df.rdd.getNumPartitions()} partitions')
        df = df.select('id', 'timestamp', 'customer_first_name', 'customer_last_name', 'amount', 'type')
        df.show(10)
        df.coalesce(1).write.mode('overwrite').parquet(path)
        logging.info(f'Normalized dataframe saved into .parquet format into {path}')
        status = close_connection(session= spark, context=sc)
        connection = 'Closed' if status == True else 'Open'
        logging.info(f'Spark session and context status: {connection}')
        return logging.info('JSONLines Dataframe succesfully loaded and normalized headers.')
    else:
        return logging.critical('Path to jsonl files doesn\'t exist')


def load_and_normalize_parquet_data(parquet_path: str) -> None:
    """
    Function that returns a dataframe from a valid directory that contains parquet files.
    """
    # initialize PySpark Session
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    latest = directories.get_latest_folder(directories.get_subdirectories(parquet_path))
    path = '/opt/airflow/dags/storage/parquet_temp'

    if os.path.exists(os.path.dirname(parquet_path)):
        df = spark.read.option('header', 'true').parquet(os.path.join(parquet_path, latest))
        df = df.withColumnRenamed('First_name', 'customer_first_name')
        df = df.withColumnRenamed('Last_name', 'customer_last_name')
        df = df.withColumnRenamed('Amount', 'amount')
        df = df.withColumnRenamed('Store_id', 'store_id')
        df = df.withColumnRenamed('Id', 'id')
        df = df.repartition(2)
        logging.info(f'Working with {df.rdd.getNumPartitions()} partitions')
        df.show(10)
        df.coalesce(1).write.mode('overwrite').parquet(path)
        logging.info(f'Normalized dataframe saved into .parquet format into {path}')
        status = close_connection(session= spark, context=sc)
        connection = 'Closed' if status == True else 'Open'
        logging.info(f'Spark session and context status: {connection}')
        return logging.info('Parquet Dataframe succesfully loaded and normalized headers.')
    else:
        return logging.critical('Path to parquet file deosn\'t exist')


def load_and_normalize_rdbms_data() -> None:
    # initialize PySpark Session
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf

    spark = SparkSession.builder.config('spark.jars', '/opt/airflow/dags/postgresql-42.5.0.jar').getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)

    path = '/opt/airflow/dags/storage/rdbms_temp'
    customer_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://capstone-db:5432/capstone') \
        .option('dbtable', 'customer').option('user', 'root') \
        .option('password', 'root') \
        .option('driver', 'org.postgresql.Driver') \
        .load()
    customer_df = customer_df.withColumnRenamed('first_name', 'customer_first_name')
    customer_df = customer_df.withColumnRenamed('last_name', 'customer_last_name')
    transaction_df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://capstone-db:5432/capstone') \
        .option('dbtable', 'transaction') \
        .option('user', 'root') \
        .option('password', 'root') \
        .option('driver', 'org.postgresql.Driver') \
        .load()
    transaction_df = transaction_df.withColumnRenamed('Customer_id', 'customer_id')
    transaction_df = transaction_df.withColumnRenamed('Transaction_ts', 'timestamp')
    transaction_df = transaction_df.withColumnRenamed('Amount', 'amount')
    rdbms_df = customer_df.join(transaction_df, ['id'], 'inner')
    rdbms_df = rdbms_df.select('id', 'customer_id', 'amount', 'customer_first_name', 'customer_last_name', 'phone_number', 'address', 'timestamp')
    rdbms_df.show(10)
    rdbms_df = rdbms_df.repartition(2)
    logging.info(f'Working with {rdbms_df.rdd.getNumPartitions()} partitions')
    rdbms_df.coalesce(1).write.mode('overwrite').parquet(path)
    status = close_connection(session= spark, context=sc)
    connection = 'Closed' if status == True else 'Open'
    logging.info(f'Spark session and context status: {connection}')


def load_and_unify(jsonl: str, parquet: str, rdbms: str):
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql.types import LongType

    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    path = '/opt/airflow/dags/storage/unified_df'

    # Load parquet files from arguments
    jsonl_df = spark.read.option('header', 'true').parquet(jsonl)
    parquet_df = spark.read.option('header', 'true').parquet(parquet)
    rdbms_df = spark.read.option('header', 'true').parquet(rdbms)
    # Repartition dataframes
    jsonl_df = jsonl_df.repartition(2)
    parquet_df = parquet_df.repartition(2)
    rdbms_df = rdbms_df.repartition(2)
    # # Unify jsonlines and parquet dataframes
    unified_df = jsonl_df.join(parquet_df, ['id', 'customer_first_name', 'customer_last_name', 'amount', 'timestamp'], "fullouter")
    unified_df = unified_df.select('id', 'type', 'store_id', 'amount', 'customer_first_name', 'customer_last_name', 'timestamp')
    # # Unify unified_df(jsonlines+parquet) with rdbms dataframe
    rdbms_df = rdbms_df.withColumn('id', rdbms_df['id'].cast(LongType()))
    unified_df = unified_df.join(rdbms_df, ['id', 'amount', 'customer_first_name', 'customer_last_name', 'timestamp'], 'fullouter')
    unified_df = unified_df.select('id', 'customer_id', 'store_id', 'type', 'amount', 'customer_first_name', 'customer_last_name', 'phone_number', 'address', 'timestamp')
    unified_df = unified_df.repartition(2)
    unified_df.show(10)
    unified_df.coalesce(1).write.mode('overwrite').parquet(path)
    status = close_connection(session= spark, context=sc)
    connection = 'Closed' if status == True else 'Open'
    return logging.info(f'Spark session and context status: {connection}')


def transform_data(path: str):
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import functions as f
    from pyspark.sql.types import FloatType, StringType
    from my_udfs import UDFCatalog

    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    sc.addFile('/opt/airflow/dags/my_udfs.py')
    save_path = '/opt/airflow/dags/storage/transformed_unified'

    catalog = UDFCatalog()
    catalog_sc = sc.broadcast(catalog)

    def amount(x):
        return catalog_sc.value.transform_amount(x)

    def timestamp(x):
        return catalog_sc.value.transform_timestamp(x)
    
    amountUDF = f.udf(amount, FloatType())
    timestampUDF = f.udf(timestamp, StringType())

    temporal_df = spark.read.option('header', 'true').parquet(path)
    dump_df = temporal_df.withColumn('float_amount', amountUDF(temporal_df.amount))
    dump_df = dump_df.withColumn('t_timestamp', timestampUDF(dump_df.timestamp))
    unified_model = dump_df.select('id', 'customer_id', 'store_id', 'type', 'float_amount', 'customer_first_name', 'customer_last_name', 'phone_number', 'address', 't_timestamp')    
    unified_model = unified_model.withColumnRenamed('float_amount', 'amount')
    unified_model = unified_model.withColumnRenamed('t_timestamp', 'timestamp')
    unified_model.show(100)
    unified_model.write.mode('append').parquet(save_path)
    status = close_connection(session= spark, context=sc)
    connection = 'Closed' if status == True else 'Open'
    return logging.info(f'Spark session and context status: {connection}')

def load_and_enrich_data(path: str):
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import functions as f
    from pyspark.sql.types import FloatType, StringType, IntegerType, LongType
    from my_udfs import UDFCatalog
    import logging

    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    sc.addFile('/opt/airflow/dags/my_udfs.py')
    save_path = 'hdfs://namenode:9000/transformed_unified_and_enriched_dataframe'

    catalog = UDFCatalog()
    catalog_sc = sc.broadcast(catalog)

    def storeID(x):
        return catalog_sc.value.transform_store_id(x)


    def phoneNumber(x):
        return catalog_sc.value.transform_phone_number(x)


    def address(x):
        return catalog_sc.value.transform_address(x)


    def customerID(x):
        return catalog_sc.value.transform_customer_id(x)


    storeId_udf = f.udf(storeID, IntegerType())
    phoneNumber_udf = f.udf(phoneNumber, StringType())
    address_udf = f.udf(address, StringType())
    customerID_udf = f.udf(customerID, LongType())
    
    temporal_df = spark.read.option('header', 'true').parquet(path)
    # Type enrichment
    dump_df = temporal_df.withColumn('t_type', f.when(temporal_df.type.isNull(), f.rand()).otherwise(temporal_df.type))
    dump_df = dump_df.withColumn('tt_type', f.when(dump_df.t_type >= 0.5, 1).otherwise(0))
    # Store id enrichment
    dump_df = dump_df.withColumn('t_store_id', storeId_udf(temporal_df.store_id))
    # Phone number enrichment
    dump_df = dump_df.withColumn('t_phone_number', phoneNumber_udf(temporal_df.phone_number))
    # Address enrichment
    dump_df = dump_df.withColumn('t_address', address_udf(temporal_df.address))
    # Customer ID enrichment
    dump_df = dump_df.withColumn('t_customer_id', customerID_udf(temporal_df.customer_id))

    unified_model = dump_df.select('id', 't_customer_id', 't_store_id', 'tt_type', 'amount', 'customer_first_name', 'customer_last_name', 't_phone_number', 't_address', 'timestamp')    
    unified_model = unified_model.withColumnRenamed('tt_type', 'type')
    unified_model = unified_model.withColumnRenamed('t_store_id', 'store_id')
    unified_model = unified_model.withColumnRenamed('t_phone_number', 'phone_number')
    unified_model = unified_model.withColumnRenamed('t_address', 'address')
    unified_model = unified_model.withColumnRenamed('t_customer_id', 'customer_id')
    unified_model.show(100)
    unified_model.write.mode('append').parquet(save_path)
    status = close_connection(session= spark, context=sc)
    connection = 'Closed' if status == True else 'Open'
    return logging.info(f'Spark session and context status: {connection}')


def load_and_aggregate(path: str):
    from pyspark.sql import SparkSession
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import functions as f

    
    spark = SparkSession.builder.getOrCreate()
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)

    save_path = 'hdfs://namenode:9000/aggregations'

    temporal_df = spark.read.option('header', 'true').parquet(path)
    dump_1 = temporal_df.select('type', 'timestamp', 'amount')
    # Count and sum amount transactions for each type for day - online & offline
    online = dump_1.filter(dump_1.type == 1).groupBy(dump_1.type, dump_1.timestamp).agg(f.count('*').alias('total_transactions'), f.sum('amount').alias('total'))
    offline = dump_1.filter(dump_1.type == 0).groupBy(dump_1.type, dump_1.timestamp).agg(f.count('*').alias('total_transactions'), f.sum('amount').alias('total'))
    online_and_offline = online.unionByName(offline, allowMissingColumns=True)
    logging.info('Working in  - Count and sum amount transactions for each type for day, online & offline - ')
    online_and_offline.show(50)
    # Count and sum transaction for each store for day
    store_metrics = temporal_df.groupBy(temporal_df.store_id, temporal_df.timestamp).agg(f.count(temporal_df.timestamp).alias('count'), f.sum(temporal_df.amount).alias('total')).sort('store_id')
    logging.info('Working in - Count and sum transaction for each store for day -')
    store_metrics.show(50)
    # Count and sum amount transactions for each city for day
    cities_transactions = temporal_df.groupBy(temporal_df.address, temporal_df.timestamp).agg(f.count('*').alias('count'), f.sum(temporal_df.amount).alias('total')).sort('timestamp')
    logging.info('Working in - Count and sum amount transactions for each city for day -')
    cities_transactions.show(50)
    
    online_and_offline.write.mode('append').parquet(f'{save_path}/online_offline')
    store_metrics.write.mode('append').parquet(f'{save_path}/store_metrics')
    cities_transactions.write.mode('append').parquet(f'{save_path}/cities_transactions')

    status = close_connection(session= spark, context=sc)
    connection = 'Closed' if status == True else 'Open'
    return logging.info(f'Spark session and context status: {connection}')