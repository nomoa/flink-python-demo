from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
t_config = TableConfig()
t_env = StreamTableEnvironment.create(env, t_config)


# configure env programmatically
t_env.add_python_archive("venv.zip")
t_env.get_config().set_python_executable("venv.zip/venv/bin/python")
# connector_jar_home = "file:///home/dcausse/flink-1.12.0/connectors"
# kafka_connector = connector_jar_home + "/flink-connector-kafka_2.11-1.12.0.jar"
# kafka_client = connector_jar_home + "/kafka-clients-2.4.1.jar"
# t_env.get_config().get_configuration().set_string("pipeline.jars", kafka_connector + ";" + kafka_client)

def eval(params):
        if params is not None and 'action' in params:
            action = params['action']
            if action == 'opensearch':
                return 'search'
            elif action == 'query' and 'list' in params and ('search' in params['list']):
                return 'search'
            elif action == 'query' and 'generator' in params and ('search' in params['generator']):
                return 'search'
            return action
        return 'unknown'


classify = udf(eval, [DataTypes.MAP(DataTypes.STRING(True), DataTypes.STRING(True))], DataTypes.STRING(True))

t_env.create_temporary_function("classify_action_api", classify)

t_env.execute_sql("""
CREATE table api_action (
    meta ROW<uri STRING, request_id STRING, dt STRING, id STRING, domain STRING, `stream` STRING>,
    http ROW<`method` STRING, client_ip STRING, request_headers MAP<STRING, STRING>, has_cookies BOOLEAN>,
    database STRING,
    backend_time_ms INTEGER,
    api_error_codes ARRAY<STRING>,
    params MAP<STRING, STRING>,
    kafka_dt TIMESTAMP(3) METADATA FROM 'timestamp', -- reads and writes a Kafka record's timestamp
    WATERMARK FOR kafka_dt AS kafka_dt - INTERVAL '10' SECOND
) WITH (
 'connector' = 'kafka',
 'topic' = 'eqiad.mediawiki.api-request',
 'properties.bootstrap.servers' = 'kafka-jumbo1001.eqiad.wmnet:9092,kafka-jumbo1002.eqiad.wmnet:9092,kafka-jumbo1003.eqiad.wmnet:9092',
 'properties.group.id' = 'flink_demo_T262942',
 'format' = 'json',
 'json.timestamp-format.standard' = 'ISO-8601',
 'scan.startup.mode' = 'latest-offset'
)
""")

sql = """
SELECT
    TUMBLE_START(kafka_dt, INTERVAL '1' MINUTE) as w,
    database,
    classify_action_api(params),
    count(1),
    avg(backend_time_ms)
FROM api_action
GROUP BY
    TUMBLE(kafka_dt, INTERVAL '1' MINUTE),
    database,
    classify_action_api(params)
"""

res = t_env.sql_query(sql).execute()
with res.collect() as results:
    for r in results:
        print(r)
