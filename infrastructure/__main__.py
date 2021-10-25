import pulumi
import pulumi_aws as aws
import pulumi_snowflake as snowflake

from sf_roles import MySnowflakeRoles
from sf_snowpipe import MySnowpipe


PROJECT_NAME = pulumi.get_project()
STACK_NAME = pulumi.get_stack()
PREFIX = f'{PROJECT_NAME}-{STACK_NAME}'.lower().replace('_', '-')


# Constants
SF_DB_NAME = f'{PROJECT_NAME}_{STACK_NAME}'.upper()
SF_STREAM_SCHEMA_NAME = 'RAW'.upper()
SF_STREAM_LOGS_TABLE_NAME = 'LOGS'.upper()

SF_LOGS_STORAGE_INTEGRATION_NAME = f'{SF_DB_NAME}_LOGS_S3'.upper()
SF_LOGS_STAGE_NAME = f'{SF_DB_NAME}_LOGS_STAGE'.upper()
SF_LOGS_PIPE_NAME = f'{SF_DB_NAME}_LOGS_PIPE'.upper()


# S3 Bucket
s3_logs_bucket = aws.s3.Bucket(
    'logs',
    bucket=PREFIX,
    acl='private',
)

# Snowflake
sf_warehouse = snowflake.Warehouse(
    f'Warehouse',
    name=f'{SF_DB_NAME}_WH',
    warehouse_size='x-small',
    auto_suspend=5,
    auto_resume=True,
)

sf_database = snowflake.Database(
    'Database',
    name=SF_DB_NAME,
)

sf_roles = MySnowflakeRoles(
    SF_DB_NAME,
    database=sf_database,
    warehouse=sf_warehouse,
)

sf_stream_schema = snowflake.Schema(
    'Stream',
    name=SF_STREAM_SCHEMA_NAME,
    database=sf_database,
    opts=pulumi.ResourceOptions(
        parent=sf_database,
        depends_on=[
            sf_roles.read_only,
            sf_roles.read_write,
        ],
    ),
)

sf_stream_logs_snowpipe = MySnowpipe(
    'Logs',
    prefix=PREFIX,
    s3_bucket=s3_logs_bucket,
    s3_logs_prefix='stream_data',
    s3_error_prefix='stream_errors',
    database=sf_database,
    schema=sf_stream_schema,
    storage_integration_name=SF_LOGS_STORAGE_INTEGRATION_NAME,
    stage_name=SF_LOGS_STAGE_NAME,
    pipe_name=SF_LOGS_PIPE_NAME,
    table_name=SF_STREAM_LOGS_TABLE_NAME,
    table_columns=[
        snowflake.TableColumnArgs(
            name='ID',
            type='VARCHAR(36)',
            nullable=False,
        ),
        snowflake.TableColumnArgs(
            name='DATA',
            type='VARIANT',
            nullable=False,
        ),
        snowflake.TableColumnArgs(
            name='LOG_FILENAME',
            type='VARCHAR(16777216)',
            nullable=False,
        ),
        snowflake.TableColumnArgs(
            name='LOG_FILE_ROW_NUMBER',
            type='NUMBER(20,0)',
            nullable=False,
        ),
        snowflake.TableColumnArgs(
            name='LOG_TIMESTAMP',
            type='TIMESTAMP_TZ(9)',
            nullable=False,
        ),
    ],
    table_cluster_bies=[
        'TO_DATE(LOG_TIMESTAMP)',  # TODO: use the log timestamp
    ],
    copy_statement=f"""
        COPY INTO {SF_DB_NAME}.{SF_STREAM_SCHEMA_NAME}.{SF_STREAM_LOGS_TABLE_NAME} (
            ID,
            DATA,
            LOG_FILENAME,
            LOG_FILE_ROW_NUMBER,
            LOG_TIMESTAMP
        )
        FROM (
            SELECT
                LOWER(UUID_STRING('da69e958-fee3-428b-9dc3-e7586429fcfc', CONCAT(metadata$filename, ':', metadata$file_row_number))),
                $1,
                metadata$filename,
                metadata$file_row_number,
                CURRENT_TIMESTAMP()
            FROM @{SF_DB_NAME}.{SF_STREAM_SCHEMA_NAME}.{SF_LOGS_STAGE_NAME}
        )
    """,
    opts=pulumi.ResourceOptions(
        parent=sf_stream_schema,
        depends_on=[
            sf_roles.read_only,
            sf_roles.read_write,
        ],
    ),
)


# Final
pulumi.export('firehose_arn', sf_stream_logs_snowpipe.firehose.arn)
pulumi.export('firehose_name', sf_stream_logs_snowpipe.firehose.name)
pulumi.export('snowflake_database_name', sf_database.name)
pulumi.export('snowflake_stream_schema_name', sf_stream_schema.name)
pulumi.export('snowflake_stream_table_name',
              sf_stream_logs_snowpipe.table.name)
