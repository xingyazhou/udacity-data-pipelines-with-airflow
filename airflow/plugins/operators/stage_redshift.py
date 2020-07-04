from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    '''Operator loads any JSON formatted files from S3 to Amazon Redshift.
   
    redshift_conn_id (string):  Airflow conn_id of Redshift connection
    aws_credentials_id (string): Airflow conn_id of the aws_credentials granting access to s3 (Default: 'aws_credentials')  
    table (string):  The name of the Amazon Redshift table where the data should be loaded
    s3_path (string): The data source where to get data from          
    
    '''

    copy_sql_date = """
        COPY {} 
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """

    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    """
     
    
    @apply_defaults               
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 table = "",
                 s3_path = "",
                 json_path = "",
                 *args, **kwargs):             

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table           
        self.s3_path = s3_path
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')
               

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")      
        # Backfill a specific date
        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_time.format(
                self.table, 
                self.s3_path, 
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key, 
                self.json_path,
                self.execution_date
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table, 
                self.s3_path, 
                credentials.access_key,
                credentials.secret_key, 
                self.json_path,
                self.execution_date
            )
            
        redshift.run(formatted_sql)