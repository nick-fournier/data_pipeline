import os
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv('data_pipeline/.env')

class PostgresResource:
    pg_host: str  = os.getenv('POSTGRES_HOST', "")
    pg_port: int = int(os.getenv('POSTGRES_PORT', 22))
    pg_database: str = os.getenv('POSTGRES_DB', "")
    
    def connect(self):
        with SSHTunnelForwarder(
            (os.getenv('SSH_HOST')),
            ssh_username=os.getenv('SSH_USER'),
            ssh_password=os.getenv('SSH_PASS'),
            remote_bind_address=(self.pg_host, self.pg_port)
        ) as tunnel:
            conn_uri = "postgres://{}:{}@{}:{}/{}".format(
                os.getenv('POSTGRES_USER'),
                os.getenv('POSTGRES_PASS'),
                getattr(tunnel, 'local_bind_host'),
                getattr(tunnel, 'local_bind_port'),
                self.pg_database
            )
            return conn_uri
        
if __name__ == '__main__':
    resource = PostgresResource()
    print(resource.connect())