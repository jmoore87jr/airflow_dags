import os
from PRIVATE import username, password

### WORKS!!!
# Make sure backend is set to basic_auth in config: 
# auth_backend = airflow.api.auth.backend.basic_auth
command = """
curl -X POST --user {username}:{password} \
-H "Accept: application/json" \
-H "Content-Type: application/json" \
--data '{"dag_run_id": "string", "execution_date": "2021-08-06T21:47:01.641764+00:00", "conf": { }}' \
http://localhost:8080/api/v1/dags/xcom/dagRuns
""".format(username, password)

os.system(command)
