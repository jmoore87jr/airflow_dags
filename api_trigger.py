import requests

dag_name = 'xcom'

url = f'http://localhost:8080/api/experimental/dags/{dag_name}/dag_runs'
headers = {'Cache-Control': 'no-cache', 
            'Content-Type': 'application/json'}

r = requests.get(url)
print(r.status_code)
print(r.text)

try:
    requests.post(url=url, headers=headers)
    print("DAG triggered")
except:
    print("DAG trigger FAILED")