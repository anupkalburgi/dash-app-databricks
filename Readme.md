
## Intro
This project showcases an interactive web application powered by Databricks as the backend. It demonstrates how any frontend technology can seamlessly integrate with Databricks, using SQL to process large-scale datasets efficiently.


## Libraries
[Databricks SQL Connector for Python](https://github.com/databricks/databricks-sql-python) - The Databricks SQL Connector for Python allows you to develop Python applications that connect to Databricks clusters and SQL warehouses. It is a Thrift-based client with no dependencies on ODBC or JDBC


## Run the project
```sh
./run.sh
```


## Deploying to Azure Container Apps
```sh
az containerapp up -g db-ak-dash-2 -n db-ak-dash-2 --ingress external --target-port 8080 --source .
```