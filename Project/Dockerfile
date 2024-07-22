FROM apache/airflow:2.9.2

RUN pip install markupsafe==2.0.1 \
	&& pip install apache-airflow-providers-odbc \
	&& pip install pyodbc \
	&& pip install kaggle \
	&& pip install apache-airflow-providers-microsoft-mssql \
	&& pip install apache-airflow-providers-microsoft-mssql[odbc] \
	&& pip install apache-airflow-providers-microsoft-azure \
	&& pip install sqlalchemy \
	&& pip install gitpython