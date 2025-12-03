export AIRFLOW_HOME=~/airflow
export AIRFLOW_VERSION=3.1.0
export AIRFLOW__CORE__LOAD_EXAMPLES=False
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
export PATH=$PATH:~/.local/bin
curl -LsSf https://astral.sh/uv/install.sh | sh
uv venv
uv pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
source ~/.venv/bin/activate
airflow db migrate
sed -i 's/^refresh_interval\s*=.*/refresh_interval = 15/' ./airflow/airflow.cfg
airflow standalone
