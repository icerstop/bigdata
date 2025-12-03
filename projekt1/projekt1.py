from datetime import datetime
from airflow import DAG
from airflow.sdk import Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

# gcloud dataproc clusters create ${CLUSTER_NAME} --enable-component-gateway --region ${REGION} --subnet default --public-ip-address --master-machine-type n2-standard-4 --master-boot-disk-size 50 --num-workers 2 --worker-machine-type n2-standard-2 --worker-boot-disk-size 50 --image-version 2.2-debian12 --project ${PROJECT_ID} --max-age=3h

with DAG(
    dag_id="projekt1-workflow",
    start_date=datetime(2015, 12, 1),
    schedule=None,
    params={
        "dags_home": Param(
            "/home/[nazwa_uzytkownika]/airflow/dags", type="string"
        ),
        "input_dir": Param(
            "gs://[nazwa zasobnika]/projekt1/input", type="string"
        ),
        "output_mr_dir": Param("/projekt1/output_mr3", type="string"),
        "output_dir": Param("/projekt1/output6", type="string"),
        "classic_or_streaming": Param(
            "classic", enum=["classic", "streaming"]
        ),
    },
    render_template_as_native_obj=True,
    catchup=False,

) as dag:
    clean_output_mr_dir = BashOperator(
        task_id="clean_output_mr_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_mr_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_mr_dir }}; fi"
        ),
    )

    clean_output_dir = BashOperator(
        task_id="clean_output_dir",
        bash_command=(
            "if hadoop fs -test -d {{ params.output_dir }}; "
            "then hadoop fs -rm -f -r {{ params.output_dir }}; fi"
        ),
    )

    def _pick_classic_or_streaming(params):
        if params["classic_or_streaming"] == "classic":
            return "mapreduce_classic"
        else:
            return "hadoop_streaming"

    pick_classic_or_streaming = BranchPythonOperator(
        task_id="pick_classic_or_streaming",
        python_callable=_pick_classic_or_streaming,
        op_kwargs={"params": dag.params},
    )

    mapreduce_classic = BashOperator(
        task_id="mapreduce_classic",
        bash_command=(
            """hadoop jar {{ params.dags_home }}/project_files/main.jar com.example.bigdata.Main {{ params.input_dir }}/datasource1 {{ params.output_mr_dir }}"""
        ),
    )

    hadoop_streaming = BashOperator(
        task_id="hadoop_streaming",
        bash_command=(
            "mapred streaming "
            "-files {{ params.dags_home }}/project_files/ . . ."
        ),
    )

    hive = BashOperator(
        task_id="hive",
        bash_command=(
            """beeline -u jdbc:hive2://localhost:10000/default \
      -f {{ params.dags_home }}/project_files/hive.hql \
      --hivevar input_dir4={{ params.input_dir }}/datasource4 \
      --hivevar input_dir3={{ params.output_mr_dir }} \
      --hivevar output_dir6={{ params.output_dir }}"""
        ),
        trigger_rule="none_failed",
    )

    get_output = BashOperator(
        task_id="get_output",
        bash_command=(
            "hadoop fs -getmerge {{ params.output_dir }} output6.json && head output6.json"
        ),
        trigger_rule="none_failed",
    )

    [clean_output_mr_dir, clean_output_dir] >> pick_classic_or_streaming
    pick_classic_or_streaming >> [mapreduce_classic, hadoop_streaming]
    [mapreduce_classic, hadoop_streaming] >> hive
    hive >> get_output
