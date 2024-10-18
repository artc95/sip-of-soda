from airflow import DAG
from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "sip_of_soda",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = "<this_project_full_path_root>"


def run_soda_scan(project_root, scan_name, checks_subpath = None):
    from soda.scan import Scan

    print("Running Soda Scan ...")
    config_file = f"{project_root}/configuration.yml"
    checks_path = f"{project_root}/soda"

    if checks_subpath:
        checks_path += f"/{checks_subpath}"

    scan = Scan()
    scan.set_verbose()
    scan.add_configuration_yaml_file(config_file)
    scan.set_data_source_name("soda_demo")
    scan.add_sodacl_yaml_files(checks_path)
    scan.set_scan_definition_name(scan_name)

    result = scan.execute()
    print(scan.get_logs_text())
    # Extract Scan results from a Python dictionary
    # r_dict = scan.get_scan_results()

    if result != 0:
        raise ValueError('Soda Scan failed')
    # Additional Scan methods as circuit breakers
    # scan.assert_no_checks_fail()
    # scan.get_checks_fail

    return result


with DAG(
    "soda_demo_data_pipeline",
    default_args=default_args,
    description="A simple data pipeline example including Soda Scans for Data Testing",    
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
):
    ingest_raw_data = EmptyOperator(task_id="ingest_raw_data")

    soda_enforce_ingestion_contract = PythonOperator(
        task_id="soda_enforce_ingestion_contract",
        python_callable=run_soda_scan,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "model_adventureworks_sales_category_ingest",
            "checks_subpath": "ingest-checks/dim_product_category.yml"
        },
    )

    soda_process_failing_records_ingest = EmptyOperator(task_id="soda_process_failing_records_ingest")

    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command= # dbt run command,
    )

    soda_enforce_transformation_contract = PythonOperator(
        task_id="soda_enforce_transformation_contract",
        python_callable=run_soda_scan,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "model_adventureworks_sales_category_transform",
            "checks_subpath": "transform-checks/fact_product_category.yml"
        },
    )

    dbt_report = BashOperator(
        task_id="dbt_report",
        bash_command= # dbt run command,
    )

    soda_enforce_analytical_contract = PythonOperator(
        task_id="soda_enforce_analytical_contract",
        python_callable=run_soda_scan,
        op_kwargs={
            "project_root": PROJECT_ROOT,
            "scan_name": "model_adventureworks_sales_category_report",
            "checks_subpath": "report-checks/report_category_sales.yml"
        },
    )

    # process_failing_records_report = EmptyOperator(task_id="process_failing_records_report")
    publish_data = EmptyOperator(task_id="publish_data")

    ingest_raw_data >> soda_enforce_ingestion_contract >> soda_process_failing_records_ingest >> dbt_transform >> soda_enforce_transformation_contract  >> dbt_report >> soda_enforce_analytical_contract >> publish_data
