def load_table_url_json(table_id: str):
    from google.cloud import bigquery

    #construct BigQuery client
    client = bigquery.Client()
    table_id = "sylvan-plane-408707.dataset_first.demo"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("emp_name", "STRING"),
            bigquery.SchemaField("emp_dept", "STRING"),
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    uri = "gs://bigueryjsondata/data.json"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        location="US",
        job_config=job_config,
    )

    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
