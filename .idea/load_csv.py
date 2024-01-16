def load_table_url_csv(table_id: str):
    from google.cloud import bigquery

    #construct BigQuery client
    client = bigquery.Client()
    table_id = "sylvan-plane-408707.dataset_first.demo"

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("dept", "STRING"),
        ],
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV
    )
    uri = "gs://bigueryjsondata/csv_data.csv"

    load_job = client.load_table_from_uri(
        uri,
        table_id,
        job_config=job_config
    )

    load_job.result()
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))
