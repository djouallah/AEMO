from google.cloud import bigquery


def load(request):
    client = bigquery.Client()
    dataset_id = 'aemodataset'

    dataset_ref = client.dataset(dataset_id)
    #### COPY FROM GOOGLE SORAGE TO BIGQUERY, DUNIT
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("UNIT", "STRING"),
        bigquery.SchemaField("DUID", "STRING"),
        bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
        bigquery.SchemaField("INTERVENTION", "STRING"),
        bigquery.SchemaField("INITIALMW", "NUMERIC"),
    ]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://ameoclean/UNITD*"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("DUNIT"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("DUNIT"))
    print("Loaded {} rows.".format(destination_table.num_rows))

    #### COPY FROM GOOGLE SORAGE TO BIGQUERY, TUNIT
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("UNIT", "STRING"),
        bigquery.SchemaField("DUID", "STRING"),
        bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
        bigquery.SchemaField("INITIALMW", "NUMERIC"),
    ]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://ameoclean/UNITT*"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("TUNIT"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("TUNIT"))
    print("Loaded {} rows.".format(destination_table.num_rows))

    #### COPY FROM GOOGLE SORAGE TO BIGQUERY, DREGION
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("UNIT", "STRING"),
        bigquery.SchemaField("REGIONID", "STRING"),
        bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
        bigquery.SchemaField("INTERVENTION", "STRING"),
        bigquery.SchemaField("RRP", "NUMERIC"),
    ]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://ameoclean/REGIOND*"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("DREGION"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("DREGION"))
    print("Loaded {} rows.".format(destination_table.num_rows))


    #### COPY FROM GOOGLE SORAGE TO BIGQUERY, TREGION
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = [
        bigquery.SchemaField("UNIT", "STRING"),
        bigquery.SchemaField("REGIONID", "STRING"),
        bigquery.SchemaField("SETTLEMENTDATE", "TIMESTAMP"),
        bigquery.SchemaField("RRP", "NUMERIC"),
    ]
    job_config.skip_leading_rows = 1
    # The source format defaults to CSV, so the line below is optional.
    job_config.source_format = bigquery.SourceFormat.CSV
    uri = "gs://ameoclean/REGIONT*"

    load_job = client.load_table_from_uri(
        uri, dataset_ref.table("TREGION"), job_config=job_config
    )  # API request
    print("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(dataset_ref.table("TREGION"))
    print("Loaded {} rows.".format(destination_table.num_rows))

    #### SCHEDULE QUERY UNITARCHIVE
    dataset_id = 'ReportingDataset'
    table_id='UNITARCHIVE'
    dataset_ref = client.dataset(dataset_id)
    location_table='asia-northeast1'


    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    sql = """
        WITH
    tt AS (
    SELECT
        UNIT,DUID, SETTLEMENTDATE,
        ARRAY_AGG(STRUCT(INTERVENTION, INITIALMW)
        ORDER BY
        INTERVENTION DESC LIMIT 1)[OFFSET (0)].* FROM  `test-187010.aemodataset.DUNIT` t
    WHERE
        DATE(SETTLEMENTDATE) < CURRENT_DATE('+10:00') AND INITIALMW !=0
    GROUP BY
        UNIT,
        DUID,
        SETTLEMENTDATE
    ORDER BY
        duid)
    SELECT
    UNIT,
    DUID,
    SETTLEMENTDATE,
    INITIALMW
    FROM
    tt
    UNION ALL
    SELECT
    UNIT,
    DUID,
    SETTLEMENTDATE,
    INITIALMW
    FROM
    `test-187010.aemodataset.TUNIT`
    WHERE
    DATE(SETTLEMENTDATE) < CURRENT_DATE('+10:00')
    AND INITIALMW !=0

    """

    query_job = client.query(sql,location='asia-northeast1',job_config=job_config)

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))

    #### SCHEDULE QUERY PRICEARCHIVE
    dataset_id = 'ReportingDataset'
    table_id='PRICEARCHIVE'
    dataset_ref = client.dataset(dataset_id)
    location_table='asia-northeast1'


    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    table_ref = client.dataset(dataset_id).table(table_id)
    job_config.destination = table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    sql = """
         WITH
         tt AS(  SELECT    UNIT,    REGIONID,    SETTLEMENTDATE,    RRP  FROM
         `test-187010.aemodataset.DREGION`
         WHERE
          INTERVENTION="0"
          AND DATE(SETTLEMENTDATE) < CURRENT_DATE('+10:00')
          UNION ALL
          SELECT
          UNIT,    REGIONID,    SETTLEMENTDATE,    RRP
          FROM
          `test-187010.aemodataset.TREGION`
          WHERE
          DATE(SETTLEMENTDATE) < CURRENT_DATE('+10:00'))
         SELECT
          CASE when UNIT ="DREGION" then "DUNIT" else "TUNIT"  end as UNIT,REGIONID,SETTLEMENTDATE,RRP
          FROM  tt

    """
    query_job = client.query(sql,location='asia-northeast1',job_config=job_config)

    query_job.result()
    print('Query results loaded to table {}'.format(table_ref.path))