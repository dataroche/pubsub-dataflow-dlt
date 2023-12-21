# Stream GCP Pub/sub JSON messages to a Postgres database

## _Experimental_

There are some multithreading/multiprocessing issues on Dataflow with DLT (data load
tool). The code works, but there are many errors in the logs. Use at your own risk!

## Usage

### Config

- input_subscription: The input Pub/Sub subscription
- sql_url: The sqlalchemy-compatible URL to your postgres database. I.e. `postgresql://user:password@localhost:5432/db`
- table_name_data_key: (optional) If defined, the JSON data can contain a specific key
  `table_name_data_key` that will define the output table name. I.e. if
  `table_name_data_key=eventName`, the JSON data should be in the format of `{
"eventName": "something", ... }`. Such an event would be stored in a table named
  `something`.
- table*name_prefix: (optional) Prefix all table names with this string. I.e. if
  `table_name_prefix=raw_events*`, the previous example would yield a table named `raw_events_something`

### Dataflow deployment

```
python pubsub_dlt_launcher.py \
    --region us-central1 \
    --input_subscription projects/my-project/subscriptions/my-subscription \
    --table_name_data_key eventName \
    --table_name_prefix raw_events_ \
    --runner DataflowRunner \
    --project my-project \
    --temp_location gs://my-project-temp/dataflow-tmp \
    --sql_url postgresql://user:password@localhost:5432/db \
    --setup_file ./setup.py \
    --max_num_workers 3
```
