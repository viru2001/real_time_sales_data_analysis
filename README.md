# real_time_sales_data_analysis

### Architecture :
![Real Time Stream Processing - Architecture](https://github.com/viru2001/real_time_sales_data_analysis/assets/52121256/65f3d996-594f-4368-8caf-176e254f0c37)
---

#### Create Tables in biguery
You can find create table statements for bigquery tables in `bigquery_queries.sql` file.

--- 
Spin up linux server on google compute engine and do google CLI setup by following steps.

#### gcloud CLI setup

1. initialise gcloud CLI

   `gcloud init --no-launch-browser`

2. login in to your google account

   `gcloud init --no-launch-browser`

3. Now to get user access credentials and save them as Application Default Credentials (ADC).

   `gcloud auth application-default login --no-launch-browser`

flag `--no-launch-browser` is used in above commands to avoid browser launching and execute command via console only.

---
#### Publish data in pub/sub topic

After doing this setup, we can run the pub/sub publisher python code.

We need to install python3 and pip3 if not installed in linux server in google compute engine.

    sudo apt update

    sudo apt install python3 python3-pip

Then we need to install bigquery and pub/sub python libraries using below commands.

 `pip3 install google-cloud-pubsub`

 `pip3 install google-cloud-bigquery`

Now we can run python publisher code.

    python3 sale_data_publisher.py

---
Now data will be published into pub/sub topic and then by using GCP Dataflow it will be ingested in GCP Bigquery in realtime.

---
Analytical queries we used in dashbaord can be found in file `dashboard queries.sql`.

[Link for Dashboard](https://lookerstudio.google.com/reporting/1b5e8221-0db3-4d11-8c42-8c14ce75371c)

Dashboard Screenshots

![dashboard ss](https://github.com/viru2001/real_time_sales_data_analysis/assets/52121256/29d7f821-4604-4808-83cc-457f820b63ed)


![dashboard ss1](https://github.com/viru2001/real_time_sales_data_analysis/assets/52121256/d0aea0a6-2fdc-433a-aef5-94fa5c26b50e)

