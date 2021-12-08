# paas-data-ingestion
Ingest and prepare data with AWS lambdas, Snowflake and dbt in a scalable, fully replayable manner.

## Overview
This repository contains a fully PaaS infrastructure for data ingestion and transformation at scale. This repository has a companion blog post (forthcoming), to which we refer the reader for in-depth analysis of the proposed patterns and the motivations behind the project.

The ingestion pipeline mimics a typical data flow for data-driven applications: clients send events, an endpoint collects them and dumps them into a stream, finally a data warehouse stores them for further processing:

<img src="https://github.com/jacopotagliabue/paas-data-ingestion/blob/main/images/data_pattern_viz.jpg" width="640">

We make use of three main technologies:

* [Pulumi](https://www.pulumi.com/), which allows us to manipulate infrastructure-as-code, and to do so in a language we are very familiar with, Python (this is our first project using it coming from Terraform, so any feedback is very welcome!).
* [Snowflake](https://signup.snowflake.com/), which allows us to store raw data at scale and manipulate it with powerful SQL queries, abstracting away all the complexity of distributed computing in a simple API.
* [dbt](https://www.getdbt.com/), which allows us to define data transformation as versioned, replayable DAGs, and mix-and-match materialization strategies to suite our needs.

Finally, it is worth mentioning that the repository is e-commerce related as a results of mainly three factors: 

* our own experience in building [pipelines for the industry](https://github.com/jacopotagliabue/you-dont-need-a-bigger-boat);
* the availability of high-volume, high-quality data to simulate a realistic scenario;
* finally, the high bar set by e-commerce as far as data quantity and data-driven applications: due to the nature of the business and the competition, even medium-sized digital shops tend to produce an enormous amount of data, and to run pretty sophisticated ML flows on top it - in other words, something that works for e-commerce is going to reasonably work for many other use cases out of the box, as they are likely less data intensive and less sophisticated ML-wise.

Our goal was to keep the code realistic enough for the target use cases, but simple enough as to make it easy for everybody to port this stack to a different industry.

## Prerequisites

TBC

## Structure

The project is divided in three main components and a simulation script.

### Infrastructure

TBC

### dbt

TBC

### AWS lambda

A simple AWS lambda function implementing a [data collection pixel](https://medium.com/tooso/serving-1x1-pixels-from-aws-lambda-endpoints-9eff73fe7631). Once deployed, the resulting `/collect` endpoint will accept `POST` requests from clients sending e-commerce data: the function is kept simple for pedagogical reason - after accepting the body, it prepares a simple but structured event, and uses another AWS PaaS service, Firehose, to dump it in a stream for downstream storaeg and further processing.

### Data pumper

To simulate a constant stream of events reaching the collect endpoint, we provide a script that can be run at will to upload e-commerce events in the [Google Analytics](https://developers.google.com/analytics/devguides/collection/protocol/v1/parameters) format.

The events are based on the real-world clickstream dataset open sourced in 2021, the [Coveo Data Challenge](https://github.com/coveooss/SIGIR-ecom-data-challenge) dataset. By using real-world anonymized events we provide practitioners with a realistic, non-toy scenario to get acquainted with the design patterns we propose. Please cite our work, share / star the repo if you find the dataset useful!

## How to run it

TBC

### Pumper

* To pump data into the newly created stack, `cd` into the `pumper` folder, create a virtual environment and activate it, then install the requirements with `pip install -r requirements.txt`.
* Download and unzip the [Coveo Data Challenge dataset](https://github.com/coveooss/SIGIR-ecom-data-challenge) into a local folder and write down the full path, e.g. `myfolder/train`
* Create a copy of `.env.local` named `.env`, and use the dataset path as the value for `DATA_FOLDER`, the AWS url for the lambda function for `COLLECT_URL`.
* Run `python pumper.py` to start sending Google Analytics events to the endpoint. The script will run until `N_EVENTS` has been sent (change the variable in the script to what you like).

Please note that at every run, `pumper.py` will send events as they are happening in that very moments: so running the code two times will not produce duplicate events, but events with similar categorical features and different id, timestamp etc.

## Contributors

This project has been brought to you with love by:

* [Luca Bigon](https://www.linkedin.com/in/bigluck/): design, infrastructure, SQL-ing
* [Jacopo Tagliabue](https://www.linkedin.com/in/jacopotagliabue/): design, data

## License
The code is provided "as is" and released under an open MIT License.