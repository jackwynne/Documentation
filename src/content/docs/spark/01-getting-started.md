---
title: Getting Started with Spark
description: Getting started with Spark
lang: en
---

## What is Apache Spark?

![250px-Apache_Spark_Logo.svg.png](../../../assets/logo/sparklogo.png)

> an open-source distributed general-purpose cluster-computing framework

You can think of Apache Spark as a parallel Pandas + Scikit-learn.

It provides a general way to express operations on data that can be done in a distributed environment, and handles execution in those environments:

- Manages the distribution of data and computed results across workers
- Parallelise your operations, and manages the parallel execution across workers
- Handles and manages worker and task failures and provides fault tolerance

**Spark allows you to work with data at virtually any scale, from megabytes to terabytes, and compute of any size, from a single laptop to 1000s of virtual machines, without changing a single line of code!**

### What does Apache Spark support?

- Written in Scala, but has APIs for Python (PySpark), Scala, Java, R (SparkR) and SQL
- Supports batch and streaming workloads, and the same code can be used in both types of workloads
- Has a large Machine Learning library providing parallel implementations of many ML algorithms
- Supports a very wide range of data formats (e.g. CSV, Avro, Parquet, Json), and a huge number of input and output sources, for example Blob, S3, HDFS, JDBC, ADLS, and additional sources can easily be developed
- Supports a number of compute engines: local machine, Hadoop YARN, Apache Mesos and Kubernetes

### Why should you use Apache Spark?

- Easy to use APIs allowing you to express data operations that are distributed across workers
- Open-source framework with wide adoption (Amazon, Google, Alibaba etc) and a large amount of support (AWS, Azure, Databricks, Cloudera etc) - free from vendor lock-in
- Large number of pre-built libraries and algorithms included and Spark community provides many more
- Data applications can be developed locally and unit-tested before being run in an environment - software development best practices should also apply to data applications

### Where can I find more information?

The quick start guide for Apache Spark can be found here: https://spark.apache.org/docs/latest/quick-start.html

## What is Azure Databricks?

![azure_databricks.png](/.attachments/azure_databricks-571dfd4a-713a-4d8b-a6ab-424557bd33b5.png)

> [Azure Databricks] provides a Unified Analytics Platform that accelerates innovation by unifying data science, engineering and business

Azure Databricks provides a fully managed Spark-as-a-Service platform, giving you Notebooks, job scheduling and table/metadata management.

Azure Databricks will manage the provisioning of clusters, scaling up and down of compute nodes and termination of clusters for you - there is no ongoing administration once the platform is created (e.g. patching, upgrades etc).

Azure Databricks and Apache Spark force you to separate storage and compute - there is no persistent storage within a cluster, all storage is cloud-based. Due to this, Azure Databricks is completely pay-as-you-go: you only pay the hourly rate for licenses and resources (VMs and attached temporary storage) that is provisioned at the time.

And best of all, Azure Databricks will scale up and scale down compute and storage as load fluctuates within clusters (even within jobs!).

### What capability does Azure Databricks offer?

- Databricks are the largest maintainer and contributor to Apache Spark, and have the most stable distribution
- Fully-managed Spark platform, supporting Python, R, Scala and SQL cells in Notebooks
- Fully collaborative workspace, allowing shared clusters, notebooks and metadata
- Job scheduling, allowing you to schedule workloads from Notebooks or compiled Jar and Python applications
- In Azure, full integration with Azure AD providing RBAC to notebooks, clusters, jobs and data
