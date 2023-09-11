# Writing directly from Azure Databricks notebook to Power BI

This page outlines how to write a dataframe as a table and then connect to this table within Power BI.

1. First we need to create a Database to store the output data from Databrick.
 `%sql
  create database if not exists {database_name};
  use {database_name};`
2. We have to write our dataframe to a table within Databricks. As one would assume setting write mode to overwrite means that the previous dataset will be overwritten everytime the below code is run.
`df.write.mode("overwrite").saveAsTable("{database_name}.{name-of-table}")`
2. Now we need to connect the table to Power BI. Once you have opened Power BI click on the "Get Data" option this will open a window with ways of reading in data to Power BI. In the search bar type "Azure Databricks" and you should see the connector.
3. Once you have clicked on the connector you will see a window like below
![image.png](/.attachments/image-b27c3ddd-80d8-447e-82ad-801adafa207b.png)

4. In order to get the required information for the connection open databricks on click on the "Compute" section. This will open a window like below. ![image.png](/.attachments/image-212e1ae9-6b2b-4ebd-bda9-f95cb23ef611.png)
Click through to the project cluster (doesn't actually matter what cluster you choose).

5. This will open the window.
![image.png](/.attachments/image-0e330975-411f-4be1-a7ce-560106f02189.png)
Click on advanced options and then the JDBC/OBC which gives you the required Server Hostname and HTTP Path. Add those to the associated fields in the Power BI connector and click ok.

6. You will then be sent to the Navigator window. Click through the hive_metastore and you should see you project id and the associated tables.
![image.png](/.attachments/image-3696c303-9815-4401-952d-099b0afd0316.png)
Click the box next to the table you want to read and voila!