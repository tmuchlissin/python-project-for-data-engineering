# Python Project for Data Engineering
## _Case Study : Acquiring and Processing Information on the World's Largest Banks_

[![forthebadge made-with-python](http://ForTheBadge.com/images/badges/made-with-python.svg)](https://www.python.org/)

## Disclaimer
This repository contains my final project submission for the [Python Project for Data Engineering](https://www.coursera.org/learn/python-project-for-data-engineering) course on Coursera. The Original files were provided by IBM Developer Skill Network for this course.

## Overview
In this project, the skills and knowledge of basic Python and data engineering principles acquired throughout the course will be applied. The task involves working with real-world data to perform Extraction, Transformation, and Loading (ETL) operations. The project scenario involves a multi-national firm that requires data engineering services to access, process, and transform data according to specific requirements.


## Data Source
The data for this project will be obtained through web scraping using Python's Requests API from a public website. The goal is to compile a list of the top 10 largest banks globally based on their market capitalization, measured in billion USD. Once this initial data is extracted, it will be transformed into three additional currencies: GBP, EUR, and INR. The exchange rates necessary for this transformation will be sourced from a CSV file. After the transformation, the data will be saved locally in a CSV format, allowing for easy access and viewing. Additionally, the transformed data will be loaded into a database table, enabling managers from different countries to query the database and retrieve market capitalization values in their local currency, facilitating informed decision-making and analysis.

## Workflow
![image](https://github.com/tmuchlissin/automated-sensor-data-processing-pipeline-implementation/assets/117092055/6c28a9be-e900-497a-bcc1-e897402fee65)

- Extract Data
- Transform Data
- Load to CSV
- Load to SQL Database
- Query the Database
- Verify Log Entries


