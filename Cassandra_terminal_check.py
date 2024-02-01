#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd

# Load the Excel file
df = pd.read_excel("raw_data_all.xlsx")


from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('de_project_check')

create_table_query = """
CREATE TABLE de_project_check.tuition_fees_by_language_and_course_name (
    course_name TEXT,
    university_name TEXT,
    teaching_language TEXT,
    tuition_fees DECIMAL,
    PRIMARY KEY ((teaching_language), course_name)
"""
session.execute(create_table_query)

for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_check.tuition_fees_by_language_and_course_name (course_name, university_name, 
    teaching_language,
    tuition_fees)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(insert_query, (row['course_name'], row['university_name'], row['teaching_language'], row['tuition_fees']))

# query = """
# SELECT * FROM Trial_Keyspace.courses
# WHERE program_duration LIKE '% semesters'
# AND CAST(SUBSTRING(program_duration FROM 1 FOR POSITION(' ' IN program_duration)) AS INT) >= 3
# AND beginning LIKE '%Winter%'
# AND degree LIKE '%Master%'
# """
# rows = session.execute(query)
# for row in rows:
#     print(row)

