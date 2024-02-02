#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
from cassandra.cluster import Cluster

# Load the Excel file
df = pd.read_csv("raw_data_all.csv")
df['tuition_fees'].fillna('No tuition fees available',inplace=True)
df['teaching_language'].fillna('No teaching language available', inplace=True)
df['university_name'].fillna('No info available',inplace=True)
df['course_name'].fillna('No info available',inplace=True)
df['degree_name'].fillna('No info available',inplace=True)
df['course_duration'].fillna('No info available',inplace=True)
df['course_description'].fillna('No info available',inplace=True)
df['ECTS'].fillna('No info available',inplace=True)
df['university_link'].fillna('No info available',inplace=True)

cluster = Cluster([])
session = cluster.connect('de_project_group10')

#Table 1
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.course_fees_by_language_and_name (course_name, university_name, 
    teaching_language,
    tuition_fees,
degree_name,
id)
    VALUES (%s, %s, %s, %s, %s,  uuid());
    """
    session.execute(insert_query, (row['course_name'], row['university_name'], row['teaching_language'], row['tuition_fees'], row['degree_name']))
print('Data inserted into Table 1!')

#Table2
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.course_details_by_field_country_duration
    (country, degree_name,
    course_duration,
    course_name, university_name, 
    course_description,
    id)
    VALUES (%s, %s, %s, %s, %s, %s,  uuid());
    """
    session.execute(insert_query, (row['country'], row['degree_name'], row['course_duration'], row['course_name'], row['university_name'], row['course_description']))
print('Data inserted into Table 2!')    

#Table3   
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.cs_course_languages_in_denmark
    (teaching_language,
    course_name, university_name,
    ECTS,
    country,
    id)
    VALUES (%s, %s, %s, %s, %s,  uuid());
    """
    session.execute(insert_query, (row['teaching_language'], row['course_name'], row['university_name'], row['ECTS'], row['country']))
print('Data inserted into Table 3!')

#Table4
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.economics_degree_at_lmu
    (university_name,
    degree_name, university_link,
    id)
    VALUES (%s, %s, %s,  uuid());
    """
    session.execute(insert_query, (row['university_name'], row['degree_name'], row['university_link']))
print('Data inserted into Table 4!')

#Table5
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.math_degrees_with_ects
    (university_name,
    course_name, course_location,
    degree_name,
    ECTS,
    id)
    VALUES (%s, %s, %s, %s, %s, uuid());
    """
    session.execute(insert_query, (row['university_name'], row['course_name'], row['course_location'], row['degree_name'], row['ECTS']))
print('Data inserted into Table 5!')

#Table6
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.data_science_course_duration_in_germany
    (course_name,
    degree_name,
    course_duration,
    country,
    id)
    VALUES (%s, %s, %s, %s, uuid());
    """
    session.execute(insert_query, (row['course_name'], row['course_name'], row['course_duration'], row['country']))
print('Data inserted into Table 6!')

#Table7
for _, row in df.iterrows():
    insert_query = """
    INSERT INTO de_project_group10.english_taught_courses_with_ects
    (course_name,
    university_name,
    degree_name,
    ECTS,
    teaching_language,
    country,
    id)
    VALUES (%s, %s, %s, %s, %s, %s, uuid());
    """
    
    session.execute(insert_query, (row['course_name'], row['university_name'], row['degree_name'], row['ECTS'], row['teaching_language'], row['country']))
print('Data inserted into Table 7!')

