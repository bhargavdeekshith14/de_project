#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# import pandas as pd

# df_daad = pd.read_excel('daad.xlsx')
# df_daad['country']='Germany'
# df_nl = pd.read_excel('Study in NL.xlsx')
# df_nl['country']='Netherlands'
# df_den = pd.read_excel('denmark.xlsx')
# df_den['country']='Denmark'
# daad = df_daad.reindex(["course_name", "university_name", "course_location", "teaching_language", "course_description", "ECTS", "degree_name", "course_duration", "university_link", "tuition_fees","country"],axis=1)
# nl = df_nl.reindex(["course_name", "university_name", "course_location", "teaching_language", "course_description", "ECTS", "degree_name", "course_duration", "university_link", "tuition_fees","country"],axis=1)
# denmark = df_den.reindex(["course_name", "university_name", "course_location", "teaching_language", "course_description", "ECTS", "degree_name", "course_duration", "university_link", "tuition_fees","country"],axis=1)

# finaldf = pd.concat([daad,nl,denmark],ignore_index=True)
# # Load the Excel file
# df = pd.read_excel("raw_data_all.xlsx")


from cassandra.cluster import Cluster

cluster = Cluster([''])
session = cluster.connect('de_project_group10')

def create_table1_query:
create_table_query = """
CREATE TABLE de_project_group10.tuition_fees_by_language_and_course_name (
    course_name TEXT,
    university_name TEXT,
    teaching_language TEXT,
    tuition_fees TEXT,
    PRIMARY KEY (teaching_language, course_name);
"""
session.execute(create_table1_query)
print('Table 1 Created!')

create_table2_query = """
CREATE TABLE de_project_group10.course_details_by_field_country_duration (
    country TEXT,
    degree_name TEXT,
    course_duration TEXT,
    course_name TEXT,
    university_name TEXT,
    course_description TEXT,
    id uuid PRIMARY KEY 
) ;
"""
session.execute(create_table2_query)
print('Table 2 Created!')

create_table3_query = """CREATE TABLE de_project_group10.cs_course_languages_in_denmark (
    teaching_language TEXT,
    course_name TEXT,
    university_name TEXT,
    ECTS TEXT,
    country TEXT,
    id uuid,
    PRIMARY KEY(teaching_language, id)
)  ;
"""
session.execute(create_table3_query)
print('Table 3 Created!')

create_table4_query = """
    CREATE TABLE de_project_group10.economics_degree_at_lmu (
    university_name TEXT,
    degree_name TEXT,
    university_link TEXT,
    id uuid PRIMARY KEY 
);
"""
session.execute(create_table4_query)
print('Table 4 Created!')

create_table5_query = """CREATE TABLE de_project_group10.math_degrees_with_ects (
    university_name TEXT,
    course_name TEXT,
    course_location TEXT,
    degree_name TEXT,
    ECTS TEXT,
    id uuid PRIMARY KEY
) ;
"""
session.execute(create_table5_query)
print('Table 5 Created!')

create_table6_query = """
    CREATE TABLE de_project_group10.data_science_course_duration_in_germany (
    course_name TEXT,
    degree_name TEXT,
    course_duration TEXT,
    country TEXT,
    id uuid,
    PRIMARY KEY (course_duration, id)
);
"""
session.execute(create_table6_query)
print('Table 6 Created!')

create_table7_query = """
    CREATE TABLE de_project_group10.english_taught_courses_with_ects (
    course_name TEXT,
    university_name TEXT,
    degree_name TEXT,
    ECTS TEXT,
    teaching_language TEXT,
    country TEXT,
    id uuid PRIMARY KEY);
"""
session.execute(create_table7_query)
print('Table 7 Created!')

