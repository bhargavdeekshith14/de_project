#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Query1
from cassandra.cluster import Cluster

cluster = Cluster([''])
session = cluster.connect('de_project_group10')

query1 = """
select course_name, teaching_language, tuition_fees, degree_name 
from de_project_group10.course_fees_by_language_and_name where teaching_language = 'English'
and degree_name in ('Data Science', 'Master of Science in Physics') ALLOW FILTERING;
 """
output1 = session.execute(query1)
print('Running Query 1!')
for row in output1:
    print(row)

#Query2
query2 = """
SELECT course_name, university_name, degree_name, course_description  
FROM de_project_group10.course_details_by_field_country_duration  
WHERE country IN ('Denmark', 'Netherlands')  
AND (degree_name in ('Master of Science', 'Master (2 years) of Economics and Business Administration')) 
AND course_duration = '2 years' ALLOW FILTERING;
 """
output2 = session.execute(query2)
print('Running Query 2!')
for row in output2:
    print(row)
    
#Query3
query3 = """
SELECT teaching_language, count(*)
from de_project_group10.cs_course_languages_in_denmark
where country = 'Germany'
and course_name = 'Mathematics'
group by teaching_language ALLOW FILTERING;
 """
output3 = session.execute(query3)
print('Running Query 3!')
for row in output3:
    print(row)
    
#Query4
query4 = """SELECT degree_name, course_name, university_link 
FROM de_project_group10.economics_degree_at_lmu 
WHERE university_name = 'Ludwig-Maximilians-Universität München' 
AND degree_name = 'Master of Science in Astrophysics' ALLOW FILTERING;"""

output4 = session.execute(query4)
print('Running Query 4!')
for row in output4:
    print(row)

#Query5
query5 = """
SELECT university_name, course_name, course_location, 
degree_name, ECTS 
FROM de_project_group10.math_degrees_with_ects 
WHERE ECTS = '120 ECTS'
and course_name = 'Mathematics'
ALLOW FILTERING;"""

output5 = session.execute(query5)
print('Running Query 5!')
for row in output5:
    print(row)

#Query6
query6 = """
SELECT course_duration, count(*)
FROM de_project_group10.data_science_course_duration_in_germany
WHERE course_name = 'Data Science'
GROUP BY course_duration
ALLOW FILTERING;"""

output6 = session.execute(query6)
print('Running Query 6!')
for row in output6:
    print(row)
    
#Query7
query7 = """
SELECT course_name,university_name, degree_name
FROM de_project_group10.english_taught_courses_with_ects
WHERE teaching_language = 'English'
and ECTS = '120 ECTS'
ALLOW FILTERING;"""

output7 = session.execute(query7)
print('Running Query 7!')
for row in output7:
    print(row)

