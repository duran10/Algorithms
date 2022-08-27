#!/usr/bin/env python
# coding: utf-8

# In[56]:


import pandas as pd
from gspread_pandas import Spread, Client
from datetime import datetime
import time
import ast
import math
import copy

startTime = datetime.now()
workers_df = pd.read_csv(file_name)


data_source_list = ['typeform', 'test']



workers_df=workers_df[workers_df['data_source'].isin(data_source_list)]


##create dataframe for filtering values
algorithm_df = workers_df[['submission_id']].copy()








#workers_df.drop(rankings_columns, axis=1, inplace=True)



#print(workers_df.head())

####This code is how you turn the objects you store in google sheets back into a list 
#import ast
#for index, row in df.iterrows():
#print(workers_df['available_days_of_week_test'])
    #recruit_row=row['available_days_of_week_test']
    #recruit_row = ast.literal_eval(recruit_row)
    #print(recruit_row[0])


# In[57]:


def strip_value(x):
    '''This function takes the entries from google sheets which are pseudo lists and turns them into real python lists'''
    x=[i.strip() for i in x]
    #x=[i.encode('utf-8') for i in x]
    x=','.join("'{0}'".format(i) for i in x)
    x = x.split(",")
    x = [i.replace("'", "") for i in x] # remove quote from each element
    return x


variable_list_job_seekers=['has_ss_number','industry_experience_type', 'experience_time', 'job_sought', 'shift_type', 'contract_type', 'available_days_of_week', 'educational_attainment', 'languages_spoken', 'personal_traits_one', 'personal_traits_two', 'personality_type']
for var in variable_list_job_seekers:
    workers_df[var]=workers_df[var].str.split(",", expand=False)
    workers_df[var]=workers_df[var].apply(strip_value)


  

#workers_df["industry_experience_type"]=workers_df["industry_experience_type"].str.split(",", expand=False)


#workers_df["industry_experience_type"]=workers_df["industry_experience_type"].apply(strip_value)




# In[58]:


#name = input("What's your name? ")
#print("Nice to meet you " + name + "!")

def my_input(x):
    '''This forces the user to hit either Y or N'''
    while True:
        x=input(x)
        x=x.upper()
    
        if x not in ('Y', 'N'):
            print("Not an appropriate choice.")
            continue
        else:
            break
    return x



#recruit_row=recruit_df.iloc[20]
recruit_input=input("Please enter the submission id of the recruitment request: ")
#recruit_input='ab28e642381e4490406c9380304a45e8'

algorithm_type=int(input("Press 1 if you are searching for candidates to interview for a job and press 2 if you have already interviewed candidates and are searching for placement"))


rankings_columns=['submission_id', 'do_not_contact', 'application_status', 'applications_status_notes', 'application_status_change_date', 'phone_interview_date', 'live_interview_date', 'punctuality_and_attendance', 'appearance_and_hygiene', 'communication_skills', 'knowledge_evaluation', 'reliability', 'ambition', 'plans_to_stay_in_the_area', 'hardworking_and_proactive', 'articulateness', 'aptitude', 'casual_restaurant_fit', 'fine_dining_fit', 'tourist_friendly_fit', 'portuguese_restaurant_fit', 'trendy_restaurant_fit', 'retention_likelihood', 'red_flags_scale']
rankings_columns_original=copy.deepcopy(rankings_columns)
if algorithm_type==2:
    print("Please answer Y or N for the following questions...")
    #input("Press Enter to continue...")
    casual_restaurant=my_input("Is it a casual restaurant?")
    fine_dining=my_input("Is it fine dining?")
    tourist_friendly=my_input("Is it a place for tourists?")
    portuguese_restaurant=my_input("Is it a Portuguese restaurant?")
    trendy_restaurant=my_input("Is it a trendy restaurant?")
    
    if casual_restaurant=='N':
        rankings_columns.remove('casual_restaurant_fit')
    else:
        pass
    if fine_dining=='N':
        rankings_columns.remove('fine_dining_fit')
    else:
        pass
    if tourist_friendly=='N':
        rankings_columns.remove('tourist_friendly_fit')
    else:
        pass
    if portuguese_restaurant=='N':
        rankings_columns.remove('portuguese_restaurant_fit')
    else:
        pass
    if trendy_restaurant=='N':
        rankings_columns.remove('trendy_restaurant_fit')
    else:
        pass    
    ###create dataframe for interview rankings
    ranking_df=workers_df[rankings_columns].copy()
    my_variables=[casual_restaurant, fine_dining, tourist_friendly, portuguese_restaurant, trendy_restaurant]
    ranking_columns_to_remove=[]
    for var in rankings_columns_original:
        if var in rankings_columns:
            pass
        else:
            ranking_columns_to_remove.append(var)
else:
    pass



#['casual_restaurant_fit', 'fine_dining_fit', 'tourist_friendly_fit', 'portuguese_restaurant_fit', 'trendy_restaurant_fit']
#for var in my_variables:
    #print(var)

#casual_restaurant_fit fine_dining_fit tourist_friendly_fit portuguese_restaurant_fit	trendy_restaurant_fit



# In[59]:


#print(rankings_columns)


# In[60]:



def translate_language(x, mydict):
    '''This function takes a dictionary of field names in one language and replaces it with field names in another language'''
    new_list=[]
    for item in x:
        translation=mydict.get(item, item)
        new_list.append(translation)
    new_list=','.join("'{0}'".format(i) for i in new_list)
    new_list = new_list.split(",")
    return new_list

def iterate_pandas(x, mydict, mydf):
    '''When iterating through a dataframe, this takes a row from the dataframe, then turns it from double quote into a single quote '''
    '''Then it calls the translate language function'''
    '''Then it stores the data back where it found it'''
    get_row_contents=row[x]
    
    get_row_contents=[x.strip("'") for x in get_row_contents]
    get_row_contents1=translate_language(get_row_contents, mydict)
    #get_row_contents=[i.encode('utf-8') for i in get_row_contents]
    get_row_contents1 = [i.replace("'", "") for i in get_row_contents1] # remove quote from each element
    #print(index, x)
    #print(get_row_contents1)
    #print(type(get_row_contents1))
    mydf.loc[index, x] = get_row_contents1



##These are the translation dictionaries from portuguese to english
industry_experience_dict={"Copeiro": "Dish Washer", "Serviço à mesa": "Table Service", "Serviço de balcão": "Counter Service", "Bartender": "Bartender", "Receção": "Host", "Ajudante de Cozinha": "Kitchen Helper", "Cozinheiro": "Cook"}
time_experience_dict={"Menos do 1 ano": "Less than 1 year","1 ao 2 anos": "1 to 2 years","3 ao 5 anos": "3 to 5 years","6 ao 10 anos": "6 to 10 years","mais de 11 anos": "more than 11 years"}
job_desired_dict={"Serviço à mesa": "Table Service","Serviço de balcão": "Counter Service","Bartender": "Bartender","Receção": "Host","Ajudante de Cozinha": "Kitchen Helper","Cozinheiro": "Cook","Chef de Cozinha": "Chef"}
shift_type_dict={"Full Time": "Full Time","Part Time": "Part Time","Extras": "On demand"}

contract_type_dict={"1-3 meses": "1-3 months","3-6 meses": "3-6 months","6 meses a 1 ano": "6 months to 1 year","Mais de 1 ano": "More than 1 year","Indefinida": "Indefinite","Qualquer duração": "Any of these"}

days_dict={"segunda-feira": "Monday","terça-feira": "Tuesday","quarta-feira": "Wednesday","quinta-feira": "Thursday","sexta-feira": "Friday","sábado": "Saturday","domingo": "Sunday"}

educ_dict={"Nenhum em especifíco": "Does not matter","Escola primária": "Primary school", "Ensino Básico":"Middle School", "Ensino secundário": "Secondary school","Ensino superior": "University Degree or above","Curso na área": "Technical course in the area","Curso superior na área": "University Degree in the area","Mestrado ou Doutorado": "Master's or Doctorate"}

language_dict={"Português": "Portuguese","Inglês": "English","Espanhol": "Spanish","Francês": "French","Alemão": "German"}
personal_traits_one_dict={"Jogador de equipa": "Team Player","Profissional": "Professional","Cativante perante o cliente": "Excellent Communication With Customers","Simpático": "Caring","Personalidade amistosa": "Friendly Personality"}
personal_traits_two_dict={"Pontual": "Punctual","Humilde": "Humble","Multitarefa": "Multi-tasker","Bom trabalhador": "hard worker","Atitude inteligente e positiva": "smart and positive attitude","Integridade": "has integrity","Aprumo com a aparência": "neat appearance / well dressed"}
personality_type_dict={"Enérgico": "energetic","Sério": "serious","Brincalhão": "playful","Tranquilo": "easy going","Desenrascado": "fast moving"}
documents_dict={"Nenhuma": "None of the above", "Nenhum deles":"None of the above", "Número de Identificação Fiscal": "Fiscal Identification Number", "Número de Segurança Social": "Social Security number", "Cidadania Europeia": "EU Citizenship", "Visto de Residência": "Residence Permit/Residency Visa"}





###This iterates through every row of the dataframe and translates all of the variables
for index, row in workers_df.iterrows():
    iterate_pandas('industry_experience_type', industry_experience_dict, workers_df )
    #quit()
    iterate_pandas('experience_time', time_experience_dict, workers_df) 
    iterate_pandas('job_sought',job_desired_dict, workers_df)
    iterate_pandas('shift_type', shift_type_dict, workers_df)
    iterate_pandas('contract_type', contract_type_dict, workers_df)
    iterate_pandas('available_days_of_week', days_dict, workers_df)
    iterate_pandas('educational_attainment', educ_dict, workers_df)
    iterate_pandas('languages_spoken', language_dict, workers_df)
    iterate_pandas('personal_traits_one', personal_traits_one_dict, workers_df)
    iterate_pandas('personal_traits_two', personal_traits_two_dict, workers_df)
    iterate_pandas('personality_type', personality_type_dict, workers_df)
    iterate_pandas('has_ss_number', documents_dict, workers_df)


# ## recruitment is below

# In[61]:






# 'Example Spreadsheet' needs to already exist and your user must have access to it
s = Spread('jordan@tuki.today', 'Database January 2019 version')  
#output = Spread('jordan@tuki.today' , 'Database January 2019 version Output') 
# This will ask to authenticate if you haven't done so before for 'example_user'

# Display available worksheets
#print(s.sheets)

s.open_sheet('recruitment') 
#df = pd.read_excel('Database January 2019 version.xlsx', sheetname='recruitment')
#Database January 2019 version
df=s.sheet_to_df(index=None) 


recruit_df=pd.DataFrame(df)


##If the data series is extracted in any other way many of the variable types get corrupted, so we extract based on index

myindex=recruit_df.index[recruit_df['submission_id'] == recruit_input].tolist()

myindex=myindex[0]


##transform the dataframe into a series

recruit_row=recruit_df.iloc[myindex]
restaurant_name=recruit_row['restaurant']
#time_of_day=recruit_row['time_of_day']

#print("is this real", time_of_day)
#dkjlf

##change this to what you will input in the program



# In[62]:



##List of variables from the recruitment worksheet we want to translate
variable_list_recruitment=['job_sought', 'experience_time', 'shift_type', 'contract_type', 'time_of_day', 'days_of_week', 'wage_type', 'languages_spoken', 'educational_attainment', 'document_type', 'personal_traits_one', 'personal_traits_two', 'personality_type']
#variable_list_recruitment=['time_of_day']
##uncomment if need be..leave it until you verify the byte nightmare
for var in variable_list_recruitment:
    #print("step 0", recruit_df[var])
    recruit_df[var]=recruit_df[var].str.split(",", expand=False)
    #print("step 1", recruit_df[var])
    recruit_df[var]=recruit_df[var].apply(strip_value)
    #print("step 2", recruit_df[var])
job_duration_dict={"1-3 meses": "1-3 months","3-6 meses": "3-6 months","6 meses a um ano": "6 months to 1 year","1 ano ou mais": "More than 1 year","Indefinida": "Indefinite"}
time_of_day_dict={"manhã": "morning","início da tarde": "early afternoon","final de tarde": "late afternoon","início da noite": "early evening","Final de noite": "late night"}
wage_type_dict={"Salário à hora": "Hourly wage","Salário mensal": "Monthly wage"}

###This iterates through every row of the recruitment dataframe and translates all of the variables
startTime = datetime.now()
#time.sleep(1)


for index, row in recruit_df.iterrows():
    iterate_pandas('job_sought', job_desired_dict, recruit_df)
    iterate_pandas('experience_time', time_experience_dict, recruit_df)
    iterate_pandas('shift_type', shift_type_dict, recruit_df)
    iterate_pandas('contract_type', contract_type_dict, recruit_df)
    iterate_pandas('time_of_day',time_of_day_dict, recruit_df)
    iterate_pandas('days_of_week',days_dict, recruit_df)
    iterate_pandas('wage_type', wage_type_dict, recruit_df)
    iterate_pandas('languages_spoken', language_dict, recruit_df)
    iterate_pandas('educational_attainment', educ_dict, recruit_df)
    iterate_pandas('document_type', documents_dict, recruit_df)
    iterate_pandas('personal_traits_one', personal_traits_one_dict, recruit_df)
    iterate_pandas('personal_traits_two', personal_traits_two_dict, recruit_df)
    iterate_pandas('personality_type', personality_type_dict, recruit_df)
    
#print(datetime.now() - startTime)
    
#for var in variable_list_recruitment:
    #print("step 3", recruit_df[var])
# # Algorithm development
# ## Dealbreaker categories
# ### Day of the week

# In[63]:


def str_to_bool(s):
    '''translates a string object that should be a boolean object into real boolean object'''
    '''Use for true/false'''
    if isinstance(s, str):
        x=s.title()
        if x == 'True':
            return True
        elif x == 'False':
            return False
        else:
            raise ValueError 
    else:
        return s


    



days_list=["Monday", "Tuesday", "Wednesday", "Thursday", "Friday","Saturday", "Sunday"]




#print(workers_df['available_days_of_week'])
#print(recruit_df['job_sought'])

#print(workers_df['restriction_days_of_week_boo'])
    
workers_df['restriction_days_of_week_boo'] = workers_df['restriction_days_of_week_boo'].apply(str_to_bool)    
#for index, row in workers_df.iterrows():
    
    #print(type(row['restriction_days_of_week_boo']), row['restriction_days_of_week_boo']) 
    
#df['new column name'] = df['df column_name'].apply(lambda x: 'value if condition is met' if x condition else 'value if condition is not met')


##day of week match
###if there are any days in recruiter dataframe that are not in workers data frame, set restriction_days_of_week_pass to false




daysofweek=recruit_row['days_of_week']

for index, row in workers_df.iterrows():
    extract_available_days_of_week=row['available_days_of_week']
    open_availability=row['restriction_days_of_week_boo']
    days_match=all(elem in extract_available_days_of_week for elem in daysofweek )
    if open_availability is True:
        open_availability1=False
    else:
        open_availability1=True
    if days_match is True or open_availability1 is True:
        update_row=True
    else:
        update_row=False
    algorithm_df.loc[index, 'restriction_days_of_week_pass'] = update_row
    

    
    
    



# ### time of day

# In[64]:


def clock_range(time_end_unavailable, time_start_unavailable):
    '''Take the time that a worker becomes available and unavailable and turn it into a list of hours'''
    if time_end_unavailable > time_start_unavailable:
        inflated_time=time_start_unavailable+24
        myrange=list(range(time_end_unavailable,inflated_time, 1))
        myrange=[i -24 if i > 23 else i for i in myrange ]
        return myrange
    else:
        myrange=list(range(time_end_unavailable,time_start_unavailable, 1))
        return myrange   


time_of_day=recruit_row['time_of_day']
#print("hi", time_of_day)
#time_of_day=[x.strip("'") for x in time_of_day]
##define a translation of time blocks from the typeform to hours of the day
time_of_day_translations={'morning':[6,7,8,9,10,11],'early afternoon':[12,13,14,15], "late afternoon":[16,17,18, 19], "early evening":[20,21,22, 23], "late night":[0,1,2,3,4,5]}
###Turn the string of true-false into a boolean value
workers_df['restrictions_time'] = workers_df['restrictions_time'].apply(str_to_bool)

###translate time of day blocks into hours
time_range_employer=[]
#time_of_day1=[i.encode.decode() for i in time_of_day]
#time_of_day1=time_of_day[0]
#print("time_of_day1", time_of_day1)
#time_of_day2=time_of_day1.decode()
#print(time_of_day2) 
#fdsfdsf
for x in time_of_day:
    #if x is not None:
    #print(x)
    time_translation=time_of_day_translations.get(x)
    #print(time_translation)
    time_range_employer.extend(time_translation)
#print(time_range_employer)
#print(time_range_employer)

##This loop can be organized with the other loops
###
for index, row in workers_df.iterrows():
    time_start_unavailable=row['time_start_unavailable']
    if time_start_unavailable:
        ###translate a string representation of time into an integer object    
        time_start_unavailable=time.strptime(time_start_unavailable, "%H:%M")
        time_start_unavailable=int(time_start_unavailable.tm_hour)
    else:
        time_start_unavailable=int()
    time_end_unavailable=row['time_end_unavailable']
    if time_end_unavailable:
            
        time_end_unavailable=time.strptime(time_end_unavailable, "%H:%M")
        time_end_unavailable=int(time_end_unavailable.tm_hour)
    else:
        time_end_unavailable=int()
    ###get the workers potential schedule
    worker_schedule=clock_range(time_end_unavailable, time_start_unavailable)
    ###find the intersection of when a worker is available and when the employer demands availability
    schedule_overlap=set(time_range_employer).intersection(worker_schedule)
    ####Find the number of hours that they overlap
    schedule_overlap_length=len(schedule_overlap)
    #####Find the number of hours the employer asks for availability
    time_range_employer_length=len(time_range_employer)
    ##Find the percentage of hours where each match
    percent_overlap=schedule_overlap_length/time_range_employer_length
    ####We don't demand 100% overlap of schedules to pass the test because interpretations of time of day blocks are subjective 
    ####and sometimes these things can be talked about to achieve an understanding, it is best left for humans to decide
    if percent_overlap > 0.5:
        time_schedule_pass=True
    else:
        time_schedule_pass=False
    ####get if they have no time restrictions from the yes no question
    restrictions_time=row['restrictions_time']
    ##flip the variable to make it logical for our purposes
    if restrictions_time is True:
        no_restrictions_time=False
    else:
        no_restrictions_time=True
    ###If either they meet our time schedule threshold or they said they have open availability, they pass this test
    if no_restrictions_time is True or time_schedule_pass is True:
        update_row=True
    else:
        update_row=False
    algorithm_df.loc[index, 'restriction_time_pass'] = update_row
    #print("schedule_overlap:", schedule_overlap, "schedule_overlap_length:", schedule_overlap_length)
    #print("\n")
    #print("employer:", time_range_employer, "worker:", time_end_unavailable, time_start_unavailable,  worker_schedule, "schedule_overlap:", schedule_overlap, "percent_overlap:", percent_overlap )
    
    

#workers_df['restriction_days_of_week_pass'] = workers_df['restriction_days_of_week_boo'].apply(lambda x: 'True' if x is False else 'False')
    
#print("employer:", time_range_employer)
#print(workers_df[['restriction_time_pass', 'restrictions_time', 'time_start_unavailable', 'time_end_unavailable' ]] )


# ### job sought category match

# In[65]:




job_sought_recruiter=recruit_row['job_sought']
job_sought_recruiter=[x.strip("'") for x in job_sought_recruiter]
job_sought_recruiter=job_sought_recruiter[0]
#print("recruiter:", job_sought_recruiter)

for index, row in workers_df.iterrows():
    job_sought_worker=row['job_sought']
    job_sought_worker=[x.strip("'") for x in job_sought_worker]
    
    if job_sought_recruiter in job_sought_worker:
        update_row=True
    else:
        update_row=False
    #print(job_sought_worker, update_row)
    algorithm_df.loc[index, 'job_sought_category_pass'] = update_row
    
    


# ### Is industry experience required and how much experience?

# In[66]:


time_experience_numeric_dict={"Less than 1 year": 1, "1 to 2 years":2,"3 to 5 years":3, "6 to 10 years":4, "more than 11 years":5}


industry_experience_required=recruit_row['industry_experience_required']

experience_time_recruit=recruit_row['experience_time']
#experience_time_recruit=["2 to 5 years"]

experience_time_recruit=[x.strip("'") for x in experience_time_recruit]

experience_time_recruit=experience_time_recruit[0]
#print("recruiter", experience_time_recruit)
experience_time_recruit_recode=time_experience_numeric_dict.get(experience_time_recruit) 
#print("experience_time_recruit:", experience_time_recruit_recode)
industry_experience_required=str_to_bool(industry_experience_required)
#industry_experience_required=False
for index, row in workers_df.iterrows():
    experience_time_worker=""
    industry_experience_has=row['industry_experience_has']
    industry_experience_has=str_to_bool(industry_experience_has)
   
    if industry_experience_required:
        if industry_experience_has:
            ####put here the amount of time
            experience_time_worker=row['experience_time']
            experience_time_worker=[x.strip("'") for x in experience_time_worker]
            experience_time_worker=experience_time_worker[0]
            experience_time_worker_recode=time_experience_numeric_dict.get(experience_time_worker) 
            #print("experience_time_worker_recode", experience_time_worker_recode)
            #print("experience_time_recruit_recode", experience_time_recruit_recode, row['submission_id'])
            if experience_time_worker_recode >= experience_time_recruit_recode:
                
                #print("inner if loop means true", experience_time_worker)
                update_row=True
            else:
                 update_row=False
                #print("inner for loop means false", experience_time_worker)
        else:
            update_row=False
    else:
        update_row=True
    algorithm_df.loc[index, 'experience_required_pass'] = update_row
    
    #if industry_experience_required:
    #print(industry_experience_required, experience_time_recruit,  industry_experience_has, experience_time_worker,  update_row)
    #else:
        #print(industry_experience_required, industry_experience_has, experience_time_recruit, update_row)
##################################################


    


# ### Do you have training in the restaurant/hotel industry? 

# In[67]:



training_has_recruiter=recruit_row['training_has']
training_has_recruiter=str_to_bool(training_has_recruiter)
#print(training_has_recruiter)

for index, row in workers_df.iterrows():
    training_has_worker=row['training_has']
    training_has_worker=str_to_bool(training_has_worker)
    
    if training_has_recruiter is False:
        update_score=True
    elif training_has_recruiter == training_has_worker:
        update_score=True
    else:
        update_score=False
    #print(training_has_worker, training_has_recruiter, row['submission_id'], update_score)
    algorithm_df.loc[index, 'training_has_pass'] = update_score


# ### What type of workload is associated with the position?

# In[68]:


shift_type_recruiter=recruit_row['shift_type']
shift_type_recruiter=[x.strip("'") for x in shift_type_recruiter]
#print("shift_type_recruiter:", shift_type_recruiter)

for index, row in workers_df.iterrows():
    shift_type_worker=row['shift_type']
    shift_type_worker=[x.strip("'") for x in shift_type_worker]
    
    workload_overlap=set(shift_type_worker).intersection(shift_type_recruiter)
    workload_overlap_len=len(workload_overlap)
   
    if workload_overlap_len >0:
        update_row=True
    else:
        update_row=False
    #print(shift_type_worker, shift_type_recruiter, workload_overlap, workload_overlap_len, update_row, row['submission_id'])    
    algorithm_df.loc[index, 'workload_pass'] = update_row



# ### Is it necessary for the applicant to have all the documents? ( Fiscal Identification number, Social Security number and Residence Visa)
# #### You may want to update the typeform to offer a "no" option, which would be illegal employment.

# In[70]:



residency_scale_dict={'Portgual':4, 'Residence Permit/Residency Visa':4, 'EU Citizenship':4, 'Social Security number':3, 'Fiscal Identification Number':2, 'None of the above':1}    



document_type_recruit=recruit_row['document_type']
#print("recruit", document_type_recruit)
document_type_recruit=[x.strip("'") for x in document_type_recruit]
#print("recruit", document_type_recruit)
#document_type_recruit=document_type_recruit[0]
#print("recruit", document_type_recruit)
#document_type_recruit='Only the Fiscal Identification number and Social Security number'
#document_type_recruit_recode=residency_scale_dict.get(document_type_recruit)
document_type_recruit_recode = [residency_scale_dict.get(x)  for x in document_type_recruit]
document_type_recruit_recode=min(document_type_recruit_recode) 

#print("recruit", document_type_recruit, document_type_recruit_recode)



for index, row in workers_df.iterrows():
        
        nationality=row['nationality']
        nationality=nationality
        has_ss_number=row['has_ss_number']
        
        has_ss_number=[x.strip("'") for x in has_ss_number]
        #print(has_ss_number)
        has_ss_number[:] = [residency_scale_dict.get(x)  for x in has_ss_number]
        if nationality=='Portugal':
            #print("portuguese")
            has_ss_number=[4]
            
        else:
            pass
        #print("my list", has_ss_number, row['has_ss_number'], row['submission_id'])
        has_ss_number_max=max(has_ss_number)
        #print(row['submission_id'], has_ss_number_max)
        
            
        #print(" hi has_ss_number_max:", has_ss_number_max)
        
        #for x in has_ss_number:
            #has_ss_number_recode=residency_scale_dict.get(has_ss_number) 
        
        
        
        #has_ss_number=has_ss_number[0]
        #has_ss_number_recode=residency_scale_dict.get(has_ss_number) 
        #print("\n", has_ss_number, has_ss_number_recode, nationality)
        #print("has_ss_number_max", has_ss_number_max, " ", row['submission_id'])
        #print("document_type_recruit_recode", document_type_recruit_recode, document_type_recruit)
        if has_ss_number_max >= document_type_recruit_recode:
            update_row=True
        else:
            update_row=False
        algorithm_df.loc[index, 'document_type_pass'] = update_row
        #print("\n", has_ss_number, has_ss_number_max, nationality, "update_row:", update_row)
        


# ## Nice to have qualities

# ### Type of experience the worker has

# In[71]:



job_sought_recruiter=recruit_row['job_sought']
job_sought_recruiter=[x.strip("'") for x in job_sought_recruiter]
job_sought_recruiter=job_sought_recruiter[0]
#job_sought_recruiter='Counter Service'
#print("recruiter:", job_sought_recruiter)
#industry_experience_required=False
for index, row in workers_df.iterrows():
    industry_experience_type_worker=row['industry_experience_type']
    industry_experience_type_worker=[x.strip("'") for x in industry_experience_type_worker]
    ##since Chef is similar but superior to cook, if they say they have been a chef we will go ahead and say they have also been a cook
    if 'Chef' in industry_experience_type_worker:
    	industry_experience_type_worker.append('Cook')
    	industry_experience_type_worker.append('Kitchen Helper')
    ###likewise cook is similar but superior to kitchen helper so we do the same for that	    
    elif 'Cook' in industry_experience_type_worker:
        industry_experience_type_worker.append('Kitchen Helper')
    else:
    	pass	
    #if job_sought_recruiter in industry_experience_type_worker:
     #   update_row=True
    #else:
     #   update_row=False
    
    if job_sought_recruiter in industry_experience_type_worker:
        update_score=1
    else:
    #print("not done. Also add half a point for people with update_score=0 if they don't require experience")
        update_score=0
    
    if industry_experience_required is False and update_score==0:
        update_score=0.5
    else:
        pass
    
    algorithm_df.loc[index, 'job_experience_match_score'] = update_score
    #print(industry_experience_type_worker, job_sought_recruiter, industry_experience_required, update_score)
    
    #workers_df.loc[index, 'industry_experience_type_score'] = update_row
    
    


# ### What languages should the candidate be able to speak? 

# In[72]:


languages_spoken_recruit=recruit_row['languages_spoken']

languages_spoken_recruit=[x.strip("'") for x in languages_spoken_recruit]
#languages_spoken_recruit=['Portuguese', 'French', 'English']
#print("recruit", languages_spoken_recruit)
for index, row in workers_df.iterrows():
    languages_spoken_worker=row['languages_spoken']
    languages_spoken_worker=[x.strip("'") for x in languages_spoken_worker]
    languages_spoken_overlap=set(languages_spoken_recruit).intersection(languages_spoken_worker)
    languages_spoken_overlap_pct=len(languages_spoken_overlap)/len(languages_spoken_recruit)
   
    algorithm_df.loc[index, 'languages_spoken_overlap_pct'] = languages_spoken_overlap_pct
    #print("worker:",languages_spoken_worker, "intersect:", languages_spoken_overlap, languages_spoken_overlap_pct, update_row )


# In[73]:


educational_attainment_decode_dict={'Primary school':1, 'Middle School':2, 'Secondary school':3,'University Degree or above':4,'Technical course in the area':5, 'University Degree in the area':6,"Master's or Doctorate":7, "Masters or Doctorate":7,"Does not matter":8}

educational_attainment_recruit=recruit_row['educational_attainment']
#print("1",educational_attainment_recruit)

educational_attainment_recruit=[x.strip("'") for x in educational_attainment_recruit]
educational_attainment_recruit=educational_attainment_recruit[0]
#educational_attainment_recruit="University Degree in the area"
#print("2",educational_attainment_recruit)
educational_attainment_recruit_recode=educational_attainment_decode_dict.get(educational_attainment_recruit) 
#print("recruit", educational_attainment_recruit, educational_attainment_recruit_recode)


#Does not matter


for index, row in workers_df.iterrows():
    educational_attainment_worker=row['educational_attainment']
    #print("10", educational_attainment_worker)
    educational_attainment_worker=[x.strip("'") for x in educational_attainment_worker]
    #print("11", educational_attainment_worker)
    educational_attainment_worker=educational_attainment_worker[0]
    submission_id=row['submission_id']
    if educational_attainment_worker == "Masters or Doctorate":
       educational_attainment_worker="Master's or Doctorate"
    else:
        #print("I passed")
        pass
    #print("12", educational_attainment_worker)
    #print("educational_attainment_recruit", educational_attainment_recruit)
   
    educational_attainment_worker_recode=educational_attainment_decode_dict.get(educational_attainment_worker)
    #print("educational_attainment_worker_recode", educational_attainment_worker_recode, educational_attainment_recruit , educational_attainment_recruit_recode , " ", submission_id)
    if educational_attainment_recruit_recode is 8: 
    	education_score=1
    elif  educational_attainment_worker_recode>=educational_attainment_recruit_recode:
        education_score=1
    else:
        education_score=educational_attainment_worker_recode / educational_attainment_recruit_recode
    algorithm_df.loc[index, 'education_score'] = education_score   
    #print(educational_attainment_worker, educational_attainment_worker_recode, education_score)


# In[74]:


personal_traits_one_recruit=recruit_row['personal_traits_one']
personal_traits_one_recruit=[x.strip("'") for x in personal_traits_one_recruit]
personal_traits_one_recruit=personal_traits_one_recruit[0]

#print("recruit", personal_traits_one_recruit)


personal_traits_two_recruit=recruit_row['personal_traits_two']
personal_traits_two_recruit=[x.strip("'") for x in personal_traits_two_recruit]

#print("recruit", personal_traits_two_recruit)

personality_type_recruit=recruit_row['personality_type']
personality_type_recruit=[x.strip("'") for x in personality_type_recruit]
personality_type_recruit=personality_type_recruit[0]

#print("recruit", personality_type_recruit)
for index, row in workers_df.iterrows():

    #score for first question
    personal_traits_one_worker=row['personal_traits_one']
    personal_traits_one_worker=[x.strip("'") for x in personal_traits_one_worker]
    personal_traits_one_worker=personal_traits_one_worker[0]
    
    if personal_traits_one_recruit == personal_traits_one_worker:
        personal_traits_one_score = 1
    else: 
        personal_traits_one_score = 0
    #print(personal_traits_one_worker, personal_traits_one_score)
    ###score for second question
    personal_traits_two_worker=row['personal_traits_two']
    personal_traits_two_worker=[x.strip("'") for x in personal_traits_two_worker]
    personal_traits_two_overlap=set(personal_traits_two_recruit).intersection(personal_traits_two_worker)
    personal_traits_two_score=len(personal_traits_two_overlap) / 2
    #print(personal_traits_two_worker, personal_traits_two_score )
    
    
    
    ###score for third question
    personality_type_worker=row['personality_type']
    personality_type_worker=[x.strip("'") for x in personality_type_worker]
    personality_type_worker=personality_type_worker[0]
    
    if personality_type_worker == personality_type_recruit:
        personality_type_score=1
    else:
        personality_type_score=0
        
    #print( personality_type_worker, personality_type_score )
    personality_score_total=(personality_type_score + personal_traits_two_score + personal_traits_one_score) / 3
    #print( personality_score_total)
    algorithm_df.loc[index, 'personality_score'] = personality_score_total
    

# ### What contract do you prefer? (Note, this should be updated once Rafael translates Jordan's changes to Portuguese

# In[69]:


contract_type_recruit=recruit_row['contract_type']
contract_type_recruit=[x.strip("'") for x in contract_type_recruit]

#contract_type_recruit=['1-3 months']
#print("recruit", contract_type_recruit)



for index, row in workers_df.iterrows():
    contract_type_worker=row['contract_type']
    contract_type_worker=[x.strip("'") for x in contract_type_worker]
    contract_type_overlap=set( contract_type_worker).intersection(contract_type_recruit)
    contract_type_overlap_len=len(contract_type_overlap)
    if 'Any of these' in contract_type_worker or 'Indefinite' in contract_type_recruit:
        any_job_flag=True
    else:
        any_job_flag=False
    if contract_type_overlap_len> 0 or any_job_flag is True:
        update_row=True
    else:
        update_row=False
    algorithm_df.loc[index, 'contract_type_pass'] = update_row
    #print(row['submission_id'], contract_type_recruit, contract_type_worker, contract_type_overlap, any_job_flag, update_row)
    


# ## Interview categories

# In[75]:


#algorithm_type=2
if algorithm_type==2:
    
    

    numeric_columns=['punctuality_and_attendance', 'appearance_and_hygiene', 'communication_skills', 'knowledge_evaluation', 'reliability', 'ambition', 'plans_to_stay_in_the_area', 'hardworking_and_proactive', 'articulateness', 'aptitude', 'casual_restaurant_fit', 'fine_dining_fit', 'tourist_friendly_fit','portuguese_restaurant_fit','trendy_restaurant_fit', 'retention_likelihood', 'red_flags_scale']
    #need this deep copy because if not it messes 
    numeric_columns_iterate=copy.deepcopy(numeric_columns)
    ###this code drops variables from the list of they were not included in the dataframe
    for var in numeric_columns_iterate:
        if var  in rankings_columns:
            pass
        else:
            numeric_columns.remove(var)
    
    #numeric_columns=['punctuality_and_attendance', 'appearance_and_hygiene', 'communication_skills', 'knowledge_evaluation', 'reliability', 'ambition', 'plans_to_stay_in_the_area', 'hardworking_and_proactive', 'articulateness', 'aptitude', 'casual_restaurant_fit', 'fine_dining_fit', 'tourist_friendly_fit','trendy_restaurant_fit', 'retention_likelihood', 'red_flags_scale']


    numeric_columns_with_adjust=copy.deepcopy(numeric_columns)
    numeric_columns_with_adjust.remove('red_flags_scale')
    numeric_columns_with_adjust.append('red_flags_scale_adjusted')
    #print(numeric_columns_with_adjust)
    #print(len(numeric_columns))
    ###turn all rankings into numeric
    for x in numeric_columns:
        ranking_df[x] = ranking_df[x].apply(pd.to_numeric, errors='coerce')


    #count the variables
    ranking_df['non_null_columns'] = ranking_df[numeric_columns].count(axis=1)
    #need to have this before the loop so it works
    #ranking_df['red_flags_scale_adjusted'] = 0
    for index, row in ranking_df.iterrows():
        non_nulls=row['non_null_columns']

        ##for the red flag ratings we want to make it a negative number since if they have lots of red flags we want to 
        ##be less likely to recommend them for a job. We also want to weight this equivalent to 1/3 of the other categories. 
        red_flags_magnitude=(math.floor((non_nulls-1)/3)*-1)
        red_flags_scale=row['red_flags_scale']
        red_flags_scale_adjusted=red_flags_scale * red_flags_magnitude
        ranking_df.loc[index, 'red_flags_scale_adjusted'] = red_flags_scale_adjusted  
        max_score=((non_nulls-1)*5)+(red_flags_magnitude)
        total_score_interviews=0
        #print(row['red_flags_scale_adjusted'], red_flags_scale_adjusted  )
        for value in numeric_columns_with_adjust:
            try:
                myvalue=row[value]
            except:
                myvalue=red_flags_scale_adjusted 
            #print(myvalue)
            total_score_interviews=total_score_interviews + myvalue
        total_score_interviews=(total_score_interviews/max_score) * 6
        total_score_interviews=round(total_score_interviews, 2)

        #print(red_flags_magnitude, red_flags_scale, red_flags_scale_adjusted, non_nulls, total_score_interviews, max_score)
        ranking_df.loc[index, 'total_score_interviews'] = total_score_interviews 


    ranking_df_final = ranking_df[['total_score_interviews']]
   
else:
    pass


# In[76]:



##this is a scratch pad
numeric_columns=['punctuality_and_attendance', 'appearance_and_hygiene', 'communication_skills', 'knowledge_evaluation', 'reliability', 'ambition', 'plans_to_stay_in_the_area', 'hardworking_and_proactive', 'articulateness', 'aptitude', 'casual_restaurant_fit', 'fine_dining_fit', 'tourist_friendly_fit','portuguese_restaurant_fit','trendy_restaurant_fit', 'retention_likelihood', 'red_flags_scale']
numeric_columns_iterate=copy.deepcopy(numeric_columns)    
    ###this code drops variables from the list of they were not included in the dataframe
#i=0
#for var in numeric_columns_iterate:
 #   if var  in rankings_columns:
        #print("yes",i)
  #  else:
   #     numeric_columns.remove(var)
        #print("no", i)
    #i=i+1
#indices=[11,12,13,14]
#result_list = [numeric_columns_iterate[i] for i in indices]
#print(result_list)
#print(rankings_columns)
#print(numeric_columns)


# # testing

# In[77]:





#print(rankings_columns)


# In[78]:


#pass_list=[restriction_days_of_week_pass, restriction_time_pass, job_sought_category_pass, experience_required_pass, training_has_pass, workload_pass,  document_type_pass  ]
for index, row in algorithm_df.iterrows():
    restriction_days_of_week_pass=row['restriction_days_of_week_pass']
    restriction_time_pass=row['restriction_time_pass']
    job_sought_category_pass=row['job_sought_category_pass']
    experience_required_pass=row['experience_required_pass']
    training_has_pass=row['training_has_pass']
    workload_pass=row['workload_pass']
    document_type_pass=row['document_type_pass']
    contract_type_pass=row['contract_type_pass']
    #print(contract_type_pass, row['submission_id'])
    pass_list=[restriction_days_of_week_pass, restriction_time_pass, job_sought_category_pass, experience_required_pass, training_has_pass, workload_pass,  document_type_pass, contract_type_pass  ]
    #print(restriction_days_of_week_pass, restriction_time_pass, job_sought_category_pass, experience_required_pass,  training_has_pass, workload_pass, document_type_pass  )
    all_pass=True
    for var in pass_list:
        #print(var)
        if var is False:
            all_pass=False
            #print("var is false")
        else:
            pass
            #print("var is true")
    algorithm_df.loc[index, 'all_pass'] = all_pass
#algorithm_df.to_excel('full_set.xlsx', index=False) 
 
###
sum_list=['job_experience_match_score', 'languages_spoken_overlap_pct', 'education_score', 'personality_score']
##A perfect score is 4
algorithm_df['total_score_pre_interview'] = algorithm_df[sum_list].sum(axis=1)
 
#algorithm_df.loc[algorithm_df['all_pass'] == True]
total_score_pre_interview = algorithm_df[['total_score_pre_interview']].copy()
workers_df_filtered=workers_df.loc[algorithm_df['all_pass'] == True]
 
 
merged_workers=pd.merge(workers_df_filtered, total_score_pre_interview, left_index=True, right_index=True )
    

merged_workers.sort_values("total_score_pre_interview", inplace=True, ascending=False) 
#merged_workers 

if algorithm_type==1:
    my_excel='pre_interview_' + restaurant_name + '_' + recruit_input + '.xlsx'
    merged_workers= merged_workers.drop(['phone_interview_date',
                                         'live_interview_date',
                                         'punctuality_and_attendance',
                                         'appearance_and_hygiene',
                                         'communication_skills',
                                         'knowledge_evaluation',
                                         'reliability',
                                         'ambition',
                                         'plans_to_stay_in_the_area',
                                         'hardworking_and_proactive',
                                         'articulateness',
                                         'aptitude',
                                         'casual_restaurant_fit',
                                         'fine_dining_fit',
                                         'tourist_friendly_fit',
                                         'portuguese_restaurant_fit',
                                         'trendy_restaurant_fit',
                                         'retention_likelihood',
                                         'red_flags_scale',
                                         ], axis=1)  
                    

    #merged_workers.to_excel(my_excel, index=False)
        # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(my_excel, engine='xlsxwriter')

    ##recruit to dataframe
    recruit_frame=recruit_row.to_frame()
    recruit_frame['index1'] = recruit_frame.index
    recruit_frame.columns = ['data', 'variable']
    recruit_frame = recruit_frame[['variable', 'data']]
    # Write each dataframe to a different worksheet.
    merged_workers.to_excel(writer, sheet_name='candidates', index=False)
    recruit_frame.to_excel(writer, sheet_name='recruitment_request', index=False)

    #Indicate workbook and worksheet for formatting
    workbook = writer.book
    worksheet = writer.sheets['candidates']

    #Iterate through each column and set the width == the max length in that column. A padding length of 2 is also added.
    for i, col in enumerate(merged_workers.columns):
        # find length of column i
        column_len = merged_workers[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) 
        # set the column length
        worksheet.set_column(i, i, column_len)

    worksheet = writer.sheets['recruitment_request']
    #print(type(recruit_frame))
    #Iterate through each column and set the width == the max length in that column. A padding length of 2 is also added.
    for i, col in enumerate(recruit_frame.columns):
        # find length of column i
        column_len = recruit_frame[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) 
        # set the column length
        worksheet.set_column(i, i, column_len)


    # Close the Pandas Excel writer and output the Excel file.
    writer.save()

#merged_workers.to_excel("test_data_frame.xlsx", index=False)
else:
    pass


# In[80]:



if algorithm_type==2:
    merged_workers1=pd.merge(merged_workers, ranking_df_final, left_index=True, right_index=True )
    #merged_workers1.drop(merged_workers1.columns[['submission_id_y']], axis=1, inplace=True)
    merged_workers1['final_score_combined'] = merged_workers1[['total_score_pre_interview','total_score_interviews']].sum(axis=1) 
    merged_workers1.sort_values("final_score_combined", inplace=True, ascending=False) 
    merged_workers1=merged_workers1.drop(ranking_columns_to_remove, axis=1)
    my_excel='post_interview_'  + restaurant_name + '_' + recruit_input + '.xlsx'
    #merged_workers.to_excel(my_excel, index=False)
        # Create a Pandas Excel writer using XlsxWriter as the engine.
    writer = pd.ExcelWriter(my_excel, engine='xlsxwriter')

    ##recruit to dataframe
    recruit_frame=recruit_row.to_frame()
    recruit_frame['index1'] = recruit_frame.index
    recruit_frame.columns = ['data', 'variable']
    recruit_frame = recruit_frame[['variable', 'data']]
    # Write each dataframe to a different worksheet.
    merged_workers1.to_excel(writer, sheet_name='candidates', index=False)
    recruit_frame.to_excel(writer, sheet_name='recruitment_request', index=False)

    #Indicate workbook and worksheet for formatting
    workbook = writer.book
    worksheet = writer.sheets['candidates']

    #Iterate through each column and set the width == the max length in that column. A padding length of 2 is also added.
    for i, col in enumerate(merged_workers1.columns):
        # find length of column i
        column_len = merged_workers1[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) 
        # set the column length
        worksheet.set_column(i, i, column_len)

    worksheet = writer.sheets['recruitment_request']
    #print(type(recruit_frame))
    #Iterate through each column and set the width == the max length in that column. A padding length of 2 is also added.
    for i, col in enumerate(recruit_frame.columns):
        # find length of column i
        column_len = recruit_frame[col].astype(str).str.len().max()
        # Setting the length if the column header is larger
        # than the max column value length
        column_len = max(column_len, len(col)) 
        # set the column length
        worksheet.set_column(i, i, column_len)


    # Close the Pandas Excel writer and output the Excel file.
    writer.save()
    
    #merged_workers1.to_excel(my_csv)
else:
    pass

print("script done")
print("Time taken", datetime.now() - startTime)
