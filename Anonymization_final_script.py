#!/usr/bin/env python
# coding: utf-8

# 1. This is the anonymization script in python.
# 2. It is tested on Mac only. Not tested on Windows and linux machines.
# 3. Needs python 3 to run.
# 4. Users need to download facebook information in the json format.
# 5. There are only three buttons. 
#     (i) to specify the path of the directory where facebook info is saved, 
#     (ii) generates the output files  in a directory called 'outputDir' within the facebook info directory.
#     (iii) to quit the application
# 6. outputDir contains all relevant files in .csv format. 
# 7. Files with names starting 'temp_' are temporary files generated. Please ignore them.
# 8. The original convertion between username to unique codes file is named as  'master_file_for_info_all_friends.csv'
# 9. Unknown user is represented with id u_999999999.
# 10. my_id  (self id) is always u_100000001.
# 11. Groups are identified by id numbers (example, 'g_10'). They are stored in groups.csv
# 

# In[1]:


import numpy as np
import pandas as pd
import json
from pprint import pprint
from pandas.io.json import json_normalize #package for flattening json to pandas df
import os
import datetime
import time
import re

from unidecode import unidecode
import string
printable = set(string.printable)

my_id = 'u_1000000001'
my_name = '' 
unknown_friend_id = 'u_999999999'
unknown_friend_name = 'unknown' # not really necessary
encoding_code ='utf-8'
char_avoid = ['!','@','£','$','%','^','&','*','(',')','€','#','§','±','=','+']


# In[2]:


#utility functions
def print_full(x):
    # This is a support function. To test and check the output.
    pd.set_option('display.max_rows', len(x))
    #print(x)
    #pd.set_option('display.max_colwidth', -1)
    pd.reset_option('display.max_rows')
    
def unidecode_user_names(name):
    # This function is a support function - accepts a string and unidecodes it    
    name1 =  ''.join(filter(lambda ch: ch not in char_avoid, name))
    name1 =  unidecode(name1)
    return name1

def get_the_user_name(words, j, stop_word):
    # This is a support function to extract user's name from message
    # here stop_word is the word that follows immediately the user's name   
    some_body = ''            
    while ( words[j] != stop_word):
        some_body = some_body +words[j]+' '
        j += 1
    some_body = some_body[:-3]
    some_body = unidecode_user_names(some_body)   
    return some_body

def get_file(folder, file_to_search):
    if (file_to_search == 'posts'):
        choice = r'your_posts*[.json]'
    elif (file_to_search == 'messages'):
        choice = r'message*[.json]'
        
    files = []
    for (_,_, filenames) in os.walk(folder):
        
        files.extend(filenames)
        break
    
    for file in files:
        if( re.match(choice, file)):
            #print(file)
            posts_file = file
            break
    return '/'+file


# # create friend.csv -- task 1

# In[3]:


# These functions are used to create friend.csv file

def join_friends_names(names):
    # This function joins user names (after converting them to lowercase and remove spaces)
    #supports messages folder as messages from individual friends are arranged with names.
    
    joined_names = pd.Series()
    for i in range(0,len(names)):
        name = names[i].lower()
        name = name.replace(" ","")
        joined_names.at[i] = name
    return joined_names

def set_my_name(directoryPath):
    try:
        profile_file = directoryPath+'/profile_information/profile_information.json'
        with open(profile_file) as f:
            prof_data = json.load(f)

        prof_df = json_normalize(prof_data['profile'])
        my_name = str(prof_df['name.full_name'][0] )+''
    except:
        my_name = ''
    return my_name
    
    
def read_friends_data_folder(directoryPath):
    # This function gathers all friends data from friends folder and generates a dataframe
    
    friends_folder = directoryPath+'/friends'
    friends_df = None
    new_df = None
    if (os.path.isdir(friends_folder)):
        
        if (os.path.isfile(friends_folder +'/friends.json')):
            with open(friends_folder +'/friends.json') as f:
                friends_data = json.load(f)
            friends_df = json_normalize(friends_data['friends'])
            if ('contact_info' in friends_df.columns):
                friends_df = friends_df.drop('contact_info',1)
            friends_df['status'] = 'current friend'
            friends_df['type_of_activity'] = 1
    
        #received_requests
        if (os.path.isfile(friends_folder +'/received_friend_requests.json')):
            with open(friends_folder+'/received_friend_requests.json') as f:
                received_requests_data = json.load(f)

            received_requests_df = json_normalize(received_requests_data['received_requests'])
            received_requests_df['status'] = 'received request'
            received_requests_df['type_of_activity'] = 3

            #new_df  = friends_df.append(received_requests_df,ignore_index=True)
            friends_df  = friends_df.append(received_requests_df,ignore_index=True)

        # rejected requests
        if (os.path.isfile(friends_folder +'/rejected_friend_requests.json')):
            with open(friends_folder+'/rejected_friend_requests.json') as f:
                rejected_requests_data = json.load(f)

            rejected_requests_df = json_normalize(rejected_requests_data['rejected_requests'])
            if ('marked_as_spam' in rejected_requests_df.columns):
                rejected_requests_df = rejected_requests_df.drop('marked_as_spam',1)
            rejected_requests_df['status'] = 'rejected request'
            rejected_requests_df['type_of_activity'] = 4

            #new_df = new_df.append(rejected_requests_df,ignore_index=True)
            friends_df  = friends_df.append(rejected_requests_df,ignore_index=True)
            
        # removed friends
        if (os.path.isfile(friends_folder +'/removed_friends.json')):
            with open(friends_folder+'/removed_friends.json') as f:
                removed_friends_data = json.load(f)

            removed_friends_df = json_normalize(removed_friends_data['deleted_friends'])
            removed_friends_df['status'] = 'deleted friend'
            removed_friends_df['type_of_activity'] = 0

            #new_df = new_df.append(removed_friends_df,ignore_index=True)
            friends_df  = friends_df.append(removed_friends_df,ignore_index=True)

        #sent requests
        if (os.path.isfile(friends_folder +'/sent_friend_requests.json')):
            with open(friends_folder+'/sent_friend_requests.json') as f:
                sent_friend_requests_data = json.load(f)

            sent_friend_requests_df = json_normalize(sent_friend_requests_data['sent_requests'])
            sent_friend_requests_df['status'] ='sent request'
            sent_friend_requests_df['type_of_activity'] = 2

            #new_df = new_df.append(sent_friend_requests_df,ignore_index=True)
            friends_df  = friends_df.append(sent_friend_requests_df,ignore_index=True)

        #new_df['name'] = new_df['name'].apply(lambda x: ''.join(filter(lambda z: z not in char_avoid, x)))
        friends_df['name']  = friends_df['name'].apply(lambda x: ''.join(filter(lambda z: z not in char_avoid, x)))
        
        #new_df['name'] = new_df['name'].apply(lambda x: unidecode(x))
        friends_df['name']  = friends_df['name'].apply(lambda x: unidecode(x))
        
        #new_df['joined_names'] =  join_friends_names(new_df['name'])
        friends_df['joined_names']  = join_friends_names(friends_df['name'])
    
    #return new_df
    if (friends_df is None):
        friends_df = pd.DataFrame(columns=['name','timestamp','status','type_of_activity','joined_names'])
    return friends_df

def make_unique_identifier(df):
    
    # This function calculates a unique id from the details and assigns this id to each friend
    combined_id = pd.Series()
    
    for i in range(0,len(df)):
        
        # to create a numeric value from name 
        input = df['name'][i].lower()
        output = 0
        for character in input:
            number = ord(character) - 96 
            output = output+number
        
        # to create a numeric value from name 
        input = df['status'][i].lower()
        for character in input:
            number = ord(character) - 96
            output = output+number
        
        output = 'u_'+str(output+df['timestamp'][i])
        
        combined_id.at[i] = output
    return combined_id

def generate_friend_csv(directoryPath, outputPath):
       
    #this function generates two files :
    #(i) All data (to be saved on the user's own machine - outputDir/master_file_for_info_all_friends.csv), 
    #(ii) data with name anonymized --friend.csv
    
    friends_folder = directoryPath+'/friends'
    dataFiles_df = read_friends_data_folder(directoryPath)
    #print(dataFiles_df.head(2))
    dataFiles_df['from_id'] = my_id 
    dataFiles_df['to_id'] = make_unique_identifier(dataFiles_df[['name','timestamp','status']])
    dataFiles_df['timestamp_date'] = dataFiles_df['timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x).isoformat() )
    dataFiles_df['unique_id'] = dataFiles_df['to_id'] 
    # Even though 'unique_id' is same as 'to_id', I have made a separate column for backup and clarity
    
    if not os.path.exists(outputPath):
        os.makedirs(outputPath)

    dataFiles_df.to_csv(outputPath+'/master_file_for_info_all_friends.csv', sep=',')# no need for 'encoding' parameter as we have already done that

    friends_uniqueid = pd.DataFrame(dataFiles_df[['from_id','to_id', 'type_of_activity']].copy())
    friends_uniqueid.to_csv(outputPath+'/friend.csv', sep=',') 


#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'

read_friends_data_folder(directoryPath)
generate_friend_csv(directoryPath, outputPath)


# ** Group info ** (creation of groups.csv in the format groups(user_id, list_friends, group_id))

# In[4]:


def create_group_information(directoryPath, outputPath):
    # this function creates group.csv 
    
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    groups_folder = directoryPath+'/groups'
    
    my_groups = pd.DataFrame(columns=['user_id','group_id','admin/member','group_name','joined_date'])
    if (os.path.isdir(groups_folder)):
        # group memberships 
        
        user_id = my_id
        group_id =0
        
        if (os.path.isfile(groups_folder+'/your_group_membership_activity.json') ):
            with open(groups_folder+'/your_group_membership_activity.json' ) as f:
                my_group_memberships = json.load(f)


            my_group_memberships_df = json_normalize(my_group_memberships['groups_joined'], record_path=['attachments','data'],
                                                     meta = ['timestamp','title'])
            for i in range(0, len(my_group_memberships_df)) :
                admin_member = 'member'

                group_name = my_group_memberships_df.loc[i]['name']
                joined_date = datetime.datetime.fromtimestamp(my_group_memberships_df.loc[i]['timestamp']).isoformat()

                my_groups.loc[group_id] = [user_id,('g_'+str(group_id)),admin_member,group_name,joined_date]
                group_id += 1

        # add groups_admined
        if (os.path.isfile(groups_folder+'/your_groups.json') ):
            with open(groups_folder+'/your_groups.json') as f:
                your_groups = json.load(f)

            your_groups_df = json_normalize(your_groups['groups_admined'])  

            for i in range(0, len(your_groups_df)):
                admin_member = 'admin'
                group_name = your_groups_df.loc[i]['name']

                joined_date = datetime.datetime.fromtimestamp(your_groups_df.loc[i]['timestamp']).isoformat()

                my_groups.loc[group_id] = [user_id,('g_'+str(group_id)),admin_member,group_name,joined_date]
                group_id += 1
    
    #print(my_groups.tail(10))
    my_groups.to_csv(outputPath+'/groups.csv', sep=',')
'''    
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
create_group_information(directoryPath, outputPath)
'''


# In[5]:


def group_posts_comments(directoryPath, outputPath):
    
    # This function is to create a group_comments.csv file
    # The 'your_posts_and_comments_in_groups.json' file has sparse values, which makes things very difficult normalize
    # First, I am adding all single comments 
    # and then, adding comments from attachments
    
    my_name= set_my_name(directoryPath)
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    groups_folder = directoryPath+'/groups'
    
    
    groups_df = pd.read_csv(outputPath+'/groups.csv')
    groups_posts = None # to check whether it is existing
    
    '''
    json_normalize() is not handling NAN values in the nested columns properly and causing 'key error'.
    So, I tried this work-around below.
    step 1: I read the json file using json.load(file_handle)
    step 2: convert to pandas data frame
    step 3: remove NAN values
    step 4: Keep only the columns you need
    step 5: convert the dataframe back to json
    step 6: json_normalise() the new json file
    '''
    if (os.path.isfile(groups_folder+'/your_posts_and_comments_in_groups.json') ):    
        with open(groups_folder+'/your_posts_and_comments_in_groups.json') as f:
            group_comments_posts = json.load(f) # step 1
        #pprint(group_comments_posts)


        groups_comments_posts_df = json_normalize(data = group_comments_posts['group_posts']) # step 2
        if ('activity_log_data' in groups_comments_posts_df.columns):
            groups_comments_posts_df = json_normalize(data = group_comments_posts['group_posts']['activity_log_data'])
            
        groups_comments_posts_df = groups_comments_posts_df.dropna(subset=['data','timestamp','title'],how='any') #step 3
        groups_comments_posts_df = groups_comments_posts_df[['data','timestamp','title']].copy() # step 4
        #print(groups_comments_posts_df.head(10))
        
        groups_comments_posts_json = groups_comments_posts_df.to_json(outputPath+'/temp_your_group_posts.json',
                                                                      orient='records') #step 5
    
        if (os.path.isfile(outputPath+'/temp_your_group_posts.json') ):    
            with open(outputPath+'/temp_your_group_posts.json') as f:
                group_comments_posts = json.load(f)

            groups_comments_posts_df = json_normalize(group_comments_posts,'data',['timestamp','comment'], 
                                                      record_prefix = '*',errors='ignore', meta_prefix='_' )#step 6

            #print(groups_comments_posts_df.columns)
            #print(groups_comments_posts_df['*comment'][0]) #we need info from this only
            groups_posts = pd.DataFrame(columns=['user_id','comment','length_of_comment','timestamp',
                                                 'timestamp_date','group_id','group_name'])
    
        for i in range(0, len(groups_comments_posts_df)):
            try:
                comment = groups_comments_posts_df['*comment'][i]    
                author = unidecode_user_names(comment['author'])
                #print(author)
                try:
                    user_id = my_id if (author == my_name) else (friends_df.loc[comment['author']==friends_df['name'],
                                                                                'unique_id'].values[0])
                except:
                    user_id = unknown_friend_id # an unknown user from a public group
                #print(user_id)
                comment_post = comment['comment']
                length = len(comment_post)
                timestamp = comment['timestamp']
                timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()
                group_name = comment['group']
                group_id = groups_df[groups_df['group_name'] == group_name]['group_id'].values[0]

                groups_posts.loc[i] = [user_id,comment_post, length, timestamp, timestamp_date,group_id,group_name]
                #print(groups_posts.loc[i])
            except:
                continue
    
    #Now add attachments comments
    if (os.path.isfile(groups_folder+'/your_posts_and_comments_in_groups.json') ):
        with open(groups_folder+'/your_posts_and_comments_in_groups.json') as f:
            group_comments_posts1 = json.load(f) 
        #pprint(group_comments_posts)
        groups_comments_posts1_df = json_normalize(data = group_comments_posts1['group_posts']) 
        if ('activity_log_data' in groups_comments_posts1_df.columns):
            groups_comments_posts1_df = json_normalize(data = group_comments_posts1['group_posts']['activity_log_data'])
        #print()   
        groups_comments_posts1_df = groups_comments_posts1_df.dropna(subset=['attachments']).reset_index(drop=True) 
        groups_comments_posts1_df = groups_comments_posts1_df[['attachments']].copy() 

        groups_comments_posts_json = groups_comments_posts1_df.to_json(outputPath+'/temp_your_group_posts1.json',
                                                                       orient='records') #step 5

        with open(outputPath+'/temp_your_group_posts1.json') as f:
            group_comments_posts1 = json.load(f)

        groups_comments_posts1_df = json_normalize(group_comments_posts1,['attachments','data'])
        groups_comments_posts_media_df = groups_comments_posts1_df['media'].to_frame().dropna().reset_index(drop=True)

        counter = len(groups_posts)
        for i in range(0,len(groups_comments_posts_media_df)):
            try:
                row = groups_comments_posts_media_df['media'][i]['comments']

                for item in row:
                    for key in item:
                        if (key == 'comment'):
                            comment_post = item[key]
                            length = len(comment_post)
                        elif (key == 'timestamp'):
                            timestamp = item['timestamp']
                            timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()
                        elif (key == 'author'):
                            author = item['author']
                            author = unidecode_user_names(author)
                            try:
                                user_id = my_id if (author == my_name) else (friends_df.loc[author==friends_df['name'],
                                                                                            'unique_id'].values[0])
                            except:
                                user_id = unknown_friend_id # an unknown user from a public group
                        elif (key == 'group'):
                            group_name = item['group']
                            group_id = groups_df[groups_df['group_name'] == group_name]['group_id'].values[0]

                    groups_posts.loc[counter] = [user_id,comment_post, length, timestamp, timestamp_date,
                                                 group_id,group_name]
                    counter += 1


            except:
                continue
    
    
    if (groups_posts is not None):
        groups_posts.to_csv(outputPath+'/groups_comments.csv',sep=',')
    else:
        groups_posts = pd.DataFrame(columns=['user_id','comment','length_of_comment','timestamp',
                                                 'timestamp_date','group_id','group_name'])
        groups_posts.to_csv(outputPath+'/groups_comments.csv',sep=',')
'''
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
group_posts_comments(directoryPath,outputPath)
'''


# # create message.csv - task 2

# In[6]:


def read_messages_inbox(directoryPath, outputPath):
   
    #this function is to read all messages, anonymize and generate the output file with length of the messages
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    messages_folder = directoryPath+'/messages/inbox'
    
    final_msg_df = pd.DataFrame([])
    
    for folder in os.listdir(messages_folder):
        folder_friendName = folder.split("_")[0].lower()
        
        # check whether the folder is a conversation with a friend. We ignore system files ex: ds.store
        
        try:
            
            if (len(folder.split("_")[1])>0 ): 
                
                # extract unique id. Incase, the friend's name is not in the friend list, ignore.        
                try:
                    #print(messages_folder+'/'+folder)
                    uniqueid = friends_df.loc[friends_df['joined_names']==folder_friendName,'unique_id'].values[0]
                    #print(uniqueid)
                    friend_name = friends_df.loc[friends_df['joined_names']==folder_friendName,'name'].values[0]

                    # read data from messages 
                    
                    #print(get_file(messages_folder+'/'+folder,'messages'))
                    
                    message_file = messages_folder+'/'+ folder + get_file(messages_folder+'/'+folder,'messages')
                    #print(message_file)

                    try:
                        with open(message_file) as f:
                            messages = json.load(f)

                        messages_df = json_normalize(messages['messages'])
                        #print(messages_df.head(1))

                        messages_df['from_id'] = np.where(messages_df['sender_name']==friend_name,uniqueid,my_id)
                        messages_df['to_id'] = np.where(messages_df['sender_name'] ==friend_name,my_id, uniqueid)
                        messages_df['length_of_msg']  = messages_df['content'].apply(lambda x: len(x))
                        messages_df = messages_df[[ 'from_id', 'to_id', 'length_of_msg', 'timestamp_ms']].copy()

                        final_msg_df = final_msg_df.append(messages_df)
                    except:
                        continue

                except: 
                    continue

            else: 
                continue
        except:
            continue
    final_msg_df = final_msg_df.reset_index()
    final_msg_df['timestamp_date'] = final_msg_df['timestamp_ms'].apply(lambda x: datetime.datetime.fromtimestamp(int(x/1000)).isoformat())
    
    #print(final_msg_df.head(10))    
    final_msg_df.to_csv(outputPath+'/message.csv', sep=',')

'''    
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
read_messages_inbox(directoryPath, outputPath)
'''


# # Timeline posts - task 3

# In[7]:


# To create post.csv, data is retrieved from the following:
#(i) comments/comments.json
#(ii)likes_and_reactions/posts_and_comments.json
#(iii) posts/your_posts.json
# It is possible that some of the comments are duplicated.
# So, I tried to sort them with timestamp column.

def read_comments(directoryPath, outputPath, post):
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    groups_df = pd.read_csv(outputPath+'/groups.csv')
    comments_folder = directoryPath+'/comments'
    
    if (os.path.isfile(comments_folder+'/comments.json') ):
        with open(comments_folder+'/comments.json') as f:
            comments_data = json.load(f)
        #pprint(comments_data)
        comments_df = json_normalize(comments_data['comments'])

        post_id = len(post)
        stop_word = ['post.','photo.','comment.','video.','album.', 'link.','event.','question.']

        user_id_from = unknown_friend_id
        user_id_to = unknown_friend_id
        post_activity = 999 # some default value
        timestamp = time.time()
        timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()

        for i in range(0, len(comments_df)):
            timestamp = comments_df.loc[i]['timestamp']
            timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()

            title = comments_df.loc[i]['title']
            words = title.split(' ')
            #user_id_from = my_id

            if ('replied to' in title):
                user_id_from = my_id
                if ('own ' in title):
                    user_id_to = my_id
                else:
                    counter = 0
                    while (words[counter] != 'to'):
                        counter += 1

                    counter += 1
                    user_to_name =''
                    while (words[counter] not in stop_word):
                        user_to_name = user_to_name+words[counter]+' '
                        counter += 1
                    user_to_name = user_to_name[:-3] 
                    user_to_name = unidecode_user_names(user_to_name) #get names compatible with friends' list
                    try:
                        user_id_to = friends_df.loc[friends_df['name']== user_to_name,'unique_id'].values[0]
                    except :
                        try:
                            #print(user_to_name)
                            user_id_to = groups_df.loc[groups_df['group_name']== user_to_name,'group_id'].values[0]
                            #print(user_id_to)
                        except : 
                            user_id_to = unknown_friend_id

                post_activity = 2
            elif ('commented on' in title):

                counter = 0
                #set user_id_from
                user_id_from = my_id
                if ('own' in title):

                    user_id_to = my_id
                else:
                    user_name =''
                    while (words[counter] != 'commented'):
                        user_name = user_name+words[counter]+' '
                        counter += 1
                    user_name = user_name[:-1]
                    user_name = unidecode_user_names(user_name)
                    try: 
                        user_id_from = my_id if (user_name == my_name) else friends_df.loc[friends_df['name']== user_name,'unique_id'].values[0]

                    except:
                        user_id_from = unknown_friend_id

                    #set the variable user_id_to
                    
                    while (words[counter] != 'on'): # skip 'on'
                        counter += 1 
                    #print(words[counter+1])
                    
                    user_to_name = ''
                    try:
                        while (words[counter] not in stop_word):

                            user_to_name = user_to_name+words[counter]+' '
                            counter += 1
                        #print(user_to_name)
                        user_to_name = unidecode_user_names(user_to_name[:-3]) # unidecode unusal characters
                        try:
                            user_id_to= friends_df.loc[friends_df['name']== user_to_name,'unique_id'].values[0]
                        except:
                            try:

                                user_id_to = groups_df.loc[groups_df['group_name']== user_to_name,'group_id'].values[0]
                                #print(user_id_to)
                            except : 
                                user_id_to = unknown_friend_id 
                    except : 
                        user_id_to = unknown_friend_id
                post_activity = 2

            post = post.append(pd.DataFrame({'user_id_from':user_id_from, 'user_id_to':user_id_to,
                                             'post_id':post_id, 'timestamp':timestamp, 'timestamp_date':timestamp_date,
                                             'post_activity':post_activity},
                                            index=[0]), ignore_index=True, sort=False)
            post_id += 1
    
    return post
'''
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
post = pd.DataFrame(columns = ['user_id_from','user_id_to','post_id','timestamp','timestamp_date','post_activity'])
read_comments(directoryPath, outputPath, post)
'''


# In[8]:


def read_likes_reactions(directoryPath,outputPath,post):
    # this function adds likes data to 'post' data frame
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    groups_df = pd.read_csv(outputPath+'/groups.csv')
    likes_reactions_folder = directoryPath+'/likes_and_reactions'
    legit_reactions = ['WOW', 'HAHA', 'LIKE', 'LOVE', 'SORRY']
    i = len(post)
    
    counter = 0 # for this file    
    with open(likes_reactions_folder+'/posts_and_comments.json') as f:
        likes_data = json.load(f)
        
    likes_df = json_normalize(likes_data['reactions'])
    reactions = ['likes','liked','reacted'] # interested only in these lines
    like_reacted_lines = [ line for line in likes_df['title'] if any(word in line for word in reactions)] # no need of this
    
    for line in like_reacted_lines:
        
        user_id_from = my_id
        user_id_to = 'u_0'
        timestamp = likes_df.loc[counter]['timestamp']
        timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()
        
        post_id = i
        post_activity = 1 # for likes
        some_body = ''
        words = line.split(' ') #  a list of words
        try:
            if any(term in line for term in ['likes','liked']):
                j = 3    
            elif any(term in line for term in ['reacted']): 
                j = 4

            while ('\'s' not in words[j] ):
                some_body = some_body +words[j]+' '
                j += 1
            some_body = unidecode_user_names(some_body+words[j][:-2])
            
            try:
                user_id_to = friends_df.loc[friends_df['name']== some_body,'unique_id'].values[0]
            except:
                try:    
                    user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                    #print(user_id_to)
                except : 
                    user_id_to = unknown_friend_id #  non friend's id
            
        except:
            continue
                
        # add info to data frame
        post = post.append(pd.DataFrame({'user_id_from':user_id_from, 'user_id_to':user_id_to, 
                                        'post_id':post_id, 'timestamp':timestamp,'timestamp_date':timestamp_date,
                                        'post_activity':post_activity},
                                       index=[0]), ignore_index=True, sort=False)
        i += 1
        counter += 1

    return post

'''
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
read_likes_reactions(directoryPath,outputPath,post)
'''


# In[9]:


def read_posts(directoryPath,outputPath,post):
    # This function reads your_posts.json for the post.csv file
    friends_df = pd.read_csv(outputPath+'/master_file_for_info_all_friends.csv')
    posts_folder = directoryPath+'/posts'
    groups_df = pd.read_csv(outputPath+'/groups.csv')
    '''
    posts_file = ''
    for _,_, files in os.walk(posts_folder):
        #print(files)
        for file in files:
            if( re.match(r'your_posts*[.json]',file)):
                #print(file)
                posts_file = file
                break
    '''
    #yourPosts = posts_folder +'/'+posts_file
    yourPosts = posts_folder + get_file(posts_folder,'posts')
    
    
    with open(yourPosts) as f:
        my_posts_data = json.load(f)
    #**********************
    # pprint(my_posts_data)
    #**********************
    if (yourPosts == (posts_folder +'/your_posts.json')):
        my_posts_df = json_normalize(my_posts_data['status_updates'])
    elif (yourPosts == (posts_folder +'/your_posts_1.json')):
        my_posts_df =json_normalize(my_posts_data)
    #print(my_posts_df.head(1))
    
    post_id = len(post)
    for i in range(0,len(my_posts_df)):
        timestamp = my_posts_df.loc[i].timestamp
        timestamp_date = datetime.datetime.fromtimestamp(timestamp).isoformat()
        user_id_from = my_id
        user_id_to = unknown_friend_id
        post_activity = 0
         
        title = str(my_posts_df.loc[i]['title'])  
        attachments = my_posts_df.loc[i]['attachments']
                
        legit_words = ['shared', 'wrote on','posted', 'added','updated','uploaded','likes']
        length_my_name = len(my_name)
               
        '''
        #The title is nan when there is an attachment not NaN
        if (title != 'nan' and attachments == 'NaN') :
            print(my_posts_df.loc[i]['attachments'])
        '''
        
        if (title != 'nan'):
            #print(my_posts_df.loc[i])
            #print(attachments)
            
            if (' shared' in title):
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                
                words = title.split(' ')
                if (len(words) <= 3):
                    user_id_to = my_id
                    post_activity = 3
                elif (words[len(words)-1] =='timeline.'):
                    
                    to_index = sum(i+1 for i,word in enumerate(words) if words[i]=='to')
                    some_body = ''
                    
                    while(words[to_index] != 'timeline.'):
                        some_body = some_body+words[to_index]+' '
                        to_index += 1
                    some_body = unidecode_user_names(some_body[:-3])
                    try:
                        user_id_to = friends_df.loc[friends_df['name']== some_body,'unique_id'].values[0]
                    except:
                        try:    
                            user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                            #print(user_id_to)
                        except : 
                            user_id_to = unknown_friend_id #  non friend's id

                    post_activity = 4
            
            title = str(my_posts_df.loc[i]['title'])  
            words = title.split(' ')
            if(' wrote' in title and (words[len(words)-1] =='Timeline.')):
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                
                words = title.split(' ')
                on_index = sum(i+1 for i,word in enumerate(words) if words[i]=='on')
                some_body = ''

                while(words[on_index] != 'Timeline.'):
                    some_body = some_body+words[on_index]+' '
                    on_index += 1
                some_body = unidecode_user_names(some_body[:-3])
                try:
                    user_id_to = friends_df.loc[friends_df['name']== some_body,'unique_id'].values[0]
                    #print(some_body +' ** '+user_id_to)
                except:
                    try:    
                        user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                        #print(user_id_to)
                    except : 
                        user_id_to = unknown_friend_id #  non friend's id

                post_activity = 4

            title = str(my_posts_df.loc[i]['title'])  
            words = title.split(' ')    
            if(' posted' in title):
                ### always 'posted in' a group
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                words = title.split(' ')
                
                in_index = sum(i+1 for i,word in enumerate(words) if words[i]=='in')
                
                some_body = ''
                
                for word in range(in_index,len(words)):
                    some_body = some_body+words[word]+' '
                some_body = unidecode_user_names(some_body[:-2])
                #print(some_body)
                try:
                    user_id_to = friends_df.loc[friends_df['name']== some_body,'unique_id'].values[0]
                    #print(some_body +' ** '+user_id_to)
                except:
                    try:    
                        user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                        #print(some_body +' ** '+user_id_to)
                    except : 
                        user_id_to = unknown_friend_id #  non friend's id
                        #print(some_body +' ** '+user_id_to)
                post_activity = 4
                
            title = str(my_posts_df.loc[i]['title'])  
            words = title.split(' ')    
            if (' added' in title):
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                words = title.split(' ')
                
                
                if ('to' in title and words[len(words)-1] =='timeline.'):
                    to_index = sum(i+1 for i,word in enumerate(words) if words[i]=='to')
                    some_body = ''
                    
                    while(words[to_index] != 'timeline.'):
                        some_body = some_body+words[to_index]+' '
                        to_index += 1
                    some_body = unidecode_user_names(some_body[:-3])
                    #print(title +' ** '+some_body)
                    try:
                        user_id_to = friends_df.loc[friends_df['name']== some_body,'unique_id'].values[0]
                    except:
                        try:    
                            user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                            #print(user_id_to)
                        except : 
                            user_id_to = unknown_friend_id #  non friend's id

                    post_activity = 4
            
                else:
                    user_id_to = my_id
                    post_activity =0
                
            title = str(my_posts_df.loc[i]['title'])  
            words = title.split(' ')    
            if(' updated' in title):  
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                words = title.split(' ')
                #print(title)
                
                if ('group' in title):
                    some_body = ''
                    in_index = sum(i+1 for i,word in enumerate(words) if words[i]=='in')
                
                    for word in range(in_index,len(words)):
                        some_body = some_body+words[word]+' '
                    some_body = unidecode_user_names(some_body[:-2])
                    #print(some_body) 
                    user_id_to = groups_df.loc[groups_df['group_name']== some_body,'group_id'].values[0]
                    post_activity = 4
                    #print(user_id_to)
                else : 
                    user_id_to = my_id #  self id
                    post_activity = 0
                    #print(user_id_to)
                    
            title = str(my_posts_df.loc[i]['title'])  
            words = title.split(' ')    
            if (' likes' in title or ' liked' in title):  
                title = title[(length_my_name+1):len(title)] # remove user_id_from
                words = title.split(' ')
                #print(title)
                
                user_id_to = my_id #  self id
                post_activity = 1
                
            # add info to data frame
            post = post.append(pd.DataFrame({'user_id_from':user_id_from, 'user_id_to':user_id_to, 
                                            'post_id':post_id, 'timestamp':timestamp,'timestamp_date':timestamp_date,
                                            'post_activity':post_activity},
                                           index=[0]), ignore_index=True, sort=False)

            post_id += 1
                
    return post
'''
#test this unit
#directoryPath = '/media/santhilata/SANTHI/facebook-paulyoung583234/'
directoryPath = '/media/santhilata/SANTHI/facebook-maitevanalboom-3/'
#outputPath = '/home/santhilata/Desktop/testFolder/paul'
outputPath = '/home/santhilata/Desktop/testFolder/maite'
read_posts(directoryPath,outputPath,post)
'''


# In[10]:


def create_post(directoryPath,outputPath):
    # THis function is to create post data frame and hence post.csv
    # The post is the combination of two functions
       
    # create empty data frame post
    post = pd.DataFrame(columns = ['user_id_from','user_id_to','post_id','timestamp','timestamp_date','post_activity'])
    post = read_comments(directoryPath,outputPath,post)
    post = read_likes_reactions(directoryPath,outputPath,post)
    post = read_posts(directoryPath,outputPath,post)    
    post.to_csv(outputPath+'/post.csv', sep=',') # create post.csv
    
    #print(post.head(10))
    #print(post.tail(20))


# ** Main function ** ( It will be called from the GUI. This should help if someone is making a single class)

# In[11]:


def main_fn():
    
    # get input directory path and set output directory path
    directoryPath= directory_path.get() 
    outputPath = output_path.get()
    
    # testing
    print("--directory path-- testing"+directoryPath) 
    
    # create friend.csv
    my_name = set_my_name(directoryPath)
    generate_friend_csv(directoryPath,outputPath) 
    
    #create group.csv
    if (os.path.exists(directoryPath+'/groups')):
        create_group_information(directoryPath, outputPath)
        group_posts_comments(directoryPath, outputPath)
    else:
        temp_df = pd.DataFrame(columns=('user_id','group_id','admin/member','group_name','joined_date'))
        temp_df.to_csv(outputPath+'/groups.csv', sep=',')
        temp_comments_df = pd.DataFrame(columns=('user_id','comment_post', 'length', 'timestamp', 'timestamp_date',
                                                 'group_id','group_name'))
        temp_comments_df.to_csv(outputPath+'/groups_comments.csv',sep=',')
        
    #create message.csv
    if (os.path.exists(directoryPath+'/messages')):
        read_messages_inbox(directoryPath,outputPath)
    else:
        temp_message_df = pd.DataFrame(columns=('from_id', 'to_id', 'length_of_msg', 'timestamp_ms','timestamp_date'))
        temp_message_df.to_csv(outputPath+'/message.csv',sep=',')
    
    # create post.csv
    if (os.path.exists(directoryPath+'/posts')):
        create_post(directoryPath,outputPath)
    else:
        temp_post_df = pd.DataFrame(columns=('user_id_from','user_id_to','post_id','timestamp',
                                             'timestamp_date','post_activity'))
        temp_post_df.to_csv(outputPath+'/post.csv',sep=',')
    
    # testing
    print("Output directory created")


# # GUI. 
# **For it's' simplicity, I chose tkinter. **

# In[12]:


import tkinter 
from tkinter import *
from tkinter import filedialog

from PIL import Image, ImageTk

global directory_path
global output_path
global spinner_wheel

def get_directory_path(initialdir='.'): #open the file 
    folder_name = tkinter.filedialog.askdirectory()  
    directory_path.set(folder_name)
    output_path.set(folder_name + '/outputDir' ) # sets the ouput directory
        
def generate_output():
    print("inside main_function")
    spinner_wheel.set("processing... ...")
    main_fn()
    spinner_wheel.set("Process complete. Check your outputDir")

win = Tk()
win.title("Anonymization")
win.configure(background= '#00aa90')
win.geometry("500x200") #You want the size of the app to be 500x200
win.resizable(0, 0) 

back = tkinter.Frame(master=win,bg='black')
#back.pack_propagate(0) #Don't allow the widgets inside to determine the frame's width / height
#back.pack(fill=tkinter.BOTH, expand=1) 
#win.option_add("*Button.Background", "black")
#win.option_add("*Button.Foreground", "white")
win.option_add("*Label.Foreground", "#BB0000")

directory_path = StringVar()
output_path = StringVar()
spinner_wheel = StringVar()
spinner_wheel.set("          ")

l1 = Label(win, text="Anonymization Software", font=('Helvetica', 18, 'bold'))
l1.config()

l1.pack() 
'''
load = Image.open("sunflower.jpg")
render = ImageTk.PhotoImage(load)
img = Label( image=render)
img.image = render
img.place(x=10, y=10)
'''

b1 = tkinter.Button(win, text='Set directory path', font=('Helvetica', 12, 'bold'),command=get_directory_path)
b1.pack(side=LEFT, padx=5, pady=15)  

b2 = Button(win, text='Generate output', font=('Helvetica', 12, 'bold'), command=generate_output)
b2.pack(side=LEFT, padx=50, pady=15)

b3 =Button(win, text="Quit", font=('Helvetica', 12, 'bold'), command=win.destroy)
b3.pack(side=LEFT, padx=5, pady=15)

#l2 = Label(win, textvariable =spinner_wheel)
#l2.pack() 

#Button(win, text="Quit", command=win.destroy).pack(side=LEFT, padx=5, pady=10)
# TO DO : make a label saying that you can close the window after creation of the output
win.mainloop()


# In[ ]:




