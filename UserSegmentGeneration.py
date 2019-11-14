
# coding: utf-8

# In[2]:

import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
#from sklearn.metrics import accuracy_score
import pandas as pd
import numpy as np
#import matplotlib.pyplot as plt
from pymongo import MongoClient
#from sklearn.metrics import accuracy_score
import pyfpgrowth
from datetime import datetime



print("------------Starting after loading modules")
client = MongoClient('mongodb://personalization_user:personalization_pass@localhost:27017/?authSource=Personalization')
print("-----------created mongo client-----------")
db = client['ECommerce']



# In[3]:

#sample of input that comes from web page
fam_lis=["BABY DIAPERS MEDIUM","BABY BATHING SOAPS/WASH"]
cat_lis=["MH-Stainless Steel Cookware","MH-Tablewares"]


# In[4]:

#if hirarchy value is category and sub hirarchy value is family then all the families selected are sent as a parameter to this function.
def if_cat_and_family_chosen(fam_lis):
    cust_ids=[]
    cat_data = pd.DataFrame(list(db["customer_transaction_details"].find({"FAMILY_NAME":{"$in":fam_lis}})))
    client.close()
    cust_id=cat_data["cust_id"]
    cust_ids=list(set(cust_id))
    return cust_ids





# In[5]:

#if hirarchy value is market and sub hirarchy value is category then all the categories selected are sent as a parameter to this function.
def if_market_and_cat_chosen(cat_lis):
    cust_ids=[]
    cat_data = pd.DataFrame(list(db["customer_transaction_details"].find({"CATEGORY_NAME":{"$in":cat_lis}})))
    client.close()
    cust_id=cat_data["cust_id"]
	#removing duplicate customer ids.
    cust_ids=list(set(cust_id))
    return cust_ids


# In[6]:

#Based on the hirarchy value chosen in web-portal function is getting executed.
def fam_or_cat_chosen(hirarchy,hierarchy_list):
    unique_cust_ids=[]
    if hirarchy=="FAMILY":
      result_cust_id=if_cat_and_family_chosen(fam_lis)
	  unique_cust_ids.append(result_cust_id)
    if hirarchy=="CATEGORY":
       result_cust_id=if_market_and_cat_chosen(cat_lis)
       unique_cust_ids.append(result_cust_id)


    return unique_cust_ids




# In[7]:


#a=fam_or_cat_chosen("FAMILY",fam_lis)


# In[8]:


#c=fam_or_cat_chosen("CATEGORY",cat_lis)


# In[14]:


#print("family-cust")
#print(a)


# In[15]:


#print("cat-cust")
#print(len(c))



####### AFFLUENT CUSTOMERS

# In[ ]:


def affluent_customers(catname):
    #selecting required data which has category as chosen
    category_data = pd.DataFrame(list(db["customer_transaction_details"].find({"CATEGORY_NAME":catname})))
    client.close()
    #Finding price-averages of families within selected category 
    value=pd.pivot_table(category_data,index=["FAMILY_NAME"],values="MO_MRP_LN_ITM_RTN",aggfunc=np.mean).reset_index()
    value=value.sort_values(by='MO_MRP_LN_ITM_RTN', ascending=False)
    #selecting top2 costliest families in that category
    top2_values=value[0:2]
    fam_avg={}
    fam_list=[]
    #storing data of family name and average in form of dictionary
    for i,row in top2_values.iterrows():
        fam_avg[row["FAMILY_NAME"]]=row["MO_MRP_LN_ITM_RTN"]
        fam_list.append(row["FAMILY_NAME"])
    print(fam_avg)
    cust_ids={}
    copy=[]
    #selecting customers who have bought from top2 costliest families and the price of the product they bought are more than 
    #family average
    for i,row in category_data.iterrows():
        if row["FAMILY_NAME"] in fam_list:
            if row["MO_MRP_LN_ITM_RTN"]>=fam_avg[row["FAMILY_NAME"]]:
                if row["cust_id"] not in cust_ids:
                    cust_ids[row["cust_id"]]=1
                else:
                    cust_ids[row["cust_id"]]=cust_ids[row["cust_id"]]+1
    required_cust_above_2=[]
    #Selecting only customers who have purchsed from that category more than 2 times
    for i in cust_ids:
        if cust_ids[i]>2:
            required_cust_above_2.append(i)
            
        
    
        
    return required_cust_above_2


# In[ ]:


af=affluent_customers("MH-Hair Care")


# In[ ]:


print(af)


####### VISIT BASED


# In[ ]:


def user_segment_based_on_number_of_visit(start_date,end_date,catname):
    #selecting data which has only selected category
    category_data = pd.DataFrame(list(db["customer_transaction_details"].find({"CATEGORY_NAME":catname})))
    client.close()
    #convering string to date to format
    start_date= datetime.strptime(start_date, '%d/%m/%Y').date()
    end_date= datetime.strptime(end_date, '%d/%m/%Y').date()
    print(start_date,end_date)
    cust_ids=[]
    cust_id_dict={}
    for i,row in category_data.iterrows():
        #date anamoly for feb data 
        if row["BUSINESS_DATE"].split('/')[0]=='2' or row["BUSINESS_DATE"].split('/')[0]=='02':
            #only in case of feb date is in format m-d-y 
            #store date as txn_date
            txn_date=datetime.strptime(row["BUSINESS_DATE"], '%m/%d/%Y').date()
        else:   
            #store date as txn_date for other months
            txn_date=datetime.strptime(row["BUSINESS_DATE"], '%d/%m/%Y').date()
        #if txn_date is between specified limit then consider that txn
        if txn_date>=start_date and txn_date<=end_date:
            cust_ids.append(row["cust_id"])
            #appending txn_dates to cust_id dict in where key is cust id and value is txn_dates
            if row["cust_id"] not in cust_id_dict:
                cust_id_dict[row["cust_id"]]=[]
                cust_id_dict[row["cust_id"]].append(txn_date)
            else:
                if txn_date not in cust_id_dict[row["cust_id"]]:
                      cust_id_dict[row["cust_id"]].append(txn_date)
    required_cust_lis=[]
    for i in cust_id_dict:
        #selecting customer who has transacted more than 2 times and less than or equal to 5 times in that category
        if len(cust_id_dict[i])>2 and len(cust_id_dict[i])<=5:
            required_cust_lis.append(i)
            
    return required_cust_lis
    


# In[ ]:


av=user_segment_based_on_number_of_visit('26/01/2016','12/5/2016',"MH-Hair Care")


# In[ ]:


print(av)






