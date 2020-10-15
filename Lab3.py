import urllib.request, json 
import pandas as pd
import numpy as np
import csv
import os
import shutil
from datetime import datetime, date, time
import dateutil.parser
import re
import time

#Загрузка
#'''

cols = ["Vacancy Name","City","Salary Min","Salary Max","Company Name","Date","Expierence","Employment","Schedule","Description","Responsibility","Requirement","Key Skills"]


rows = []

def addValue1(data, name, row, col):
    if name in data and data[name] != None:
        addToIndex = cols.index(col)
        addValue = data[name]
        if addToIndex != -1:
            if not pd.isna(row[addToIndex]):
                row[addToIndex] = row[addToIndex]+";"+addValue
            else:
                row[addToIndex] = addValue

def addValue2(data, name, name2, row, col):
    if name in data and data[name] != None and name2 in data[name]:
        addToIndex = cols.index(col)
        addValue = data[name][name2]
        if addToIndex != -1:
            if not pd.isna(row[addToIndex]):
                row[addToIndex] = row[addToIndex]+";"+addValue
            else:
                row[addToIndex] = addValue

def addValueM(data, name, name2, row, col):
    if name in data and data[name] != None:
        addToIndex = cols.index(col)
        for skill in data[name]:
            if addToIndex != -1:
                if not pd.isna(row[addToIndex]):
                    row[addToIndex] = row[addToIndex]+";"+skill["name"]
                else:
                    row[addToIndex] = skill["name"]

ids = []


def parseOneVacancy(id, snippet):
    s = False
    while(not s):
        try:
            with urllib.request.urlopen("https://api.hh.ru/vacancies/"+id) as url:
                data = json.loads(url.read().decode("utf-8"))
                item = data
                #print(data)
                newRow = [np.nan] * len(cols)
                addValue1(item, "name", newRow, "Vacancy Name")
                addValue2(item, "address", "city", newRow, "City")
                addValue2(item, "salary", "from", newRow, "Salary Min")
                addValue2(item, "salary", "to", newRow, "Salary Max")
                addValue2(item, "employer", "name", newRow, "Company Name")
                addValue1(item, "published_at", newRow, "Date")
                addValue2(item, "experience", "name", newRow, "Expierence")
                addValue2(item, "employment", "name", newRow, "Employment")
                addValue2(item, "schedule", "name", newRow, "Schedule")
                addValue1(item, "description", newRow, "Description")
                if snippet:
                    addValue1(snippet,"responsibility", newRow, "Responsibility")
                    addValue1(snippet, "requirement", newRow, "Requirement")
                addValueM(item, "key_skills", "name", newRow, "Key Skills")
                rows.append(newRow)
                ids.append(item['id'])
                s = True
        except:
            time.sleep(1)

def parseUsrl(url1):
    pages = 1
    v = 0
    page = 0
    while page < pages:
        s = False
        while(not s):
            try:
                with urllib.request.urlopen(url1+"&per_page=100&page="+str(page)) as url:
                    data = json.loads(url.read().decode("utf-8"))
                    items = data["items"]
                    pages = data["pages"]
                    for item in items:
                        if item['id'] not in ids:
                            parseOneVacancy(item['id'], item['snippet'])
                            v = v + 1
                    page = page + 1
                    s = True
            except:
                time.sleep(1)
        print(page)



with urllib.request.urlopen("https://api.hh.ru/specializations") as url:
    data = json.loads(url.read().decode("utf-8"))
    for i in data[0]['specializations']:
        print(str(i)+" "+str(len(data[0]['specializations'])))
        parseUsrl("https://api.hh.ru/vacancies?specialization="+i['id'])
        result = pd.DataFrame(rows, columns=cols)




result = pd.DataFrame(rows, columns=cols)
result.to_csv("result.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

#'''
#Обработка



result = pd.read_csv("result.csv", sep=',', delimiter=",", index_col=[0], na_values=['NA'], low_memory=False)


def formatForFileName(value):
    value = str(value)
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', ' ', value)
    return value

for column in result.columns:
    if column != "Date" and column != "Key Skills":
        dataTypeObj = result.dtypes[column]
        if dataTypeObj == 'object':
            result[column] = result[column].apply(lambda x: formatForFileName(x))


salaryDf = result.loc[(result['Salary Max'].notnull()) & (result['Salary Min'].notnull())]

salaryDf = salaryDf.sort_values(by=["Salary Max", "Salary Min"])

sMaxUnique = salaryDf["Salary Max"].unique()

sParts = np.array_split(sMaxUnique,10)

sGroups = []

shutil.rmtree('results', ignore_errors=True)
os.mkdir('results')
os.mkdir('./results/salary')

def saveCount(df, colName, fileName):
    vacancies = []
    names = df[colName].unique()
    for name in names:
        count = (df[colName] == name).sum()
        vacancies.append([name, count])
    vacancies2 = pd.DataFrame(vacancies, columns=[colName, "Count"])
    vacancies2 = vacancies2.sort_values(by=["Count"],ascending=[False])
    vacancies2.to_csv(fileName,  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

last = 0

sDaysRows = []

for sRange in sParts:
    selected = salaryDf.loc[(salaryDf['Salary Min'] > last) & (salaryDf['Salary Max'] <= max(sRange))]
    sRangeS = str(last) + "-" + str(int(max(sRange)))

    last = max(sRange)
    sGroups.append(selected)
    os.mkdir('./results/salary/' + sRangeS)

    saveCount(selected, "Vacancy Name", './results/salary/' + sRangeS + "/vacancies.csv")
    sDates = selected["Date"].apply(lambda x: dateutil.parser.parse(x, ignoretz=True))
    if len(sDates)>0:
        sDates = (datetime.now() - sDates).dt.days
        sDaysRows.append([sRangeS, sDates.mean(), sDates.min(), sDates.max()])
    else:
        sDaysRows.append([np.nan,np.nan,np.nan])

    saveCount(selected, "Expierence", './results/salary/' + sRangeS + "/Expierence.csv")

    saveCount(selected, "Employment", './results/salary/' + sRangeS + "/Employment.csv")

    saveCount(selected, "Schedule", './results/salary/' + sRangeS + "/Schedule.csv")

    vSkills = selected.loc[(selected["Key Skills"].notnull())]
    if len(vSkills) != 0 :
        b = pd.DataFrame(vSkills["Key Skills"], columns=["Key Skills"])
        b["Key Skills"] = b["Key Skills"].apply(lambda x: x.split(';'))
        b = b.explode("Key Skills")
        b["Key Skills"].apply(lambda x: formatForFileName(x))
        saveCount(b, "Key Skills", './results/salary/' + sRangeS + "/Skills.csv")

    
sDaysresult = pd.DataFrame(sDaysRows, columns=['Range', "Avg", "Min", "Max"])
sDaysresult.to_csv("./results/salary/days.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

#################

os.mkdir('./results/vacancy')

vNames = result["Vacancy Name"].unique()

SalaryMin = []
SalaryMax = []
Expierence = []
Employment = []
Schedule = []
KeySkills = []

print(len(vNames))

counter = 0

for vName in vNames:
    selected = salaryDf.loc[(salaryDf["Vacancy Name"] == vName)]

    items = selected["Salary Min"].unique()
    for item in items:
        count = (selected["Salary Min"] == item).sum()
        SalaryMin.append([vName, item, count])

    items = selected["Salary Max"].unique()
    for item in items:
        count = (selected["Salary Max"] == item).sum()
        SalaryMax.append([vName, item, count])

    items = selected["Expierence"].unique()
    for item in items:
        count = (selected["Expierence"] == item).sum()
        Expierence.append([vName, item, count])

    items = selected["Employment"].unique()
    for item in items:
        count = (selected["Employment"] == item).sum()
        Employment.append([vName, item, count])

    items = selected["Schedule"].unique()
    for item in items:
        count = (selected["Schedule"] == item).sum()
        Schedule.append([vName, item, count])


    vSkills = selected.loc[(selected["Key Skills"].notnull())]
    if len(vSkills) != 0 :
        b = pd.DataFrame(vSkills["Key Skills"], columns=["Key Skills"])
        b["Key Skills"] = b["Key Skills"].apply(lambda x: x.split(';'))
        b = b.explode("Key Skills")
        b["Key Skills"].apply(lambda x: formatForFileName(x))
        items = b["Key Skills"].unique()
        for item in items:
            count = (b["Key Skills"] == item).sum()
            KeySkills.append([vName, item, count])

            
    
    counter = counter+1
    if counter%1000==0:
        print(counter)


SalaryMin = pd.DataFrame(SalaryMin, columns=["Vacancy", "Value", "Count"])
SalaryMin = SalaryMin.sort_values(by=["Count"],ascending=[False])
SalaryMin.to_csv("./results/vacancy/SalaryMin.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

SalaryMax = pd.DataFrame(SalaryMax, columns=["Vacancy", "Value", "Count"])
SalaryMax = SalaryMax.sort_values(by=["Count"],ascending=[False])
SalaryMax.to_csv("./results/vacancy/SalaryMax.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

Expierence = pd.DataFrame(Expierence, columns=["Vacancy", "Value", "Count"])
Expierence = Expierence.sort_values(by=["Count"],ascending=[False])
Expierence.to_csv("./results/vacancy/Expierence.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

Employment = pd.DataFrame(Employment, columns=["Vacancy", "Value", "Count"])
Employment = Employment.sort_values(by=["Count"],ascending=[False])
Employment.to_csv("./results/vacancy/Employment.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

Schedule = pd.DataFrame(Schedule, columns=["Vacancy", "Value", "Count"])
Schedule = Schedule.sort_values(by=["Count"],ascending=[False])
Schedule.to_csv("./results/vacancy/Schedule.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

KeySkills = pd.DataFrame(KeySkills, columns=["Vacancy", "Value", "Count"])
KeySkills = KeySkills.sort_values(by=["Count"],ascending=[False])
KeySkills.to_csv("./results/vacancy/KeySkills.csv",  na_rep = 'NA', index=True, index_label="",quotechar='"',quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")
