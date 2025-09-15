import numpy as np
import pandas as pd
from pathlib import Path
import re
from deep_translator import GoogleTranslator
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

#-----------------get the raw data and convert it into python------------------------
folder_path = Path(r"C:\Users\Mahsa\Desktop\geo")
province_code = [f"{i:02d}" for i in range(31)]
#file_path = Path(r"C:\Users\Mahsa\Desktop\geo\GEO89Xlsx.xlsx")
#file_name = "GEO89Xlsx.xlsx"
for file_path in folder_path.glob('*.xlsx'):
    file_name = file_path.name
    print(f"Processing file: {file_name}")
    print(f"The file path is: {file_path}")
    match = re.search(r"\d+", file_name)
    if match:
        year = int(match.group())
    print(f"extracted year: {year}")
    
    if year <= 92:
        df = pd.DataFrame()
        for code in province_code:
            excel_data = pd.read_excel(file_path, skiprows=2, sheet_name= code, dtype={"آدرس": "str"})
            df = pd.concat([df, excel_data], ignore_index=True)
            print(df.shape)
        df=df[df["استان"]!="استان"]
        print(np.all(df["شهرستان"].isna().sum()==31))
        df["year"] = year
        pd.to_pickle(df, fr"C:\Users\Mahsa\Desktop\geo\processed\GEO_{year}.pkl")
    elif year >= 93 and year < 99:
        df = pd.read_excel(file_path, dtype={"address": "str"})
        df["year"] = year
        pd.to_pickle(df, fr"C:\Users\Mahsa\Desktop\geo\processed\GEO_{year}.pkl")
    else:
        df = pd.read_excel(file_path,dtype={"address": "str"})
        df["year"] = year
        pd.to_pickle(df, fr"C:\Users\Mahsa\Desktop\geo\processed\GEO_{year}.pkl")
      
#-------------------------------data cleaning----------------------------------------------
folder_path = Path(r"C:\Users\Mahsa\Desktop\geo\processed")
for file_path in folder_path.glob('*.pkl'):
   df = pd.read_pickle(file_path)
   file_name = file_path.name.split(".")[0]
   print(f"The file is: {file_name}")
   match = re.search(r"\d+",file_name)
   if match:
       year = int(match.group())
   print(f"The extracted year is: {year}") 
   if year < 92:
       df.rename(columns={"آدرس":"address",
                          "استان":"province",
                          "شهرستان": "county",
                          "بخش":"bakhsh",
                          "شهر":"city",
                          "دهستان": "dehestan",
                          "آبادی": "abadi"}, inplace = True)
   elif year == 92:
       df[df["شهرستان"].isna()]
       df = df.dropna(subset = ["شهرستان"])
       df = df.replace(r'^\s*$', np.nan, regex=True)
       print(np.all(df["شهرستان"].isna().sum()==31))
       df.rename(columns={"آدرس":"address",
                          "استان":"province",
                          "شهرستان": "county",
                          "بخش":"bakhsh",
                          "شهر":"city",
                          "دهستان": "dehestan",
                          "آبادی": "abadi"}, inplace = True)
   elif year >= 93 and year<95:
       df.rename(columns={"استان":"province",
                          "شهرستان": "county",
                          "بخش":"bakhsh",
                          "شهر":"city",
                          "دهستان": "dehestan",
                          "آبادی": "abadi"}, inplace = True)
   elif year >= 95 and year <97:
       df.rename(columns={"Ostan_Name":"province",
                          "shahrest_Name": "county",
                          "Bakhsh_Name":"bakhsh",
                          "City_Name":"city",
                          "Dehestan_Name": "dehestan",
                          "Abadi_Name": "abadi"}, inplace = True)
   elif year == 97:
       df.rename(columns={"نام استان":"province",
                          "نام شهرستان": "county",
                          "نام بخش":"bakhsh",
                          "نام شهر":"city",
                          "نام دهستان": "dehestan",
                          "نام آبادی": "abadi"}, inplace = True)
       df.drop(columns = ["Unnamed: 7","Unnamed: 8"], inplace = True)
   elif year == 98:
       df.rename(columns={"نام استان":"province",
                          "نام شهرستان": "county",
                          "نام بخش":"bakhsh",
                          "نام شهر":"city",
                          "نام دهستان": "dehestan",
                          "نام آبادی": "abadi"}, inplace = True)
   elif year == 99 or year==1401:
       df.rename(columns={"نام استان":"province",
                          "نام شهرستان": "county",
                          "نام بخش":"bakhsh",
                          "نام شهر":"city",
                          "نام دهستان": "dehestan",
                          "NAME": "abadi"}, inplace = True)
   elif year == 1400:
       df.rename(columns={"Ostan_name":"province",
                          "Shahrestan_name": "county",
                          "Bakhsh_name":"bakhsh",
                          "Dehestan/Shahr_name": "dehestan",
                          "NAME": "abadi"}, inplace = True)
   elif year == 1402:
       df.rename(columns={"نام استان":"province",
                          "نام شهرستان": "county",
                          "نام بخش":"bakhsh",
                          "نام دهستان/ شهر": "dehestan",
                          "نام": "abadi"}, inplace = True)
   print(df.columns)
   pd.to_pickle(df, fr"C:\Users\Mahsa\Desktop\geo\processed\{file_name}_2.pkl")


#---------------------------------first analysis--------------------------------------------


list_inOrder = [
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_89_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_90_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_91_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_92_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_93_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_94_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_95_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_96_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_97_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_98_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_99_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_1400_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_1401_2.pkl",
    r"C:\Users\Mahsa\Desktop\geo\processed\GEO_1402_2.pkl"
]
for file_path in list_inOrder:
    file_name = file_path.split('\\')[-1]
    print(f"The file name is {file_name}")
    match = re.search(r'\d+', file_name)
    if match:
        num = int(match.group())
    print(f"The number is {num}")
    if num == 89:
        GEO_all = pd.read_pickle(file_path)
        GEO_all = GEO_all.loc[:,["address","year"]]
        GEO_all["address"] = GEO_all["address"].str.strip()
        GEO_all = GEO_all[GEO_all["address"].str.len()==4]
        print(GEO_all.shape[0])
    else:
        df = pd.read_pickle(file_path)
        df = df.loc[:,["address"]]
        df["address"] = df["address"].str.strip()
        df = df[df["address"].str.len()==4]
        print(df.shape[0])
        GEO_all = pd.merge(df, GEO_all, on = ["address"], how = "outer")
        GEO_all.loc[GEO_all["year"].isna(),"year"] = num
GEO_all["year"] = GEO_all["year"].astype(int)
GEO_all.to_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\GEO_all.pkl")


#---------------------------------deep into it----------------------------------------

geo_all = pd.read_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\GEO_all.pkl")
for year in sorted(geo_all["year"].unique()):
    df = pd.read_pickle(fr"C:\Users\Mahsa\Desktop\geo\processed\GEO_{year}_2.pkl")
    df = df.loc[:, ["address", "county"]]
    df["address"] = df["address"].str.strip()
    if year ==89:
        geo = pd.merge(geo_all, df, on = ["address"], how= "left")
    else:
        geo = pd.merge(geo, df, on = ["address"], how= "left")
    geo.rename(columns= {
        "county": f"county{year}"
    }, inplace= True)
    print(year)

geo.isna().sum()
geo.to_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\geo.pkl")
#geo.to_excel(r"C:\Users\Mahsa\Desktop\geo\processed\geo.xlsx")

#-----------------------------------------check the data----------------------------------------

geo_all = pd.read_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\GEO_all.pkl")
geo = pd.read_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\geo.pkl")
geo_all["year"].value_counts()
np.all([
    geo["county89"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]==89].shape[0],
    geo["county91"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=91].shape[0] + 1, # طبس
    geo["county92"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=92].shape[0] + 1, # طبس
    geo["county97"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=97].shape[0] + 1, # طبس
    geo["county98"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=98].shape[0] + 1, # طبس
    geo["county99"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=99].shape[0] + 1, # طبس
    geo["county1400"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=1400].shape[0] + 1, # طبس
    geo["county1401"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=1401].shape[0] + 1, # طبس
    geo["county1402"].isna().sum() == geo_all.shape[0] - geo_all[geo_all["year"]<=1402].shape[0] + 1, # طبس
])
geo.columns
'''
GEO_all["province"] = GEO_all["province"].apply(lambda x: GoogleTranslator(source='auto', target='en').translate(x))
GEO_all["county"] = GEO_all["county"].apply(lambda x: GoogleTranslator(source='auto', target='en').translate(x))

'''



list_ofColumns = [
    "2010-2011",
    "2011-2012",
    "2012-2013",
    "2013-2014",
    "2014-2015",
    "2015-2016",
    "2016-2017",
    "2017-2018",
    "2018-2019",
    "2019-2020",
    "2020-2021",
    "2021-2022",
    "2022-2023",
    "2023-2024"
]
num_counties = [
    397,397,421,429,429,429,429,429,434,448,457,469,474,482
]

plt.figure(figsize=(20, 10))
plt.bar(list_ofColumns, num_counties)
plt.ylim(350, 500)
plt.xticks(fontsize=14, rotation = 45)
plt.yticks(fontsize=14)
plt.show()



#--------------------------------------get the name of provinces-----------------------

df = pd.read_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\GEO_1402_2.pkl")
df["address"] = df["address"].str.strip()
df = df[df["address"].str.len()==2]
df = df[["address","province"]]
df.shape
df.columns
df.head()
df.to_pickle(r"C:\Users\Mahsa\Desktop\geo\processed\geo_province.pkl")
geo["province_code"] = geo["address"].str[0:2]
geo = pd.merge(geo, df, right_on = "address", left_on= "province_code", how = "left")
geo.rename(columns={
    "address_x": "county_code"
}, inplace=True)
geo.drop(columns=["address_y"], inplace=True)
geo.head()

#---------------------------------------------------------------------------------------------
geo_p = geo.groupby("province_code").aggregate(
        province = ("province", 'first'),
        county89=("county89", lambda x: x.notna().sum()),
        county1402 = ("county1402", lambda x: x.notna().sum())
    )
geo_p["dif"] = geo_p["county1402"] - geo_p["county89"]
geo_p[["province","dif"]]


# the number of counties added is each province should be equal to the number of changes in the country
geo_p["dif"].sum() == 482 - 397



