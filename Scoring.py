def to_format(csv1):
    import pandas as pd    
    import requests, json
    import urllib.parse
    from math import pi,sqrt,sin,cos,atan2, acos
    #Choix des coefficients
    
    coeff_entité = 0.1
    coeff_délai = 1
    coeff_consultant = 2
    coeff_langue = 1.5
    coeff_salaire = 1
    coeff_distance = 1
    coeff_experience = 1
    
    comp_E = []
    comp_NE = []
    
    
    data_comp_E = ['PYTHON','R', 'SCALA','C++','HADOOP','SPARK','JAVASCRIPT','DOCKER']
    data_comp_NE = ['HIVE','HBASE','MESOS','COUCHBASE', 'ELK','KUBERNETES','SQL']
    
    java_comp_E = ['JAVA', 'J2EE', 'SPRING', 'SPRING BOOT', 'REST']
    java_comp_NE = ['HIBERNATE', 'GROOVY', 'GRAILS', 'ANGULAR', 'IAAS', 'AWS', 'ELK']
    
    devops_comp_E = ['IAC','PACKER', 'ANSIBLE', 'SALTSTACK', 'PUPPET', 'CHEF','DOCKER','RANCHEROS','PYTHON','BASH','SHELL']
    devops_comp_NE = ['JENKINS','GITLAB', 'TRAVIS', 'XL-DEPLOY','KUBERNETES','AWS']
    
    competence = data_comp_E + data_comp_NE + java_comp_E + java_comp_NE+ devops_comp_E + devops_comp_NE

    df = pd.read_csv('y_pred_1.csv',encoding=('utf-8'))
    df2 = pd.DataFrame(columns=['index', 'Lieu'])
    
    rue = df[df['Lines'].str.contains('rue|boulevard|boulevard|quai|avenue')]
    rue.rename(columns={"Lines": "Lieu"},inplace=True)
    
    #rue['Lieu'] = rue['Lieu'].apply(lambda x:re.sub(regex_tel,"",x))
    rue['Lieu'] = rue['Lieu'].apply(lambda x:re.findall("\d+[\s, ]+\w+[\s \w]*",x))
    
    rue = rue.reset_index()

    for i in range(0,len(rue)):
        #print(rue.loc[i,'Lieu'])
        string = rue.loc[i,'Lieu'][0]
        index = rue.loc[i,'index']
        a = []
        a.append(index)
        a.append(string)
        splitted = string.split()
        if(len(splitted[0])>3):
            a = []
        else:
            df2.loc[i,'index'] = int(a[0])
            df2.loc[i,'Lieu']  = a[1]
    rue['Lieu']= df2['Lieu']
    mission = df[df['Lines'].str.contains('poste|intitulé|mission',case = False)]
    mission.rename(columns={"Lines": "Poste"},inplace=True)        
    expérience = df[df['Lines'].str.contains('experience|expérience|année|ans|an',case = False)]
    expérience['Lines'] = expérience['Lines'].apply(lambda x : re.sub("[^0-9]","",x))

    nan_value = float("NaN")
    expérience.replace("", nan_value, inplace=True)
    expérience.dropna(subset = ["Lines"], inplace=True)
    expérience['Lines'] = expérience['Lines'].apply(lambda x :int(x))
    
    expérience['Lines'] = expérience[expérience['Lines']<50]
    expérience.dropna(subset = ["Lines"], inplace=True)
    expérience.rename(columns={"Lines": "Experience"},inplace=True)
    consultant = df[df['Lines'].str.contains('consultant',case = False)]
    consultant['Lines'] = consultant['Lines'].apply(lambda x : re.sub("[^0-9]","",x))
    consultant.rename(columns={"Lines": "Nb_Consultant"},inplace=True)
    salaire = df[df['Lines'].str.contains('€|salaire|euros|euro',case = False)]
    salaire['Lines'] = salaire['Lines'].apply(lambda x : re.sub("[^0-9]","",x))
    salaire.rename(columns={"Lines": "Salaire"},inplace=True)
    compétence = df[df['Lines'].str.contains('compétence',case = False)]
    compétence.rename(columns={"Lines": "Compétence"},inplace=True)
    companies = pd.read_csv('preprocessed_CAC40.csv',sep=',')
    CAC40 = pd.read_csv('preprocessed_CAC40.csv',sep=',')
    CAC40 = CAC40.groupby('Name').sum('Volume').reset_index()
    CAC40.rename(columns = {'Unnamed: 0' : 'Valeur', 'Name' : 'Entité'},inplace = True)
    CAC40 = CAC40.sort_values('Valeur', ascending = False)
    CAC40 = CAC40[['Entité']]
    CAC40['Entité']= CAC40['Entité'].apply(lambda x : x.upper())
    CAC40.reset_index(inplace = True)
    CAC40.rename(columns = {'index' : 'Valeur'},inplace = True)
    
    companies= companies['Name'].unique().tolist()
    CAC40 = CAC40['Entité'].tolist()
    corp = companies + CAC40
    corp = [x.upper() for x in corp]
    companies = pd.read_csv('companies_ranking.csv')
    companies['Entité']= companies['Entité'].apply(lambda x : x.upper())
    
    for i in range(0,len(df['Lines'])):
        for sentences in df.iloc[i]:
            for word in sentences.split():
                word = word.upper()
                if word in corp:
                    df['Lines'].iloc[i]=word
                else :
                    df['Lines'].iloc[i]=''
                
    entreprise = df
    entreprise.rename(columns={"Lines": "Entité"},inplace=True)
    df = pd.read_csv('y_pred_1.csv',encoding=('utf-8'))
    df = df.join(rue).join(mission).join(expérience).join(consultant).join(salaire).join(compétence).join(entreprise)
    df = df.drop('Lines',axis=1)
    #df.to_csv('Finalized.csv')
    
    df =df.drop(columns={'index'},axis=1)
    #lang = df['Compétence'].str.get_dummies(sep=';')
    lang = df['Compétence']
    df = df.drop('Compétence',axis=1)
    lang = lang.dropna()
    #lang = lang.str.get_dummies(sep=',')
    lang = lang.apply(lambda x : x.upper())   
    def extract(s):
        s= s.split()
        l = []
        for i in s:
            if i in competence:
                l.append(i)
        return l
    
    lang = lang.apply(lambda x : extract(x))
    
    lang = lang.apply(lambda x: str(x)[1:-1])
    lang = lang.str.get_dummies()
    companies = pd.read_csv('companies_ranking.csv')
    companies['Entité']= companies['Entité'].apply(lambda x : x.upper())
    mask = []
    for i in range(len(df)):
        if df.iloc[i]['Entité'] in companies.Entité.values:
            mask.append(df.iloc[i]['Entité'])
            #pd.concat(df.iloc[i],df[df['Entité']==df.iloc[i]['Entité']])
            
    companies = companies.loc[companies['Entité'].isin(mask)]
    CAC40 = pd.read_csv('preprocessed_CAC40.csv',sep=',')
    CAC40 = CAC40.groupby('Name').sum('Volume').reset_index()
    CAC40.rename(columns = {'Unnamed: 0' : 'Valeur', 'Name' : 'Entité'},inplace = True)
    CAC40 = CAC40.sort_values('Valeur', ascending = False)
    CAC40 = CAC40[['Entité']]
    CAC40['Entité']= CAC40['Entité'].apply(lambda x : x.upper())
    CAC40.reset_index(inplace = True)
    CAC40.rename(columns = {'index' : 'Valeur'},inplace = True)
    for i in range(len(df)):
        if df.iloc[i]['Entité'] in CAC40.Entité.values:
            mask.append(df.iloc[i]['Entité'])
            #pd.concat(df.iloc[i],df[df['Entité']==df.iloc[i]['Entité']])
        
    CAC40 = CAC40.loc[CAC40['Entité'].isin(mask)]    
    for i in range(len(df)):
        if df.iloc[i]['Entité'] in companies.Entité.values:
            df['Entité'][i] = int(companies.loc[companies['Entité']==df.iloc[i]['Entité']]['Valeur'].values)
        elif df.iloc[i]['Entité'] in CAC40.Entité.values:
            df['Entité'][i] = int(CAC40.loc[CAC40['Entité']==df.iloc[i]['Entité']]['Valeur'].values)
        else :
            df['Entité'][i] = 0    
            
    def con_Scoring(x):
        if (x > 10) : 
            x = 1
        elif (x > 5) :
            x = 0.66 
        elif (x > 0) :
            x = 0.33
        else :
            x = 0
        return x
    
    df['Nb_Consultant'] = df['Nb_Consultant'].apply(lambda x : float(x))
    df['Nb_Consultant'] = df['Nb_Consultant'].apply(lambda x : con_Scoring(x))
    df['Nb_Consultant']
    
    def exp_Scoring(x):
        if (x > 10) : 
            x = 0.25
        elif (x > 5) :
            x = 0.5
        elif (x > 2) :
            x = 0.75 
        elif (x > 0) :
            x = 1
        else :
            x = 0
        return x
    
    df['Experience'] = df['Experience'].apply(lambda x : exp_Scoring(x))
    df['Salaire']=df['Salaire'].fillna(0)
    df['Salaire']=df['Salaire'].replace('',0)
    df['Salaire'] = df['Salaire'].apply(lambda x: int(x))    
    def sal_Scoring(x):
        if (x > 8000) : 
            x = 1
        elif (x > 5000) :
            x = 0.66
        elif (x > 3000) :
            x = 0.33
        else :
            x = 0
        return x
    
    df['Salaire'] = df['Salaire'].apply(lambda x : sal_Scoring(x))
    df['Salaire'] 
    poste = df[['Poste']]
    df.drop('Poste',axis=1,inplace=True)

    Lieu = df[['Lieu']]
    df.drop('Lieu',axis=1,inplace=True)
    normalized_df=(df-df.min())/(df.max()-df.min())
    normalized_df

    df = poste.join(normalized_df)                
    def lang_scoring(i,e,ne):
        for x in lang:
                if x in e:
                    lang.iloc[i][x] = lang.iloc[i][x]*2
                elif x in ne:
                    lang.iloc[i][x] = lang.iloc[i][x]*1
                else :
                    pass    
            
    for i in lang.index:
        if df.iloc[i]['Poste'] == 'DATA':
            lang_scoring(i,data_comp_E,data_comp_NE)
        elif df.iloc[i]['Poste'] == 'JAVA':
            lang_scoring(i,java_comp_E,java_comp_NE)
        elif df.iloc[i]['Poste'] == 'DEVOPS':
            lang_scoring(i,devops_comp_E,devops_comp_NE)
        else :
            lang_scoring(i,comp_E,comp_NE) 
            
    lang['lang_Score'] = lang.sum(axis=1)
    
    Score = lang['lang_Score']
    Score =(Score-Score.min())/(Score.max()-Score.min())
    normalized_df = normalized_df.join(Score)
    Lieu['Lieu'].dropna(inplace= True)
    nan_value = float("NaN")
    Lieu['Lieu'].replace("", nan_value, inplace=True)
    Lieu.dropna(subset = ["Lieu"], inplace=True)


    api_url = "https://api-adresse.data.gouv.fr/search/?q="

    def get_add(x):
        adr = x
        try:
            r = requests.get(api_url + urllib.parse.quote(adr))
            data = json.loads(r.content)
            return data.get('features')[0].get("geometry").get('coordinates')
        except (ValueError,IndexError):
            return 0
            print("Oops!  Invalid address")
    
       
        
        return data.get('features')[0].get("geometry").get('coordinates')
    
    Lieu['coord'] = Lieu['Lieu'].apply(lambda x : get_add(x))
    l = [42,2]
    Lieu = Lieu.applymap(lambda x: l if x == 0 else x)
    add_op = '29 Rue des Sablons, 75116 Paris'

    add_op = get_add(add_op)
    lat_op = add_op[1]
    lon_op = add_op[0]
    
     
    def distanceGPS(longB, latB):
        
        degree_to_rad = float(pi / 180.0)
        
        d_lat = (latB - lat_op) * degree_to_rad
        d_long = (longB - lon_op) * degree_to_rad
        a = pow(sin(d_lat / 2), 2) + cos(lat_op * degree_to_rad) * cos(latB * degree_to_rad) * pow(sin(d_long / 2), 2)
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        km = 6367 * c
        return km
     
    Lieu['distance'] = Lieu['coord'].apply(lambda x : distanceGPS(x[0],x[1]))
    
    def dist_score(x):
        if (x > 30) : 
            x = 0.33
        elif (x > 15) :
            x = 0.66 
        elif (x <= 15) :
            x = 1
        else :
            x = 0
        return x
    

    Lieu['dist_score'] = Lieu['distance'].apply(lambda x : dist_score(x))
    dist_score = Lieu[['dist_score']]
    normalized_df = normalized_df.join(dist_score)
    
    def Exp_coeff(x):
        x = x*coeff_experience
        return x    
    def En_coeff(x):
        x = x*coeff_entité
        return x
    def Del_coeff(x):
        x= x*coeff_délai
        return x
    def Con_coeff(x):
        x= x*coeff_consultant
        return x
    def Sal_coeff(x):
        x = x*coeff_salaire
        return x
    def Lang_coeff(x):
        x = x*coeff_langue
        return x
    def Dist_coeff(x):
        x = x*coeff_distance
        return x

    def get_score(x):
        x['Entité'] = x['Entité'].apply(lambda x : En_coeff(x))
        x['Nb_Consultant'] =  x['Nb_Consultant'].apply(lambda x : Con_coeff(x))
        x['lang_Score'] =  x['lang_Score'].apply(lambda x : Lang_coeff(x))
        x['dist_score'] =  x['dist_score'].apply(lambda x : Dist_coeff(x))
        x['Experience'] = x['Experience'].apply(lambda x : Exp_coeff(x))
        x['Salaire'] =  x['Salaire'].apply(lambda x : Sal_coeff(x))
        #x['Délai'] =  x['Délai'].apply(lambda x : Del_coeff(x))
        
    get_score(normalized_df)
    normalized_df['Score'] = normalized_df.sum(axis=1)
    nm=normalized_df.sort_values(by='Score',ascending=False)
    
    return nm
