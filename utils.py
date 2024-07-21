import re
    
    
def condition_clean(condition_str, lst_conditions):
    ''' 
    Function that clears the "Park Conditions" column using a list of adjectives that was extracted with spacy (nlp tool)
    receives the string of conditions, cleans it and compares it
    inputs:
        condition_str: string with feature from the "Park Condition" column
        lst_conditions: list of the adjectives extracted
    '''
    if condition_str:
        con_output = re.sub(r'\[.*?\]', '', condition_str)
        con_output = re.sub(r'\(.*?\)', '', con_output)
        con_output = re.sub(r'\{.*?\}', '', con_output)
        con_output = con_output.replace('/', ' ')
        con_output = con_output.replace("'", 'g')
        con_output = re.sub(r'[^\w\s]', '', con_output)
        con_output = con_output.lower().split()
        
        conditions = None
        for word in con_output:
            if word in lst_conditions:
                condition = word
                
        return condition    
        
    else:
        return None
        
        
def animal_clean(animal_input, lst_animals):
    ''' 
    This function cleans the animals by comparing them against a list of animals obtained from kaggle
    inputs:
        animal_input: string with the feature from "Other Animal Sightings" column
        lst_animas: list of animals from a .txt
    '''
    if animal_input:
                
        lst_animals = lst_animals[0].replace('\n', ' ')
        lst_animals = lst_animals.replace(')', '')
        lst_animals = lst_animals.replace('(', '').lower().split(",")
        
        word_list = re.split(r'[,\s]+', animal_input.lower())
        word_list = [re.sub(r'[^a-zA-Z]', '', word) for word in word_list]
      
        
        animal_output = []
        
        for word in word_list:
            # Apply simple rules to convert plural to singular
            if word.endswith('es'):
                singular_word = word[:-2]  
            elif word.lower().endswith('s'):
                singular_word = word[:-1]  
            else:
                singular_word = word  # No change needed
            
            if singular_word in lst_animals:
                animal_output.append(singular_word)
        
        animal_output = [words for words in animal_output if words]
        
        if len(animal_output) < 1:
            return None
        else:
            return animal_output
        
    else:
        return None

def litter_clean(litter_input):
    ''' 
    cleans the "Litter" column by taking 3 possible situations (some, abundant, none)
    intputs:
        litter_input: string with the column feature
    '''
    if litter_input:
        litter_lst = ['some', 'abundant', 'none']
        
        for word in litter_lst:
            if word in re.split(r'[,\s]+', litter_input.lower()):
                return word
        
        return None
    else:
        return None

def extract_weather(tw_input):
    ''' 
    extraxt the weather part from "Time & Weather" column taking in count the most common words used to desrcibe the weather
    intputs:
        tw_input: string with the feature from the column
        output: list of strings with the weather conditions
    '''
    weather_conditions = [
    "sunny", "cloudy", "overcast", "rainy", "stormy", "windy", "breezy",
    "chilly", "cold", "cool", "warm", "hot", "humid", "muggy", "dry",
    "wet", "foggy", "misty", "hazy", "clear", "frosty", "icy", "snowy",
    "blustery", "thunderous", "light", "heavy", "freezing", "sweltering", "scorching"
    ]
    if tw_input:
 
        weather_words = re.split(r'[,\s]+', tw_input.lower())
        weather_words = [re.sub(r'[^a-zA-Z]', '', word) for word in weather_words]
        
        weather_list = []
        
        for condition in weather_conditions:
            if condition in weather_words:
                weather_list.append(condition)
        
        if len(weather_list) < 1:
            return None
        else:
            return weather_list
            
    else:
        return None


def extract_temp(tw_input):
    ''' 
    extraxt the tempreature part from "Time & Weather" column by finding the "degree" patern in the string
    intputs:
        tw_input: string with the feature from the column
    output: integer of the tempreature
    '''
    if tw_input:
        pattern = rf"(\d+)\s+{'degrees'}"
        match = re.search(pattern, tw_input)

        if match:
            temp = int(match.group(1))    
        else:
            temp = None
        return temp
        
    else:
        return None  
        
    
def convert_furhighlight(fh_input):
    ''' 
    function that standardize the fur color highlights
    intputs:
        fh_input: the string feature from column     
    '''
    if fh_input:
        fh_output = fh_input.lower().split(",")
        return fh_output
    else:
        return None

    
def convert_activities2(activities_str, lst_activities):
    ''' 
    function that cleans and compare the Squirrel activities from "Activities" column with a list of common activities extracted with nlp (spacy)
    input:
        activities_str: string from the column Activities
        lst_activities: list of strings with all possible activities of the squirrels
    '''
    if activities_str:
        act_output = re.sub(r'\[.*?\]', '', activities_str)
        act_output = re.sub(r'\(.*?\)', '', act_output)
        act_output = re.sub(r'\{.*?\}', '', act_output)
        act_output = act_output.replace('/', ' ')
        act_output = act_output.replace("'", 'g')
        act_output = re.sub(r'[^\w\s]', '', act_output)
        act_output = act_output.lower().split()
        
        verbs = []
        for word in act_output:
            if word in lst_activities:
                verbs.append(word)
        
        
        if len(verbs) < 1 :
            return None
        else:
            verbs = ' '.join(verbs)
            return verbs
    else:
        return None
    
def load_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read().strip()
    return query