import json
import requests
import pandas as pd
us_states_and_capitals = {
    "Alabama": "Montgomery",
    "Alaska": "Juneau",
    "Arizona": "Phoenix",
    "Arkansas": "Little Rock",
    "California": "Sacramento",
    "Colorado": "Denver",
    "Connecticut": "Hartford",
    "Delaware": "Dover",
    "Florida": "Tallahassee",
    "Georgia": "Atlanta",
    "Hawaii": "Honolulu",
    "Idaho": "Boise",
    "Illinois": "Springfield",
    "Indiana": "Indianapolis",
    "Iowa": "Des Moines",
    "Kansas": "Topeka",
    "Kentucky": "Frankfort",
    "Louisiana": "Baton Rouge",
    "Maine": "Augusta",
    "Maryland": "Annapolis",
    "Massachusetts": "Boston",
    "Michigan": "Lansing",
    "Minnesota": "Saint Paul",
    "Mississippi": "Jackson",
    "Missouri": "Jefferson City",
    "Montana": "Helena",
    "Nebraska": "Lincoln",
    "Nevada": "Carson City",
    "New Hampshire": "Concord",
    "New Jersey": "Trenton",
    "New Mexico": "Santa Fe",
    "New York": "Albany",
    "North Carolina": "Raleigh",
    "North Dakota": "Bismarck",
    "Ohio": "Columbus",
    "Oklahoma": "Oklahoma City",
    "Oregon": "Salem",
    "Pennsylvania": "Harrisburg",
    "Rhode Island": "Providence",
    "South Carolina": "Columbia",
    "South Dakota": "Pierre",
    "Tennessee": "Nashville",
    "Texas": "Austin",
    "Utah": "Salt Lake City",
    "Vermont": "Montpelier",
    "Virginia": "Richmond",
    "Washington": "Olympia",
    "West Virginia": "Charleston",
    "Wisconsin": "Madison",
    "Wyoming": "Cheyenne"
    }
def extract_data(city):
# API endpoint and key
    API_KEY = "fedd02a3eddd57ec04138f8671cb7e90"
    city = city
    API_URL = "https://api.openweathermap.org/data/2.5/forecast"
    
    # Parameters for the API request
    params = {
        "q":city,
        "appid":API_KEY,
        "units":"metric"
    }
    cleaned_data = []
    
    response = requests.get(API_URL,params)
    data = response.json()
    for i in data['list']:
        dataitem = {}
        dataitem['date'] = i['dt_txt'].split(" ")[0]
        dataitem['time'] = i['dt_txt'].split(" ")[1]
        dataitem['avg_temp'] = i['main']['temp']
        dataitem['max_temp'] = i['main']['temp_max']
        dataitem['min_temp'] = i['main']['temp_min']
        dataitem['weather'] = i['weather'][0]['main']
        dataitem['wind_speed'] = i['wind']['speed']
        dataitem['humidity'] = i['main']['humidity']
        cleaned_data.append(dataitem.copy())
    # for i in cleaned_data:
    #     print(i)
    city_data = pd.DataFrame(cleaned_data)
    city_data.to_csv(f"./outputs/{city}.csv",index = False,encoding='UTF-8')

if __name__ == "__main__":
    for i in us_states_and_capitals:
        extract_data(i)