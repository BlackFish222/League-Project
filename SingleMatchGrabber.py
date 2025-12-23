from Config import api_key
import json
import os
import requests

#Setup and the player you want so save the most recent match for
summonerName = "Braumadan"
summonerTagline = "5420"
api_url = "https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/"+summonerName+"/"+ summonerTagline
fullKey = api_url + "?api_key=" + api_key

out = requests.get(fullKey)
playerInfo = out.json()

puuid = playerInfo['puuid']
matchesAPI = "https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/" + puuid +"/ids?start=0&count=20"
apiUrlMatches = matchesAPI  + "&api_key=" + api_key
matchesPlayed = requests.get(apiUrlMatches)
matches = matchesPlayed.json()

selectedGame = matches[0] # most recent game
matchTimeline = "https://americas.api.riotgames.com/lol/match/v5/matches/"+ selectedGame +"/timeline"
matchTimelineAPI = matchTimeline +"?api_key=" + api_key
mt = requests.get(matchTimelineAPI)
timeline = mt.json()

filePath = "Data/Raw/MatchTimeline"
try:
    with open(filePath, 'w') as json_file:
        json.dump(timeline,json_file)
    print("Data saved")
except IOError as e:
    print("failed to save", e)