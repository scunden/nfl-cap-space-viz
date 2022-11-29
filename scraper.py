from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import re
import pickle
import logging

logging.basicConfig(
    level=logging.DEBUG, format='%(asctime)s %(message)s', 
    handlers=[
        logging.FileHandler("scraping_logs.log"),
        logging.StreamHandler()
    ],
    datefmt='%m/%d/%Y %I:%M:%S %p')

MAIN = 'https://www.spotrac.com/nfl/'

def get_teams_cap_url():
    
    response = requests.get(MAIN)
    soup = BeautifulSoup(response.text, 'html.parser')
    urls = []
    
    logging.info('Scraping team URLs...')
    for a in soup.find_all("a", href=True):
        if a['href'].endswith('/cap/') and 'nfl' in a['href'] and a['href']!='https://www.spotrac.com/nfl/cap/':
            logging.debug('Appending {}'.format(a['href']))
            urls.append(a['href'])

    urls = list(set(urls))
    logging.info('Scraping team URLs done.')

    return urls

def scrape_player_details(soup, year, team):
    logging.debug('Scraping player details for the {} {}...'.format(year, team))
    df = pd.read_html(str(soup.find_all("table")[0]))[0]
    df['Year'] = year
    df['Team'] = team
    active = [x for x in df.columns if 'Active' in x][0]
    df.rename({active:'Player Name'}, axis=1, inplace=True)
    if 'Cap Hit.1' in df.columns:
        df.drop(['Cap Hit.1'], axis=1, inplace=True)
    return df

def scrape_team_details(soup, year, team):
    logging.debug('Scraping team details for the {} {}...'.format(year, team))
    tag = soup.find_all('section',{'class':'module-singles xs-hide'})[0]
    metrics = [x.text.strip(':') for x in tag.find_all('span',{'class':'info'})]
    values = [float(x.text.replace('$','').replace(',','')) if x.text!= '-' else 0 for x in tag.find_all('a') ]
    data = {metrics[i]:[values[i]] for i in range(len(metrics))}
    data.update({'Team':[team],'Year':[year]})
    return pd.DataFrame(data= data)

def classify_positions(df):
    off = ['LT', 'WR', 'RT',  'G', 'RB', 'QB', 'C', 'TE', 'T', 'FB','OL']

    df['Position Level 1'] = np.where(df['Pos.'].isin(off),"Offense","Defense & ST")
    df['Position Level 2'] = np.where(df['Pos.'].isin(['LT','RT','OL','C','G','T']),"OL",df['Pos.'])
    df['Position Level 2'] = np.where(df['Pos.'].isin(['RB','FB']),"HB",df['Position Level 2'])
    df['Position Level 2'] = np.where(df['Pos.'].isin(['CB','SS','S','FS']),"DB",df['Position Level 2'])
    df['Position Level 2'] = np.where(df['Pos.'].isin(['ILB','OLB','LB']),"LB",df['Position Level 2'])
    df['Position Level 2'] = np.where(df['Pos.'].isin(['DE','DT']),"DL",df['Position Level 2'])
    df['Position Level 2'] = np.where(df['Pos.'].isin(['K','P','LS']),"ST",df['Position Level 2'])

    df.rename({'Pos.':'Position Level 3'}, axis=1, inplace=True)

    df['Position Level 3'] = np.where(df['Position Level 3'].isin(['RT','LT','T']),"T",df['Position Level 3'])

    return df

def scrape_data():
    teams_url = get_teams_cap_url()
    player_details = pd.DataFrame()
    team_details_22 = pd.DataFrame()
    team_details_23 = pd.DataFrame()

    for url in teams_url:
        for year in [2022, 2023]:
            url = url+'2023/' if year == 2023 else url
            team = re.search('nfl/(.*)/cap/', url).group(1).replace('-',' ').title()
            logging.info('Scraping the {} {}...'.format(year, team))

            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            team_detail = scrape_team_details(soup, year, team)
            player_detail = scrape_player_details(soup, year, team)

            player_details = pd.concat([player_details, player_detail])
            if year == 2023:
                team_details_23 = pd.concat([team_details_23, team_detail])
            else:
                team_details_22 = pd.concat([team_details_22, team_detail])

    player_details = classify_positions(player_details)  
    player_details = player_details.loc[~player_details['Player Name'].str.contains('Active Roster')]

    return player_details, team_details_22, team_details_23

def main():
    player_details, team_details_22, team_details_23 = scrape_data()

    logging.info('Exporting player_details')
    with open('data/player_details.pickle', 'wb') as handle:
        pickle.dump(player_details, handle, protocol=pickle.HIGHEST_PROTOCOL)

    logging.info('Exporting team_details_22')
    with open('data/team_details_22.pickle', 'wb') as handle:
        pickle.dump(team_details_22, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
    logging.info('Exporting team_details_23')
    with open('data/team_details_23.pickle', 'wb') as handle:
        pickle.dump(team_details_23, handle, protocol=pickle.HIGHEST_PROTOCOL)

# with open('filename.pickle', 'rb') as handle:
#     b = pickle.load(handle)

if __name__=='__main__':
    main()