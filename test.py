import websocket, json, requests, re, threading, time, html
import repo, urllib.parse
from websocket import create_connection
from multiprocessing.pool import ThreadPool

def _insert_match(match, counter=0):
    try:
        x = match["fixture"]
        tournament = x["tournament"]
        q = f"""if not EXISTS (SELECT 1 FROM tournament WHERE RawId='{tournament['id']}')
                                BEGIN
                                insert into tournament values ('{tournament['id']}', '{html.escape(tournament['name'])}', '{tournament['countryCode']}', '{urllib.parse.quote(tournament['logo'])}', '{html.escape(tournament['description'])}', {1 if tournament['showTournamentInfo'] else 0}, '{tournament['prizePool']}', '{tournament['dateStart'].split('T')[0]}', null, GETDATE())
                                select Scope_Identity() as Id
                                END
                                else
                                select Id from tournament WHERE RawId='{tournament['id']}'"""
        tid = repo.select(q)[0]["Id"]
        competitors = x["competitors"]
        home = competitors[0]
        hid = repo.select(f"""if not EXISTS (SELECT * FROM team WHERE RawId='{home['id']}')
                                begin
                                insert into team values ('{home['id']}', '{html.escape(home['name'])}', '{urllib.parse.quote(home['logo'])}', GETDATE())
                                select Scope_Identity() as Id
                                END
                                else
                                select Id from team WHERE RawId='{home['id']}'""")[0]["Id"]
        away = competitors[1]
        aid = repo.select(f"""if not EXISTS (SELECT * FROM team WHERE RawId='{away['id']}')
                                begin
                                insert into team values ('{away['id']}', '{html.escape(away['name'])}', '{urllib.parse.quote(away['logo'])}', GETDATE())
                                select Scope_Identity() as Id
                                end
                                else
                                select Id from team WHERE RawId='{away['id']}'""")[0]["Id"]

        h, a = x["score"].split(":")
        mid = repo.select(f"""if not Exists (SELECT * from match where RawId='{match['id']}')
                        begin
                            insert into match values ('{match['id']}', '{html.escape(x['title'])}', '{x['startTime'].split('+')[0]}','{x['status']}', {1 if x['liveCoverage'] else 0}, '{x['streams'][0]['url'] if x['streams'] else ''}', '{x['sportId']}', {h}, {a}, {hid}, {aid}, {tid}, GETDATE())
                            select Scope_Identity() as Id
                        end
                        else
                            select Id from match WHERE RawId='{match['id']}'""")[0]["Id"]

        markets = match["markets"]
        for market in markets:
            marketId = repo.select(f"""if not EXISTS (SELECT * FROM MARKET WHERE RawId='{market['id']}' and matchid = {mid})
                                begin
                                insert into market values ('{market['id']}', '{html.escape(market['name'])}', '{market['status']}', {market['typeId']}, {market['priority']}, {mid}, GETDATE())
                                select Scope_Identity() as Id
                                end
                                else
                                update market set status='{market['status']}' where RawId='{market['id']}' and matchid={mid}
                                select Id from market where RawId='{market['id']}' and matchid={mid}
                            """)[0]["Id"]
            odds = market["odds"]
            for o in odds:
                query = f"""update Odds set Value={o['value']}, IsActive= {1 if o['isActive'] else 0}, status='{o['status']}', modifiedon=GETDATE() where RawId = '{o['id']}' and marketid = {marketId}
                            if @@ROWCOUNT=0
                            insert into odds values ('{o['id']}', '{html.escape(o['name'])}', {o['value']}, {1 if o['isActive'] else 0}, '{o['status']}', '{o['competitorId']}', '{marketId}', GETDATE(), GETDATE())
                        """
                repo.execute(query)
    except Exception as ex:
        print(str(ex))
        print(q)
        print(f"Failed try ${counter + 1} times")
        raise
        _insert_match(match, counter+1)

def _insert_matches(matches):
    print(len(matches))
    pool = ThreadPool(processes=1)
    pool.map(_insert_match, (match for match in matches))
    pool.close()
    # for match in matches:
    #     _insert_match(match)

def _on_message(self, ws, message):
    data = json.loads(message)
    if "id" in data and int(data["id"]) >= 2 and data["type"] == "data":
        if "payload" in data:
            self._insert_matches(data["payload"]["data"]["matches"])
            
        else:
            print(data)

def _on_error(self, ws, error):
    print("err: " + error)

def _on_close(self, ws):
    print("### match socket closed ###")

def _on_open(self, ws):
    msg1 = {"type":"connection_init","payload":{"headers":{"X-Auth-Token":self.token},"X-Auth-Token":self.token}}
    msg2 = {"id": "1","type":"start","payload":{"variables":{"sportIds":["esports_call_of_duty","esports_counter_strike","esports_dota_2","esports_hearthstone","esports_heroes_of_the_storm","esports_league_of_legends","esports_overwatch","esports_starcraft","esports_world_of_tanks","esports_street_fighter_5","esports_vainglory","esports_warcraft_3","esports_rainbow_six","esports_rocket_league","esports_smite","esports_soccer_mythical","esports_halo","esports_crossfire","esports_battlegrounds","esports_fifa","esports_starcraft_1","esports_king_of_glory","esports_nba_2k18","esports_fortnite","esports_artifact"]},"extensions":{},"operationName":"GetSports","query":"query GetSports($sportIds: [String!]) {\n  sports(sportIds: $sportIds) {\n    id\n    name\n  }\n}\n"}}
    ws.send(json.dumps(msg1))
    ws.send(json.dumps(msg2))
    self.started = True
    print("Socket started")
    self.send(2)

if __name__ == "__main__":
    while True:
        try:
            resp = requests.get("https://ggbet.com/en/betting")
            token = re.search('token: "(.+?)",', resp.text)[1]
            print(f"Token is {token}")
            ws = create_connection("wss://betting-public-gql.gin.bet/graphql")
            msg1 = {"type":"connection_init","payload":{"headers":{"X-Auth-Token":token},"X-Auth-Token":token}}
            msg2 = {"id": "1","type":"start","payload":{"variables":{"sportIds":["esports_call_of_duty","esports_counter_strike","esports_dota_2","esports_hearthstone","esports_heroes_of_the_storm","esports_league_of_legends","esports_overwatch","esports_starcraft","esports_world_of_tanks","esports_street_fighter_5","esports_vainglory","esports_warcraft_3","esports_rainbow_six","esports_rocket_league","esports_smite","esports_soccer_mythical","esports_halo","esports_crossfire","esports_battlegrounds","esports_fifa","esports_starcraft_1","esports_king_of_glory","esports_nba_2k18","esports_fortnite","esports_artifact"]},"extensions":{},"operationName":"GetSports","query":"query GetSports($sportIds: [String!]) {\n  sports(sportIds: $sportIds) {\n    id\n    name\n  }\n}\n"}}
            ws.send(json.dumps(msg1))
            ws.send(json.dumps(msg2))
            msg3 = {"id": "2","type":"start","payload":{"variables":{"offset":0,"limit":20,"marketStatuses":["ACTIVE","SUSPENDED"],"matchStatuses":["NOT_STARTED","SUSPENDED","LIVE"],"sportEventTypes":["MATCH"],"sportIds":["esports_call_of_duty","esports_counter_strike","esports_dota_2","esports_hearthstone","esports_heroes_of_the_storm","esports_league_of_legends","esports_overwatch","esports_starcraft","esports_world_of_tanks","esports_street_fighter_5","esports_vainglory","esports_warcraft_3","esports_rainbow_six","esports_rocket_league","esports_smite","esports_soccer_mythical","esports_halo","esports_crossfire","esports_battlegrounds","esports_fifa","esports_starcraft_1","esports_king_of_glory","esports_nba_2k18","esports_fortnite","esports_artifact"],"providerIds":[]},"extensions":{},"operationName":"GetMatchesByFilters","query":"query GetMatchesByFilters($offset: Int!, $limit: Int!, $search: String, $dateFrom: String, $dateTo: String, $providerIds: [Int!], $matchStatuses: [SportEventStatus!], $sportIds: [String!], $tournamentIds: [String!], $competitorIds: [String!], $marketStatuses: [MarketStatus!], $marketLimit: Int = 1, $dateSortAscending: Boolean, $sportEventTypes: [SportEventType!], $withMarketsCount: Boolean = true, $marketTypes: [Int!]) {\n  matches: sportEventsByFilters(offset: $offset, limit: $limit, searchString: $search, dateFrom: $dateFrom, dateTo: $dateTo, providerIds: $providerIds, matchStatuses: $matchStatuses, sportIds: $sportIds, tournamentIds: $tournamentIds, competitorIds: $competitorIds, marketStatuses: $marketStatuses, sportEventTypes: $sportEventTypes, dateSortAscending: $dateSortAscending, marketLimit: $marketLimit, marketTypes: $marketTypes) {\n    ...Match\n    marketsCount @include(if: $withMarketsCount)\n  }\n}\n\nfragment Match on SportEvent {\n  id\n  disabled\n  providerId\n  hasMatchLog\n  fixture {\n    ...MatchFixture\n  }\n  markets {\n    id\n    name\n    status\n    typeId\n    priority\n    specifiers {\n      name\n      value\n    }\n    odds {\n      id\n      name\n      value\n      isActive\n      status\n      competitorId\n    }\n  }\n}\n\nfragment MatchFixture on SportEventFixture {\n  score\n  title\n  status\n  type\n  startTime\n  sportId\n  liveCoverage\n  streams {\n    id\n    locale\n    url\n  }\n  tournament {\n    id\n    name\n    masterId\n    countryCode\n    logo\n    description\n    showTournamentInfo\n    prizePool\n    dateStart\n    dateEnd\n    isLocalizationOverridden\n  }\n  competitors {\n    id: masterId\n    name\n    type\n    homeAway\n    logo\n    templatePosition\n  }\n}\n"}}
            ws.send(json.dumps(msg3))
            count = 0
            while True:
                data = json.loads(ws.recv())
                if "id" in data and int(data["id"]) >= 2:
                    print(data["id"])
                    _insert_matches(data["payload"]["data"]["matches"])
                    break
            ws.close()
            time.sleep(1)
        except Exception as ex:
            print(str(ex))
            print("retry")
            time.sleep(1)