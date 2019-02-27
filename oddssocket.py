import websocket, json, requests, re, threading, time
import repo, random, string
class OddsSocket(threading.Thread):
    def __init__(self, token):
        self.started = False
        super().__init__(daemon=True)
        self.token = token
        self.uid = 2
        websocket.enableTrace(True)
        self.rn = random.randint(0, 1000)
        self.rid = ''.join(random.choice(string.ascii_lowercase + string.digits) for c in range(8))
        self.ws = websocket.WebSocketApp(f"wss://betting-async.gin.bet/sockjs/{self.rn}/{self.rid}/websocket",
                              on_message = lambda ws, message: self._on_message(ws, message),
                              on_error = lambda ws, error: self._on_error(ws, error),
                              on_close = lambda ws: self._on_close(ws),
                              on_open = lambda ws: self._on_open(ws))

    def run(self):
        self.ws.run_forever()

    def send(self, id):
        if not self.started:
            print("Socket not started yet")
            return
        msg3 = {"type":"start","payload":{"variables":{"offset":0,"limit":20,"marketStatuses":["ACTIVE","SUSPENDED"],"matchStatuses":["NOT_STARTED","SUSPENDED","LIVE"],"sportEventTypes":["MATCH"],"sportIds":["esports_call_of_duty","esports_counter_strike","esports_dota_2","esports_hearthstone","esports_heroes_of_the_storm","esports_league_of_legends","esports_overwatch","esports_starcraft","esports_world_of_tanks","esports_street_fighter_5","esports_vainglory","esports_warcraft_3","esports_rainbow_six","esports_rocket_league","esports_smite","esports_soccer_mythical","esports_halo","esports_crossfire","esports_battlegrounds","esports_fifa","esports_starcraft_1","esports_king_of_glory","esports_nba_2k18","esports_fortnite","esports_artifact"],"providerIds":[]},"extensions":{},"operationName":"GetMatchesByFilters","query":"query GetMatchesByFilters($offset: Int!, $limit: Int!, $search: String, $dateFrom: String, $dateTo: String, $providerIds: [Int!], $matchStatuses: [SportEventStatus!], $sportIds: [String!], $tournamentIds: [String!], $competitorIds: [String!], $marketStatuses: [MarketStatus!], $marketLimit: Int = 1, $dateSortAscending: Boolean, $sportEventTypes: [SportEventType!], $withMarketsCount: Boolean = true, $marketTypes: [Int!]) {\n  matches: sportEventsByFilters(offset: $offset, limit: $limit, searchString: $search, dateFrom: $dateFrom, dateTo: $dateTo, providerIds: $providerIds, matchStatuses: $matchStatuses, sportIds: $sportIds, tournamentIds: $tournamentIds, competitorIds: $competitorIds, marketStatuses: $marketStatuses, sportEventTypes: $sportEventTypes, dateSortAscending: $dateSortAscending, marketLimit: $marketLimit, marketTypes: $marketTypes) {\n    ...Match\n    marketsCount @include(if: $withMarketsCount)\n  }\n}\n\nfragment Match on SportEvent {\n  id\n  disabled\n  providerId\n  hasMatchLog\n  fixture {\n    ...MatchFixture\n  }\n  markets {\n    id\n    name\n    status\n    typeId\n    priority\n    specifiers {\n      name\n      value\n    }\n    odds {\n      id\n      name\n      value\n      isActive\n      status\n      competitorId\n    }\n  }\n}\n\nfragment MatchFixture on SportEventFixture {\n  score\n  title\n  status\n  type\n  startTime\n  sportId\n  liveCoverage\n  streams {\n    id\n    locale\n    url\n  }\n  tournament {\n    id\n    name\n    masterId\n    countryCode\n    logo\n    description\n    showTournamentInfo\n    prizePool\n    dateStart\n    dateEnd\n    isLocalizationOverridden\n  }\n  competitors {\n    id: masterId\n    name\n    type\n    homeAway\n    logo\n    templatePosition\n  }\n}\n"}}
        msg3["id"] = str(id)
        self.ws.send(json.dumps(msg3))

    def _on_message(self, ws, message):
        # print(message)
        if message == 'o':
            msg1 = {"uid":"1","method":"auth","params":{"token":self.token}}
            ws.send('[""]')
        
    def _on_error(self, ws, error):
        print("err: " + str(error))

    def _on_close(self, ws):
        print("### odds socket closed ###")

    def _on_open(self, ws):
        self.started = True
        print("Socket started")