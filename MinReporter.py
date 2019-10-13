from datetime import datetime
from tqdm import tqdm
from typing import Generator, Dict, List
import asyncio
import aiohttp
import urllib.parse


class BungieApi:
    def __init__(self, api_key: str, base_url: str = "https://www.bungie.net/Platform/") -> None:
        self.HEADERS = {"X-API-Key": api_key}
        self.BASE_URL = base_url
        self.loop = asyncio.get_event_loop()

    async def async_send(self, path: str, retries=5, *args, **kwargs) -> dict:
        """
        Send a request by supplying a relative path to the base path.
        Retries argument is a recursion to retry requests on the odd chance daddy Cloudflare cockblocks our request
        Because this is an async send, you will need to use a loop to complete it. Ideally you should use async_query
        or batch_query.
        """
        url: str = self.BASE_URL + urllib.parse.quote(path)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=self.HEADERS, *args, **kwargs) as response:
                try:
                    data = await response.json()
                    return data

                except aiohttp.client.ContentTypeError:
                    if retries > 0:
                        await asyncio.sleep(100)
                        return await self.async_send(path, retries=retries-1, *args, **kwargs)

                    await print(response.content)
                    raise ValueError("Encountered unexpected response. Aborting...")

    def async_query(self, path: str, *args, **kwargs):
        """
        Wrapper for async_send on a single request.
        """
        return self.loop.run_until_complete(self.async_send(path, *args, **kwargs))

    def batch_query(self, generator: Generator):
        """
        Wrapper for async_send (but also any async generator) for batch requests.
        """
        return self.loop.run_until_complete(asyncio.gather(*generator))


class Clan:
    def __init__(self, clan_name: str, apikey, clan_id: str = "") -> None:
        self.APIKEY = apikey
        self.bungo: BungieApi = BungieApi(self.APIKEY)
        self.clan_name: str = clan_name
        if clan_id:
            self.clan_id = clan_id

        else:
            response = self.bungo.async_query(f"GroupV2/Name/{self.clan_name}/1/")
            self.clan_id = response['Response']['detail']['groupId']

        self.members = self.get_members()

    # TODO: Can I make Member creation async by using a generator and batch_query?
    def get_members(self) -> dict:
        """
        Fetch clan members, then parse out relevant info and initialize each member as a Member()
        Members are stored in a dict with the member_id as the key and the class as the value. Maybe this is dumb, idk
        """
        tqdm.write("This will populate all clan members and all characters for each member. This takes a minute, so grab a drink.")
        response = self.bungo.async_query(f"GroupV2/{self.clan_id}/members")
        raw_members = response['Response']['results']

        members: Dict[str, Member] = dict()
        for member_json in tqdm(raw_members):
            membership_id = member_json['destinyUserInfo']['membershipId']
            membership_type = member_json['destinyUserInfo']['membershipType']
            display_name = member_json['destinyUserInfo']['displayName']
            join_date = member_json['joinDate']

            # Initializing a member is slow because each member fetches characters and initializes the character classes
            members[membership_id] = Member(membership_id, display_name, membership_type, join_date, self)

        return members


class Member:
    def __init__(self, member_id: str, display_name: str, member_type: str, join_date: str, clan: Clan) -> None:
        self.clan: Clan = clan
        self.bungo_stats: BungieApi = BungieApi(self.clan.APIKEY, base_url='https://stats.bungie.net/Platform/')
        self.member_id: str = member_id
        self.display_name: str = display_name
        self.member_type = member_type
        self.join_date = join_date
        self.private = False
        self.characters = self.get_characters()

    def get_characters(self) -> dict:
        """
        See Clan.get_members
        """
        characters: Dict[str, Character] = dict()
        try:
            response = self.clan.bungo.async_query(f"Destiny2/{self.member_type}/Profile/{self.member_id}/",
                                             params={'components': 'Profiles,Characters'})

            for character_id in response['Response']['profile']['data']['characterIds']:
                characters[character_id] = Character(character_id, self)

            return characters

        except:
            return dict()

    async def players_in_activity(self, activity_id) -> list:
        """
        Given an ativity ID, retrieve all players IDs from that activity
        """
        response = await self.bungo_stats.async_send(f"Destiny2/Stats/PostGameCarnageReport/{activity_id}/")
        activity_players = list()
        for entry in response['Response']['entries']:
            try:
                activity_players.append(entry['player']['destinyUserInfo']['membershipId'])

            except KeyError:
                tqdm.write(entry['player'])

        return activity_players

    def recent_players_and_activities(self, search_depth=50) -> None:
        """
        Accumulates the X most recent activities from each character as specified by search_depth.
        Then for each activity retrieves all players.
        Used to view data per. member rather than per. character
        """
        self.activities: Dict[str, str] = dict() # { activity_id : timestamp }
        self.recent_players: List[str] = list() # [ *player_id ]

        get_recent_activities = (character.recent_activities(activity_count=search_depth) for character_id, character in self.characters.items())
        for activity in self.clan.bungo.batch_query(get_recent_activities):
            self.activities.update(activity)

        get_recent_players = (self.players_in_activity(activity) for activity in self.activities)
        for activity_players in self.clan.bungo.batch_query(get_recent_players):
            self.recent_players += activity_players

    def recent_clanmates(self, activity_count=50) -> Dict[str, Dict[str, dict]]:
        """
        Builds off of recent_players_and_activities().
        Cross-references each recent player ID against clan member IDs and counts the number of occurrences.
        """
        if not hasattr(self, "recent_players"):
            self.recent_players_and_activities(search_depth=activity_count)

        player_relationships: Dict[str, Dict[str, dict]] = dict() # { member_id : { 'display_name' : str, 'times_played' : int }}
        for player in self.recent_players:
            if player == self.member_id: # Gotta exclude ourselves
                continue

            if player in self.clan.members:
                if player not in player_relationships:
                    player_relationships[player] = {
                        "display_name": self.clan.members[player].display_name,
                        "times_played": 1}

                else:
                    player_relationships[player]['times_played'] += 1

        return player_relationships


class Character:
    def __init__(self, character_id: str, member: Member) -> None:
        self.character_id: str = character_id
        self.member: Member = member

    async def recent_activities(self, activity_count=50) -> Dict[str, datetime]:
        """
        Lookup up X most recent activities based on activity_count then attempt to get the activity ID and timestamp.
        This is also our test to see if the player is set to private.
        This is async because we use it in a generator later, so it has to be executed in an event loop.
        """
        response = await self.member.clan.bungo.async_send(
            f"Destiny2/{self.member.member_type}/Account/{self.member.member_id}/Character/{self.character_id}/Stats/Activities/",
            params={'count': activity_count, 'mode': 0, 'page': 0})

        activities: Dict[str, datetime] = dict()
        # Don't you just love sanity checks :rage:
        if 'ErrorStatus' in response:
            status = response['ErrorStatus'] # If I don't use this placeholder variable then the check fails :shrug:
            if status == 'DestinyPrivacyRestriction':
                self.member.private = True

        if 'Response' in response:
            if response['Response']:
                for activity in response['Response']['activities']:
                    activities[activity['activityDetails']['instanceId']] = datetime.strptime(activity['period'], "%Y-%m-%dT%H:%M:%SZ")

        return activities


if __name__ == '__main__':
    import argparse
    import statistics
    import time
    from pprint import pprint


    parser = argparse.ArgumentParser(description="A tool to map what clan member play habits. "
                                                 "Specifically how often they play with other clan members.")
    parser.add_argument("apikey", help="Your Bungie API key (https://www.bungie.net/en/Application)")
    args = parser.parse_args()

    bcg = Clan("Box Canyon Guardians", args.apikey)
    times = list()
    player_relationships = dict()
    for m_id, member in bcg.members.items():
        a = time.time()
        print(f"\ngathering clan participation for {member.display_name} ({m_id})")
        print(f"Joined: {member.join_date}")
        member_relationships = member.recent_clanmates(activity_count=50)
        if member.private:
            print("PROFILE IS PRIVATE")

        try:
            oldest_activity = min([timestamp for activity_id, timestamp in member.activities.items()])
            oldest_activity = datetime.strftime(oldest_activity, '%Y-%m-%dT%H:%M:%SZ')

        except (ValueError, TypeError):
            oldest_activity = "No Activities Present"

        print(f"Oldest Activity: {oldest_activity}")

        player_relationships[m_id] = {member.display_name : member_relationships}
        times.append(time.time() - a)
        pprint(member_relationships)

    print(statistics.mean(times))
