from scrape import scrape_ottoneu
from pymongo import MongoClient, UpdateOne
import os
from pandas import DataFrame

client = MongoClient(host=os.environ.get("ATLAS_URI"))
ottoneu_db = client.ottoneu


def main():
    scraper = scrape_ottoneu.Scrape_Ottoneu("FirefoxURL")
    leagues: DataFrame = scraper.get_all_leagues_by_format(OPL=False)

    update_leagues = []
    print(f"League list  = {leagues.index.to_list()}")
    for idx, league in leagues.iterrows():
        try:
            league_dict = dict()
            league_dict["format_name"] = league["Game Type"]
            league_dict["format"] = process_game_type(league["Game Type"])

            update_leagues.append(
                UpdateOne({"_id": idx}, {"$set": league_dict}, upsert=True)
            )
        except Exception as e:
            print(f"Exception for league {idx}")
            print(e)

    # print(update_leagues[0:5])

    if update_leagues:
        try:
            leagues_col = ottoneu_db.leagues
            leagues_col.bulk_write(update_leagues, ordered=False)
        except Exception as e:
            print(e)


def process_game_type(game_type: str) -> int:
    match game_type:
        case "Ottoneu Classic (4x4)":
            return 1
        case "Old School (5x5)":
            return 2
        case "FanGraphs Points":
            return 3
        case "SABR Points":
            return 4
        case "H2H FanGraphs Points":
            return 5
        case "H2H SABR Points":
            return 6
        case _:
            raise AttributeError(f"Invalid game type passed {game_type}")


if __name__ == "__main__":
    main()
