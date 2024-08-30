sc_team_to_fg_team = {
    'ATL' : 'Braves',
    'CHC' : 'Cubs',
    'CLE' : 'Guardians',
    'COL' : 'Rockies',
    'KC'  : 'Royals',
    'LAD' : 'Dodgers',
    'MIA' : 'Marlins',
    'NYY' : 'Yankees',
    'OAK' : 'Athletics',
    'PIT' : 'Pirates',
    'SEA' : 'Mariners',
    'STL' : 'Cardinals',
    'TEX' : 'Rangers',
    'TOR' : 'Blue Jays',
    'WSH' : 'Nationals',
    'MIL' : 'Brewers',
    'MIN' : 'Twins',
    'AZ'  : 'Diamondbacks',
    'NYM' : 'Mets',
    'BOS' : 'Red Sox',
    'PHI' : 'Phillies',
    'CIN' : 'Reds',
    'LAA' : 'Angels',
    'CWS' : 'White Sox',
    'SD'  : 'Padres',
    'DET' : 'Tigers',
    'TB'  : 'Rays',
    'HOU' : 'Astros',
    'BAL' : 'Orioles',
    'SF'  : 'Giants'
}

def get_fg_from_sc_team(sc_team:str) -> str:
    return sc_team_to_fg_team.get(sc_team, None)

normalMap = {'À': 'A', 'Á': 'A', 'Â': 'A', 'Ã': 'A', 'Ä': 'A',
             'à': 'a', 'á': 'a', 'â': 'a', 'ã': 'a', 'ä': 'a', 'ª': 'A',
             'È': 'E', 'É': 'E', 'Ê': 'E', 'Ë': 'E',
             'è': 'e', 'é': 'e', 'ê': 'e', 'ë': 'e',
             'Í': 'I', 'Ì': 'I', 'Î': 'I', 'Ï': 'I',
             'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
             'Ò': 'O', 'Ó': 'O', 'Ô': 'O', 'Õ': 'O', 'Ö': 'O',
             'ò': 'o', 'ó': 'o', 'ô': 'o', 'õ': 'o', 'ö': 'o', 'º': 'O',
             'Ù': 'U', 'Ú': 'U', 'Û': 'U', 'Ü': 'U',
             'ù': 'u', 'ú': 'u', 'û': 'u', 'ü': 'u',
             'Ñ': 'N', 'ñ': 'n',
             'Ç': 'C', 'ç': 'c',
             '§': 'S',  '³': '3', '²': '2', '¹': '1'}

def normalize(value:str) -> str:
    """Function that removes most diacritics from strings and returns value in all caps"""
    normalize = str.maketrans(normalMap)
    val = value.translate(normalize)
    return val.upper()