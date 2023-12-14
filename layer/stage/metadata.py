"""Holds metadata about ACS.

Somewhat copy pasted from:
https://github.com/socialfoundations/folktables/blob/main/folktables/load_acs.py
"""

STATE_CODE_MAP = {'AL': '01', 'AK': '02', 'AZ': '04', 'AR': '05', 'CA': '06',
                'CO': '08', 'CT': '09', 'DE': '10', 'FL': '12', 'GA': '13',
                'HI': '15', 'ID': '16', 'IL': '17', 'IN': '18', 'IA': '19',
                'KS': '20', 'KY': '21', 'LA': '22', 'ME': '23', 'MD': '24',
                'MA': '25', 'MI': '26', 'MN': '27', 'MS': '28', 'MO': '29',
                'MT': '30', 'NE': '31', 'NV': '32', 'NH': '33', 'NJ': '34',
                'NM': '35', 'NY': '36', 'NC': '37', 'ND': '38', 'OH': '39',
                'OK': '40', 'OR': '41', 'PA': '42', 'RI': '44', 'SC': '45',
                'SD': '46', 'TN': '47', 'TX': '48', 'UT': '49', 'VT': '50',
                'VA': '51', 'WA': '53', 'WV': '54', 'WI': '55', 'WY': '56',
                'PR': '72'}

STATE_LIST = list(STATE_CODE_MAP.keys())

# list of years to download.
# TODO: ACS goes back to 2014, but data definitions changed slightly at 2017
YEARS = [2021, 2022]

# data we will load TODO: load more
INPUT_LIST = [
    # METADATA
    "REGION", # region
    "DIVISION", # area
    "ST", # state
    "PUMA", # area code (combine with state)
    "PWGTP", # weight, number of people this point represents
    # AGE
    "AGEP", # persons age, top coded
    # GENDER
    "FER", # recently gave birth
    "SEX", # binary coded
    # RACE
    "RAC1P", # simple race code. 2P and 3P are more specific.
    "RACNUM", # simple multi-racial code

    # INCOME (y)
    "OIP", # other income
    "PAP", # public income
    "RETP", # retirement
    "WAGP", # wage or salary income. needs adjinc
    "WKHP", # hours worked
    "WKWN", # weeks worked last 12 months
    "PERNP", # total earnings
    "PINCP", # total income
    "ADJINC", # adjustment factor

    # EDUCATION
    "SCH", # in school
    "SCHG", # grade
    "SCHL", # edu attainment

    # FAMILY STATUS
    "MAR", # marital status
    "MARHYP", # last year married, na if not or <15
    "MSP", # spousal status
    "PAOC", # own children, female coded.

    # VETERAN STATUS
    "MIL", # military service. 1/2/3/4. bunch of interesting MLP codes.
    "MLPA", # server after 9/11
    "MLPB", # persian gulf

    # CITIZENSHIP & LANGUAGE
    "ENG", # english speaking
    "YOEP", # year of entry
    "NATIVITY", # native or foreign born
    "NOP", # nativity of parent
    "WAOB", # area of birth

    # DISABILITY
    "DEAR", # hearing
    "DEYE", # vision
    "DOUT", # indep living, has nans
    "DPHY", # ambluatory difficulty
    "DRAT", # veteran disability rating (1-6, needs map)
    "DIS", # disability recode

    # HEALTH CARE
    "HINS1", # employer/union
    "HINS2", # direct
    "HINS3", # medicare
    "HINS4", # medicaid
    "HINS5", # tricare / military
    "HINS6", # VA
    "HINS7", # Indian

    # CATEGORICAL CODES
    "ANC1P", # primary ancestry
    "ANC2P", # secondary ancestry
    "FOD1P", # primary field of degree
    "OCCP", # occupation code
]
