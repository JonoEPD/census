"""Define inputs based on raw data.

TODO: figure out how to process metadata somehow. """

INPUT_LIST = [
    # HEIRARCHY
    "REGION", # region
    "DIVISION", # area
    "ST", # state
    "PUMA", # area code (combine with state)
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
    "POAC", # own children, female coded.

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
    "BPHY", # ambluatory difficulty
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
