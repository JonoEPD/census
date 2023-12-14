"""Holds metadata about ACS.

Somewhat copy pasted from:
https://github.com/socialfoundations/folktables/blob/main/folktables/load_acs.py
"""
import typing

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
CATALOG = "data" # change to rest to use rest
METADATA_LIST = [] # FIXME: stub to stop complaining
DATA_PATH = "/home/iceberg/data/" # location of iceberg data
APP_PATH = "/home/iceberg/app"

class Metadata:
    """Define some basic information about processing a class."""

    def __init__(self, column: str, comment: str, iceberg_type="int", **kwargs):
        self.column = column.lower()
        self.comment = comment
        self.iceberg_type = iceberg_type
        self.processing_sql = kwargs.get("fn", self.identity_fn)
        self.nullable = kwargs.get("nullable", False)

    def get_select_fragment(self):
        select = self.column
        if self.nullable:
            select = f"COALESCE({select}, NULL)"
        query = self.processing_sql(select)
        return self.wrap_with_cast(query)

    def wrap_with_cast(self, query):
        """Wrap query with cast but keep name the same."""
        return f"CAST ({query} AS {self.iceberg_type}) {self.column}"

    def identity_fn(self, select):
        return f"{select}" # mark not nullable

# queries for bronze census. move to table folder
def generate_bronze_query(metadatas):
    s = f"SELECT \ncast(YEAR as int) year" # comma management        
    for m in metadatas[1:]:
        s += f",\n{m.get_select_fragment()}"
    s += f"\nFROM {CATALOG}.stage.census"
    s += "\nWHERE AGEP >= 18"
    return s

def comment_queries(metadatas):
    queries = []
    for m in metadatas[1:]:
        queries.append(f"""
        ALTER TABLE {CATALOG}.bronze.census
        ALTER COLUMN {m.column} COMMENT '{m.comment}'""")
    return queries

def generate_bronze_df(spark, metadatas):
    query = generate_bronze_query(metadatas)
    return spark.sql(query)

METADATA_LIST = [
    # METADATA
    #Metadata("YEAR", "synthetically created YEAR column"),
    Metadata("REGION", "region"),
    Metadata("DIVISION", "area"),
    Metadata("ST", "state"),
    Metadata("PUMA", "area code (combine with state)"),
    Metadata("PWGTP", "weight, number of people this point represents"),
    # AGE
    Metadata("AGEP", "persons age, top coded"),
    # SEX ( TODO: add FER)
    Metadata("SEX", "binary coded"),
    # RACE
    Metadata("RAC1P", "simple race code. 2P and 3P are more specific."),
    Metadata("RACNUM", "simple multi-racial code"),
    # INCOME (y)
    Metadata("OIP", "other income"), # TODO: get rid of bbbbbb
    Metadata("PAP", "public income"), # TODO: get rid of bbb
    Metadata("RETP", "retirement"),
    Metadata("WAGP", "wage or salary income. needs adjinc"),
    Metadata("WKHP", "hours worked"),
    Metadata("WKWN", "weeks worked last 12 months", nullable=True),
    Metadata("PERNP", "total earnings"),
    Metadata("PINCP", "total income"),
    Metadata("ADJINC", "adjustment factor, last 6 digits are after decimal"),
    # EDUCATION
    Metadata("SCH", "in school"),
    Metadata("SCHG", "grade"),
    Metadata("SCHL", "educational attainment"),
    # FAMILY STATUS
    Metadata("MAR", "marital status"),
    Metadata("MARHYP", "last year married, na if not or <15", nullable=True),
    Metadata("MSP", "spousal status"),
    Metadata("PAOC", "own children, coding is female specific", nullable=True),
    # VETERAN STATUS
    Metadata("MIL", "military service. 1/2/3/4. bunch of interesting MLP codes."),
    Metadata("MLPA", "served after 9/11", nullable=True),
    Metadata("MLPB", "served during persian gulf war", nullable=True),
    # CITIZENSHIP & LANGUAGE
    Metadata("ENG", "english speaking", nullable=True),
    Metadata("YOEP", "year of entry to USA"),
    Metadata("NATIVITY", "native to USA or foreign born"),
    Metadata("WAOB", "area of birth"),
    # DISABILITY
    Metadata("DEAR", "hearing disability"),
    Metadata("DEYE", "vision disability"),
    Metadata("DOUT", "living, has nans"),
    Metadata("DPHY", "ambluatory difficulty"),
    Metadata("DRAT", "veteran disability rating (1-6, needs map)", nullable=True),
    Metadata("DIS", "disability recode"),
    # HEALTH CARE
    Metadata("HINS1", "employer/union"),
    Metadata("HINS2", "direct"),
    Metadata("HINS3", "medicare"),
    Metadata("HINS4", "medicaid"),
    Metadata("HINS5", "tricare / military"),
    Metadata("HINS6", "VA"),
    Metadata("HINS7", "Indian"),
    # CATEGORICAL CODES
    Metadata("ANC1P", "primary ancestry"),
    Metadata("ANC2P", "secondary ancestry"),
    Metadata("FOD1P", "primary field of degree"),
    Metadata("OCCP", "occupation code"),
]

INPUT_LIST = [ m.column for m in METADATA_LIST]

SILVER_IDENT_COLS = [ "id", "year"]