"""Script to download ACS data for the listed years."""

import metadata
import folktables

def sync_acs_data(survey_year, states=metadata.STATE_LIST, horizon='1-Year', survey='person') -> None:
    """Idempodent download of files.

    Relies on local file structure: example data/2021/'1-Year'/*.csv
    Python usually dies even though the download worked due to async implementation bug.
    """
    data_source = folktables.ACSDataSource(survey_year=survey_year, horizon=horizon, survey=survey)
    _ = data_source.get_data(states=states, download=True)

def get_state(survey_year, state):
    """Return all data for state in local env."""
    data_source = folktables.ACSDataSource(survey_year=survey_year, horizon='1-Year', survey='person')
    return data_source.get_data(states=[state])
