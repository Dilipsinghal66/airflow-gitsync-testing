from googleapiclient.discovery import build
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from common.db_functions import get_data_from_db
from typing import Dict, Optional
import pandas as pd


SPREADSHEET_ID = '1Stvnw3ezJAJYYjGBTl4ou5Tr1ZcJbmx7ChmYrDg2d1I'
RANGE_NAME = 'Sheet1'
STATUS = 4
TYPE = 0
DESCRIPTION = "AZ"
INITIATED_BY = "SCHEDULED TASK"


######## GETTING DATA FROM GOOGLE SPREADSHEET ######

# noinspection PyAbstractClass
class GoogleSheetsHook(GoogleCloudBaseHook):
    """
    Hook for the Google Drive APIs.

    :param api_version: API version used (for example v3).
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    _conn = None

    def __init__(
            self,
            spreadsheet_id: str,
            gcp_conn_id: str = 'gcp_sheet',
            api_version: str = 'v4',
            delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.spreadsheet_id = spreadsheet_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self._conn = None

    def get_conn(self):

        """
        Retrieves the connection to Google Drive.


        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('sheets', self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    @GoogleCloudBaseHook.catch_http_exception
    def get_values(
            self,
            range_: str,
            major_dimension: str,
            value_render_option: str = 'FORMATTED_VALUE',
            date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Gets values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
        :param range_: The A1 notation of the values to retrieve.
        :type range_: str
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: List
        """
        service = self.get_conn()
        response = service.spreadsheets().values().get(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            majorDimension=major_dimension,
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option
        ).execute()

        return response.get('values', [])


######## DATABASE SYNC CODE #########

# check_for_null() checks the data for null values and substitutes it with an empty string
def check_for_null(input_string):

    if input_string is not None:
        return input_string
    else:
        return " "


# record_exist() checks the database for existence of the record.
# It helps in deciding if the query should be INSERT or UPDATE.
def record_exist(spreadsheet_data, row, cursor):

    select_query = "SELECT EXISTS(SELECT * FROM zylaapi.doc_profile  WHERE phoneno = '" + \
                   str(spreadsheet_data['Phone Number (+91)'][row]) + "');"

    cursor.execute(select_query)
    response = cursor.fetchone()[0]

    if response == '0':
        return False
    else:
        return True


# dump_data_in_db handles updating of data in the mysql database
def dump_data_in_db(spreadsheet_data, cursor, connection):

    # print("\n ##### In function dump data ##### \n")

    for row in range(len(spreadsheet_data)):

        if spreadsheet_data['Phone Number (+91)'][row] is not None:

            if record_exist(spreadsheet_data, row, cursor):
                print("\n Update sql query")
                update_sql_query = " UPDATE zylaapi.doc_profile SET " \
                                   "profile_image = '" + \
                                   str(check_for_null(spreadsheet_data['Photo (Keep name of photo as doctor code)'][row])) + "', " +\
                                   "name = '" + str(spreadsheet_data['Name of Dcotor'][row]) + "', " + \
                                   "speciality = '" + str(check_for_null(spreadsheet_data['Speciality'][row])) + "', " \
                                   + "clinicHospital = '" + str(check_for_null(spreadsheet_data['Hospital/Clinic'][row])) \
                                   + "', " + "location = '" + str(check_for_null(spreadsheet_data['City'][row])) + "', " \
                                   + "licenseNumber = '" + str(spreadsheet_data['Doctor Code'][row]) + "', " + \
                                   "status = '" + str(STATUS) + "', " + \
                                   "initiated_by = '" + str(INITIATED_BY) + "', " + \
                                   "type = '" + str(TYPE) + "', " + \
                                   "description = '" + str(DESCRIPTION) + "', " + \
                                   "title = '" + str(spreadsheet_data['Title'][row]) + "', " + \
                                   "code = '" + str(spreadsheet_data['Doctor Code'][row]) + "', " + \
                                   "email = '" + str(check_for_null(spreadsheet_data['Email Id'][row])) + \
                                   "' WHERE phoneno = '" + str(spreadsheet_data['Phone Number (+91)'][row]) + "'"
                print(update_sql_query)
                cursor.execute(update_sql_query)

            else:
                print("\n Insert sql query")
                insert_sql_query = "INSERT INTO zylaapi.doc_profile (phoneno, profile_image, name, " \
                               "speciality, clinicHospital, location, licenseNumber, status, " \
                               "initiated_by, type, description, title, code, email) VALUES ('" + \
                               str(spreadsheet_data['Phone Number (+91)'][row]) + "', '" + \
                               str(check_for_null(spreadsheet_data['Photo (Keep name of photo as doctor code)'][row])) \
                               + "', '" + \
                               str(spreadsheet_data['Name of Dcotor'][row]) + "', '" + \
                               str(check_for_null(spreadsheet_data['Speciality'][row])) + "', '" + \
                               str(check_for_null(spreadsheet_data['Hospital/Clinic'][row])) + "', '" + \
                               str(check_for_null(spreadsheet_data['City'][row])) + "', '" + \
                               str(spreadsheet_data['Doctor Code'][row]) + "', '" + \
                               str(STATUS) + "', '" + \
                               str(INITIATED_BY) + "', '" + \
                               str(TYPE) + "', '" + \
                               str(DESCRIPTION) + "', '" + \
                               str(spreadsheet_data['Title'][row]) + "', '" + \
                               str(spreadsheet_data['Doctor Code'][row]) + "', '" + \
                               str(check_for_null(spreadsheet_data['Email Id'][row])) + "');"

                print(insert_sql_query)
                cursor.execute(insert_sql_query)

    connection.commit()
    print("Successfully Committed")


# initializer() is the driver function for this script
def initializer():

    g_sheet_hook = GoogleSheetsHook(SPREADSHEET_ID)

    g_sheet_hook.get_conn()

    spreadsheet_data = g_sheet_hook.get_values(range_=RANGE_NAME, major_dimension="ROWS")

    spreadsheet_data = pd.DataFrame(spreadsheet_data[1:], columns=spreadsheet_data[0])

    engine = get_data_from_db(db_type="mysql", conn_id="mysql_monolith")
    connection = engine.get_conn()
    cursor = connection.cursor()

    dump_data_in_db(spreadsheet_data, cursor, connection)






