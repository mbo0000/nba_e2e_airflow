from airflow.hooks.base import BaseHook
from gspread import Client
import logging
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GSheetsHook(BaseHook):

    def __init__(self) -> None:
        super().__init__()

    def _create_assertion_session(self):

        hook                = GoogleBaseHook(gcp_conn_id='airflow-gc-conn')
        credentials         = hook.get_credentials()
        return Client(auth=credentials)

    def get_gsheet(self, gsheet_id):

        session = self._create_assertion_session()
        logging.info(f'Opening google sheet "{gsheet_id}"')

        return session.open_by_key(gsheet_id)

    def update_worksheet(self, gsheet_id, df, worksheet_title):
        gsheet      = self.get_gsheet(gsheet_id)
        worksheet   = gsheet.worksheet(worksheet_title)
        worksheet.clear()
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())

    def get_worksheet_data(self, gsheet_id):
        
        gsh = self.get_gsheet(gsheet_id)
        if not gsh:
            logging.info(f'Spreadsheet {gsheet_id} was not found')
            return None

        for index, worksheet in enumerate(gsh.worksheets()):
            pass
