import openpyxl
import logging
from string import Formatter
from pathlib import Path
from kafka_excel_sim.message_schedule import MessageSchedule

logger: logging.Logger = logging.getLogger(__name__)

class ExcelReader:
    """Reads and parses the simulation Excel file.

    Expected workbook structure:
      - Sheet 'Message types':      columns [message_type_id, topic, payload]
                                    'payload' is a JSON string; fields the user
                                    wants to override at send-time are wrapped in
                                    curly braces, e.g. {lat}, {lon}, {speed}.
      - Sheet 'Message timetable':  columns [offset_s, message_type_id, <override_fields>...]
                                    Additional columns correspond 1-to-1 with the
                                    modifiable placeholders declared in the payload.
      - Sheet 'Configuration':      rows with configuration values, such as kafka connection
                                    configuration.

    Attributes:
        file_path: Path to the .xlsx file.
    """
    SHEET_TYPES = 'Message types'
    SHEET_TIMETABLES = 'Message schedule'
    SHEET_CONFIGURATION = 'Configuration'

    def __init__(self, file_path: Path) -> None:
        self.file_path: Path = file_path
        self._workbook: openpyxl.Workbook | None = None
        logger.debug(f'ExcelReader initiated with {file_path=}')
    
    def __enter__(self) -> "ExcelReader":
        self._workbook = openpyxl.load_workbook(
            filename  = self.file_path,
            read_only = True,
            data_only = True,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._workbook:
            self._workbook.close()
            self._workbook = None
        return False

    def load_config(self) -> dict[str,dict[str, str]]:
        """Parse the 'Configuration' sheet.

        Returns:
            A dict with configuration keys and values
        """
        ws: openpyxl.worksheet.worksheet.Worksheet = self._workbook[self.SHEET_CONFIGURATION]
        
        config: dict[str,dict[str, str]] = {}
        current_category: str | None = None

        for row in ws.iter_rows(min_col = 1, max_col = 3, values_only = True):
            category, key, value = row
            if key is None: continue
            if category is not None:
                current_category = str(category)
                config[current_category] = dict()
            config[current_category][str(key)] = str(value)
        
        return config

    def load_types(self) -> dict[str, dict[str, list[str] | str]]:
        """Parse the 'Message types' sheet.

        Returns:
            A dict mapping each identifier to its raw payload template string.
        """
        ws: openpyxl.worksheet.worksheet.Worksheet = self._workbook[self.SHEET_TYPES]
        headers: list[str] = [str(cell.value) for cell in ws[1]]
        logger.debug(f'Loaded types headers: {headers}')
        types_dict = {}

        for row in ws.iter_rows(min_row = 2, values_only = True):
            row_dict: dict[str, str] = dict(zip(headers, row))
            overridable_parameters: list[str] = \
            [str(field_name) for literal_text, field_name, format_spec, conversion\
                 in Formatter().parse(row_dict['payload']) if field_name is not None]
            new_type: dict[str, dict[str, list[str] | str]] = {
                row_dict['message_type_id']:
                  {
                    'topic': row_dict['kafka_topic'],
                    'raw_payload': row_dict['payload'],
                    'overridable_parameters': overridable_parameters,
                  }
                }
            logger.debug(f'New type prepared: {new_type}')
            types_dict.update(new_type)
        return types_dict

    def load_schedule(self) -> list[MessageSchedule]:
        """Parse the 'timetable' sheet.

        Returns:
            An ordered list of MessageSchedule objects, sorted by
            offset_seconds ascending, then by row_number ascending
            to ensure deterministic order for messages with the same offset.
        """
        ws: openpyxl.worksheet.worksheet.Worksheet = self._workbook[self.SHEET_TIMETABLES]
        headers: list[str] = [str(cell.value) for cell in ws[1]]
        logger.debug(f'Loaded schedule headers: {headers}')
        message_schedule_list: list[MessageSchedule] = []

        for row_idx, row in enumerate(ws.iter_rows(min_row = 3, values_only = True), start=3):
            row_dict: dict[str, object] = dict(zip(headers,row))
            new_schedule = MessageSchedule(
                offset_seconds = float(row_dict['offset_s']),
                message_type = str(row_dict['message_type_id']),
                overrides = {k:v for k, v in row_dict.items() \
                     if k not in {'offset_s','message_type_id'}},
                row_number = row_idx
            )
            logger.debug(f'New schedule loaded: {new_schedule}')
            message_schedule_list.append(new_schedule)
        
        return sorted(message_schedule_list, key = lambda entry: (entry.offset_seconds, entry.row_number))
    
    def validate(
        self, 
        types: dict[str, dict[str, list[str] | str]], 
        schedule_list: list[MessageSchedule]
    ) -> None:
        """Validates the schedule list against the provided types.

        Arguments:
            types: dict returned from load_types()
            schedule_list: list of scheduled returned from load_schedule()
        Raises:
            ValueError: If a message_type_id specified is not present in types
        """

        loaded_types_set: set[str] = set(types.keys())
        bad_types: list[str] = []
        for schedule in schedule_list:
            message_type: str = schedule.message_type
            if message_type not in loaded_types_set:
                bad_types.append(message_type)
        
        if bad_types:
            logger.error(f'Found undefined type(s): {bad_types}')
            raise ValueError(f'Found undefined type(s): {bad_types}')

