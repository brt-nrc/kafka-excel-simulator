import json
import copy
import functools
import re
from kafka_excel_sim.message import Message
from kafka_excel_sim.message_schedule import MessageSchedule
from kafka_excel_sim.config import MessageConfig


class MessageBuilder:
    """Builds a Message by merging a payload template with schedule overrides.

    The builder holds all payload templates (loaded once from the 'types'
    sheet) and produces a ready-to-send Message for each scheduled entry.

    Attributes:
        topic:     Kafka topic all messages will be sent to.
        templates: Dict mapping type identifier → raw payload template string.
    """

    def __init__(self, templates:  dict[str, dict[str, list[str] | str]], message_config: MessageConfig) -> None:
        # Add payload: json.loads(raw_payload) to all items
        self.templates = {
            template_id: {
                **template_data,
                'payload':json.loads(template_data['raw_payload'])
            }
            for template_id, template_data in templates.items()
        }
        self.message_configuration: MessageConfig = message_config

    def build(self, schedule: MessageSchedule, key: str | None = None) -> Message:
        """Produce a Message from a scheduled entry.

        Steps:
          1. Look up the payload template by schedule.message_type.
          2. Apply schedule.overrides to fill in the modifiable placeholders.
          3. Parse the result into a dict.
          4. Wrap it in a Message.

        Args:
            schedule: The timetable entry to process.
            key:      Optional Kafka partition key.

        Returns:
            A Message ready for MessageSender.

        Raises:
            KeyError:  If the message_type is not found in templates.
            ValueError: If the rendered payload is not valid JSON.
        """
        try:
            template: dict[str, str | list[str]] = self.templates[schedule.message_type]
        except KeyError as exc:
            raise KeyError(f'Type {schedule.message_type} has not been defined.') from exc
        except Exception as exc:
            raise exc

        renames: dict[str, str] = {}
        override_values: dict[str, str] = {}
        for k, v in schedule.overrides.items():
            if k.endswith('_rename_to'):
                destination_dict = renames
            else:
                destination_dict = override_values
            destination_dict.update({k:v})

        # built_payload: str = template['raw_payload'].format_map(override_values)
        # built_payload_dict: dict = json.loads(built_payload)
        template_dict = json.loads(template['payload'])

        # Filter overrides to only include paths that exist in the template
        # Also skip empty/null values if keep_template_value_where_null is True
        keep_template = self.message_configuration.keep_template_value_where_null
        valid_overrides = {
            path: value
            for path, value in override_values.items()
            if self._path_exists(template_dict, path)
            and not (keep_template and (value is None or value == ""))
        }

        # Apply value overrides with type conversion
        for path, value in valid_overrides.items():
            typed_value = self._parse_value(value)
            self._set_nested(template_dict, path, typed_value)
        
        # Filter renames to only include paths that exist in the template
        valid_renames = {
            old_path: new_name
            for rename_spec, new_name in renames.items()
            if (old_path := rename_spec.replace('_rename_to', '')) 
            and self._path_exists(template_dict, old_path)
        }

        # Apply renames
        for old_path, new_name in valid_renames.items():
            self._rename_key(template_dict, old_path, new_name)

        built_message = Message(
            topic = template['topic'],
            key = str(key),
            payload = template_dict
        )

        return built_message

    # Sentinel value to represent wildcard in paths
    WILDCARD = '__WILDCARD__'

    _PATH_PATTERN = re.compile(r'([\w-]+)|\[(\d+)\]|\.(\d+)|\[(\*)\]|\.(\*)')

    # Separator for paths that cross into a JSON-encoded string field
    STRING_JSON_SEPARATOR = '::'

    def _set_nested(self, obj: dict, path: str, value: any) -> None:
        """Set a value at a nested path in a dict. e.g. 'payload.metrics.value' 
        
        Supports array indexing:
        - 'payload.items[0].name' - bracket notation
        - 'payload.items.0.name' - dot notation with numeric index
        
        Supports wildcards to apply to all array elements:
        - 'payload.items[*].name' - bracket wildcard
        - 'payload.items.*.name' - dot wildcard
        
        Supports JSON-encoded string traversal with :: separator:
        - 'payload.metrics.value::telemetries.timestamp'
          Navigates to 'payload.metrics.value', parses the string as JSON,
          sets 'telemetries.timestamp' inside it, then re-serializes to string.
        """
        if self.STRING_JSON_SEPARATOR in path:
            outer_path, inner_path = path.split(self.STRING_JSON_SEPARATOR, 1)
            self._set_in_string_json(obj, outer_path, inner_path, value)
            return
        keys = self._parse_path(path)
        self._set_nested_recursive(obj, keys, value)
    
    def _set_nested_recursive(self, current: any, keys: list, value: any) -> None:
        """Recursively traverse and set value, handling wildcards."""
        if not keys:
            return
        
        key = keys[0]
        
        # Handle wildcard - iterate over all elements in list/dict
        if key == self.WILDCARD:
            if isinstance(current, list):
                for item in current:
                    self._set_nested_recursive(item, keys[1:], value)
            elif isinstance(current, dict):
                for item in current.values():
                    self._set_nested_recursive(item, keys[1:], value)
            return
        
        # Regular key traversal
        if len(keys) == 1:
            # Final key - set the value
            if isinstance(key, int) and isinstance(current, list):
                current[key] = value
            else:
                current[key] = value
        else:
            # More keys to traverse
            next_obj = current[key] if not isinstance(key, int) else current[key]
            self._set_nested_recursive(next_obj, keys[1:], value)
    
    def _path_exists(self, obj: dict, path: str) -> bool:
        """Check if a nested path exists in a dict. e.g. 'payload.metrics.value'
        
        Supports array indexing:
        - 'payload.items[0].name' - bracket notation
        - 'payload.items.0.name' - dot notation with numeric index
        
        Supports wildcards:
        - 'payload.items[*].name' - returns True if path is valid for at least one element
        - 'payload.items.*.name' - same as above
        
        Supports JSON-encoded string traversal with :: separator:
        - 'payload.metrics.value::telemetries.timestamp'
          Checks if 'payload.metrics.value' exists and is a JSON string,
          then checks if 'telemetries.timestamp' exists inside the parsed JSON.
        """
        if self.STRING_JSON_SEPARATOR in path:
            outer_path, inner_path = path.split(self.STRING_JSON_SEPARATOR, 1)
            return self._path_exists_in_string_json(obj, outer_path, inner_path)
        keys = self._parse_path(path)
        return self._path_exists_recursive(obj, keys)
    
    def _path_exists_recursive(self, current: any, keys: list) -> bool:
        """Recursively check if path exists, handling wildcards."""
        if not keys:
            return True
        
        key = keys[0]
        
        # Handle wildcard
        if key == self.WILDCARD:
            if isinstance(current, list):
                return any(self._path_exists_recursive(item, keys[1:]) for item in current)
            elif isinstance(current, dict):
                return any(self._path_exists_recursive(item, keys[1:]) for item in current.values())
            return False
        
        # Check if key exists
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return False
            return self._path_exists_recursive(current[key], keys[1:])
        else:
            if not isinstance(current, dict) or key not in current:
                return False
            return self._path_exists_recursive(current[key], keys[1:])
    
    @functools.lru_cache(maxsize=1024)
    def _parse_path(self, path: str) -> list:
        """Parse a path string into a list of keys and indices.
        
        Examples:
        - 'payload.metrics.value' -> ['payload', 'metrics', 'value']
        - 'payload.items[0].name' -> ['payload', 'items', 0, 'name']
        - 'payload.items.0.name' -> ['payload', 'items', 0, 'name']
        - 'payload.items[*].name' -> ['payload', 'items', WILDCARD, 'name']
        - 'payload.items.*.name' -> ['payload', 'items', WILDCARD, 'name']
        - 'Good-morning-from' -> ['Good-morning-from'] (keys with dashes)
        """
        keys = []
        # Match: word chars (including dashes and underscores), array index [n], .n, wildcard [*], or .*
        matches = self._PATH_PATTERN.findall(path)
        
        for match in matches:
            if match[0]:  # word characters (including dashes/underscores)
                keys.append(match[0])
            elif match[1]:  # bracket notation [n]
                keys.append(int(match[1]))
            elif match[2]:  # dot notation .n
                keys.append(int(match[2]))
            elif match[3]:  # bracket wildcard [*]
                keys.append(self.WILDCARD)
            elif match[4]:  # dot wildcard .*
                keys.append(self.WILDCARD)
        
        return keys
    
    def _rename_key(self, obj: dict, path: str, new_name: str) -> None:
        """Rename a key at a nested path.
        
        Supports array indexing:
        - 'payload.items[0].name' - bracket notation
        - 'payload.items.0.name' - dot notation with numeric index
        
        Supports JSON-encoded string traversal with :: separator:
        - 'payload.metrics.value::telemetries.uid_rename_to'
        
        Only renames if the path exists in the object.
        """
        if not self._path_exists(obj, path):
            return  # Path doesn't exist, skip rename
        
        if self.STRING_JSON_SEPARATOR in path:
            outer_path, inner_path = path.split(self.STRING_JSON_SEPARATOR, 1)
            self._rename_in_string_json(obj, outer_path, inner_path, new_name)
            return
        
        keys = self._parse_path(path)
        current = obj
        for key in keys[:-1]:
            if isinstance(key, int):
                current = current[key]
            else:
                current = current[key]
        old_key = keys[-1]
        if isinstance(old_key, int):
            # For arrays, we can't rename an index, but handle dict inside array
            if isinstance(current[old_key], dict) and isinstance(new_name, str):
                current[old_key][new_name] = current[old_key].pop(old_key)
        elif old_key in current:
            current[new_name] = current.pop(old_key)
    
    # ─── String-embedded JSON helpers ────────────────────────────────────────

    def _get_nested_value(self, obj: any, keys: list) -> any:
        """Retrieve a value at a nested path (list of keys/indices)."""
        current = obj
        for key in keys:
            if isinstance(key, int):
                current = current[key]
            else:
                current = current[key]
        return current

    def _path_exists_in_string_json(self, obj: dict, outer_path: str, inner_path: str) -> bool:
        """Check if inner_path exists inside a JSON-encoded string at outer_path.
        
        Returns True if:
          1. outer_path exists in obj
          2. The value at outer_path is a string containing valid JSON
          3. inner_path exists inside the parsed JSON
        """
        outer_keys = self._parse_path(outer_path)
        if not self._path_exists_recursive(obj, outer_keys):
            return False
        try:
            string_value = self._get_nested_value(obj, outer_keys)
        except (KeyError, IndexError, TypeError):
            return False
        if not isinstance(string_value, str):
            return False
        try:
            parsed = json.loads(string_value)
        except (json.JSONDecodeError, TypeError):
            return False
        inner_keys = self._parse_path(inner_path)
        return self._path_exists_recursive(parsed, inner_keys)

    def _set_in_string_json(self, obj: dict, outer_path: str, inner_path: str, value: any) -> None:
        """Set a value inside a JSON-encoded string field.
        
        1. Navigate to outer_path and read the string
        2. Parse the string as JSON
        3. Set value at inner_path inside the parsed dict
        4. Re-serialize to JSON string and write it back
        """
        outer_keys = self._parse_path(outer_path)
        string_value = self._get_nested_value(obj, outer_keys)
        parsed = json.loads(string_value)
        
        inner_keys = self._parse_path(inner_path)
        self._set_nested_recursive(parsed, inner_keys, value)
        
        # Re-serialize and write back
        new_string = json.dumps(parsed, separators=(',', ':'), ensure_ascii=False)
        self._set_nested_recursive(obj, outer_keys, new_string)

    def _rename_in_string_json(self, obj: dict, outer_path: str, inner_path: str, new_name: str) -> None:
        """Rename a key inside a JSON-encoded string field.
        
        1. Navigate to outer_path and read the string
        2. Parse the string as JSON
        3. Rename the key at inner_path
        4. Re-serialize to JSON string and write it back
        """
        outer_keys = self._parse_path(outer_path)
        string_value = self._get_nested_value(obj, outer_keys)
        parsed = json.loads(string_value)
        
        inner_keys = self._parse_path(inner_path)
        # Navigate to parent and rename
        current = parsed
        for key in inner_keys[:-1]:
            if isinstance(key, int):
                current = current[key]
            else:
                current = current[key]
        old_key = inner_keys[-1]
        if isinstance(old_key, int):
            if isinstance(current[old_key], dict) and isinstance(new_name, str):
                current[old_key][new_name] = current[old_key].pop(old_key)
        elif old_key in current:
            current[new_name] = current.pop(old_key)
        
        # Re-serialize and write back
        new_string = json.dumps(parsed, separators=(',', ':'), ensure_ascii=False)
        self._set_nested_recursive(obj, outer_keys, new_string)

    # ─── End string-embedded JSON helpers ────────────────────────────────────

    def _parse_value(self, value: any) -> any:
        """Convert string values to appropriate types.
        
        - 123 → 123 (int)
        - 123.45 → 123.45 (float)
        - true/false → bool
        - "true"/"false" → "true"/"false"
        - "123" (with quotes) → "123" (string, quotes stripped)
        - Otherwise → str
        """
        if not isinstance(value, str):
            return value  # already typed (from Excel number cell, etc)
        
        value = value.strip()
        
        # Explicit string (quoted) — remove quotes and return as string
        if (value.startswith('"') and value.endswith('"')) or \
        (value.startswith("'") and value.endswith("'")):
            return value[1:-1]  # strip outer quotes
        
        value_lower = value.lower()
        
        # Boolean
        if value_lower in ('true', 'false'):
            return value_lower == 'true'
        
        # Null
        if value_lower == 'null':
            return None
        
        # Number (int first, then float)
        try:
            if '.' not in value:
                return int(value)
            return float(value)
        except ValueError:
            pass
        
        # Default to string
        return value