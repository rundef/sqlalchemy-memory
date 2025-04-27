from .abstract import FunctionResolver


class JsonExtractResolver(FunctionResolver):
    def _extract_json_value(self, data_dict, path):
        # Traverse nested keys for a JSON path like 'ref.abc.xyz'
        current = data_dict or {}
        for key in path.split('.'):
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    def accessor(self, item, attr_name):
        path_expr = self.clauses[0]

        raw = path_expr.value if hasattr(path_expr, 'value') else str(path_expr).strip('"')

        # Strip leading '$.' or '$'
        if raw.startswith('$.'):
            raw_path = raw[2:]
        elif raw.startswith('$'):
            raw_path = raw[1:]
        else:
            raw_path = raw

        return self._extract_json_value(getattr(item, attr_name), raw_path)
