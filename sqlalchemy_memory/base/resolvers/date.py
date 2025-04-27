from .abstract import FunctionResolver


class DateResolver(FunctionResolver):
    def accessor(self, item, attr_name):
        value = getattr(item, attr_name)
        return value.date() if value else None
