#!/usr/env python3


from typing import Type


class ValueChoicesError(ValueError):
    def __init__(self, supported_values, actual_value, argument_name=None):
        if len(supported_values) == 0:
            supported_values = [supported_values]

        self.supported_values = supported_values
        self.actual_value = actual_value
        self.argument_name = argument_name

        super().__init__(supported_values, actual_value, argument_name)

    def __str__(self):
        if self.argument_name:
            return (
                f"the value of argument {self.argument_name!r} cannot be {self.actual_value!r}"
                f" (supported values ​​are in {self.supported_values!r})."
            )
        else:
            return f"value {self.actual_value!r} is invalid (supported values ​​are in {self.supported_values!r})."


class TypeErrorEx(TypeError):
    def __init__(self, supported_types, actual_value, argument_name=None):
        if isinstance(supported_types, Type):
            supported_types = [supported_types]

        self.supported_types = supported_types
        self.actual_value = actual_value
        self.argument_name = argument_name

        super().__init__(supported_types, actual_value, argument_name)

    def __str__(self):
        if len(self.supported_types) == 1:
            expect_hint = f"{self.supported_types[0]}"
        else:
            expect_hint = f"{', '.join(str(item) for item in self.supported_types[:-1])} or {self.supported_types[1]}"

        base = f"expect {expect_hint}, not {type(self.actual_value)}"

        if self.argument_name:
            base += f" for argument {self.argument_name!r}"

        return base


class ConfigError(Exception):
    """配置相关的错误"""


class ConfigLoadError(ConfigError):
    """配置加载错误"""

    def __init__(self, file_path, reason=None):
        self.file_path = file_path
        self.reason = reason
        super().__init__(file_path)

    def __str__(self):
        reason_str = "." if self.reason is None else f": {self.reason}"
        return f"Configuration file ({self.file_path}) loading error{reason_str}"


class ExplicitlyStopHandlingError(Exception):
    """明确停止处理错误，用于处理器中，将导致整个作业停止"""


class ExplicitlySkipHandlingError(Exception):
    """明确跳过处理错误，用于处理器中，将导致当前作业的单个处理步骤被跳过"""


class UserNotFoundError(ExplicitlySkipHandlingError):
    """用户未找到错误"""

    def __init__(self, uid):
        self.uid = uid
        super().__init__(uid)

    def __str__(self):
        return f"No user found with id {self.uid}"


class ConfigNotLoadedError(ConfigError):
    """配置未加载错误"""


class JobCancelError(Exception):
    """作业取消错误，用于中断作业执行"""
