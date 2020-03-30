#!/usr/env python3
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
