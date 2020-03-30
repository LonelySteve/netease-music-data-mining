#!/usr/env python3
from configparser import ConfigParser
from logging import Formatter, Logger, getLevelName
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import quote_plus

from pymongo import MongoClient

from src.exceptions import ConfigLoadError, ConfigNotLoadedError


class ConfigMeta(type):
    def __getattr__(self, item):
        if not getattr(self, "_loaded", None):
            raise ConfigNotLoadedError
        return super().__getattribute__(item)


class Config(object, metaclass=ConfigMeta):
    _loaded = False
    _parser = ConfigParser()

    # database
    database_type: str
    database_name: str
    database_host: str
    database_port: int
    database_user: Optional[str]
    database_password: Optional[str]

    # logger
    logger_level: str
    logger_log_file_path: Path
    logger_log_file_rollover_interval: str
    logger_log_file_interval: int
    logger_log_file_backup_count: int
    logger_log_file_encoding: str
    logger_format: str

    # api
    api_user_info_url: str

    @classmethod
    def set_parser(cls, parser: Optional[ConfigParser]):
        cls._parser = parser

    @classmethod
    def load(cls, file_path, encoding=None):
        cls._parser.read(file_path, encoding)
        cls._load(file_path)
        cls._loaded = True

    @classmethod
    def dump(cls, file_path, encoding=None):
        with open(file_path, "w", encoding=encoding) as fp:
            cls._parser.write(fp)

    @classmethod
    def _load(cls, file_path):
        fields = {
            # database
            "database_type": lambda: cls._parser.get("database", "type", fallback="mongodb"),
            "database_name": lambda: cls._parser.get("database", "name", fallback="nmdm-fetcher"),
            "database_host": lambda: cls._parser.get("database", "host", fallback="127.0.0.1"),
            "database_port": lambda: cls._parser.getint("database", "port", fallback="27017"),
            "database_user": lambda: cls._parser.get("database", "user", fallback=None),
            "database_password": lambda: cls._parser.get("database", "password", fallback=None),

            # logger
            "logger_level": lambda: cls._parser.get("logger", "level", fallback="INFO"),
            "logger_log_file_path": lambda: Path(
                cls._parser.get(
                    "logger", "log_file_path",
                    fallback="logs/fetcher.log"
                )
            ),
            "logger_log_file_rollover_interval": lambda: cls._parser.get(
                "logger",
                "log_file_rollover_interval",
                fallback="D"
            ),
            "logger_log_file_interval": lambda: cls._parser.getint("logger", "log_file_interval", fallback=1),
            "logger_log_file_backup_count": lambda: cls._parser.getint("logger", "log_file_backup_count", fallback=0),
            "logger_log_file_encoding": lambda: cls._parser.get("logger", "log_file_encoding", fallback="utf-8"),
            "logger_format": lambda: cls._parser.get(
                "logger", "format",
                fallback="[%(asctime)s][%(name)s/%(threadName)s][%(levelname)s]: %(message)s"),
            # api
            "api_user_info_url": lambda: cls._parser.get("api", "user_info_url",
                                                         fallback="http://127.0.0.1:3000/user/detail")
        }
        # 遍历加载
        for key, getter in fields.items():
            try:
                setattr(cls, key, getter())
            except Exception:
                raise ConfigLoadError(
                    file_path, f"An exception occurred while getting the value of field {key!r}")


def get_mongo_database():
    if "mongo" not in Config.database_type.lower():
        raise RuntimeError("MongoDB is not currently supported.")

    user_pass = "" if Config.database_user is None else \
        f"{quote_plus(Config.database_user)}:{quote_plus(Config.database_password)}@"
    host_port = f"{quote_plus(Config.database_host)}:{Config.database_port}"

    return MongoClient(f"mongodb://{user_pass}{host_port}")[Config.database_name]


_loggers: Dict[str, Logger] = {}


def get_logger(name: str) -> Logger:
    if name in _loggers:
        return _loggers[name]

    Config.logger_log_file_path.parent.mkdir(parents=True, exist_ok=True)

    logger = Logger(name, getLevelName(Config.logger_level))
    handler = TimedRotatingFileHandler(
        filename=str(Config.logger_log_file_path),
        when=Config.logger_log_file_rollover_interval,
        interval=Config.logger_log_file_interval,
        backupCount=Config.logger_log_file_backup_count,
        encoding=Config.logger_log_file_encoding
    )
    handler.setFormatter(Formatter(Config.logger_format))
    logger.addHandler(handler)

    _loggers[name] = logger

    return logger
