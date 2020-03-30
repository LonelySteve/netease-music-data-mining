import os
from logging import Logger
from typing import List, Optional, Union

import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from starlette.responses import RedirectResponse

from src.config import Config, get_logger, get_mongo_database
from src.exceptions import ExplicitlyStopHandlingError, UserNotFoundError
from src.fetcher import IndexFetcher
from src.flag import ThreadFlag
from src.monitor import IndexFetcherMonitor
from src.util import get_traceback_text

app = FastAPI()

_fetchers: List[IndexFetcher] = []
_monitor = IndexFetcherMonitor(_fetchers)
_db = None
_logger: Optional[Logger] = None


@app.on_event("startup")
def startup():
    config_file_path = os.getenv("CONFIG_FILE_PATH", "config.ini.example")
    Config.load(config_file_path, encoding="utf8")
    global _db, _logger
    _db = get_mongo_database()
    _logger = get_logger("nmdm-fetcher-logger")
    _monitor.start()


@app.on_event("shutdown")
def shutdown():
    _monitor.stop()
    _logger.shutdown()


@app.get("/")
def home():
    return RedirectResponse("/fetcher")


@app.get("/monitor")
def monitor():
    return {
        "monitor": {
            "flag": str(_monitor.flag),
            "processed": _monitor.processed,
            "averageSpeed": _monitor.average_speed,
            "effectiveAverageSpeed": _monitor.effective_average_speed,
            "remainingTime": _monitor.remaining_time,
            "age": _monitor.age,
        },
        "jobs": {
            str(job_status_data.job): {
                "flag": str(job_status_data.flag),
                "processed": job_status_data.processed,
                "averageSpeed": job_status_data.average_speed,
                "effectiveAverageSpeed": job_status_data.effective_average_speed,
                "remainingTime": job_status_data.remaining_time,
                "age": job_status_data.age,
            } for job_status_data in _monitor.monitored_jobs.values()
        }
    }


@app.get("/fetcher")
def fetcher_list():
    return {f.name: str(f) for f in _fetchers}


@app.get("/fetcher/new")
def fetcher_new(begin: int, end: Optional[int] = Query(None), step: int = 1,
                weights: Optional[List[Union[int, float]]] = Query(None)):
    fetcher = IndexFetcher(
        begin=begin, end=end, step=step, thread_weights=weights, name=f"Fetcher-{len(_fetchers)}"
    )

    with requests.session() as session:
        @fetcher.handlers.add
        def scrape_user_info(i):
            r = session.get(Config.api_user_info_url, params={"uid": i})
            data = None
            if r.status_code == 200:
                data = r.json()
            if r.status_code == 404 or data["code"] == 404:
                raise UserNotFoundError(i)
            if r.status_code != 200 or data["code"] != 200:
                raise ExplicitlyStopHandlingError(f"{data}")

            _db["user_info"].update_one({"userPoint.userId": i}, {"$set": data}, upsert=True)

    @fetcher.emitter.on("IndexJob.running")
    def on_running(sender):
        _logger.info(f"即将开始的作业：{sender}")

    @fetcher.emitter.on("IndexJob.stopped")
    def on_stopped(sender, err_info):
        _logger.info(f"即将结束的作业：{sender}, 导致结束的出错堆栈：\n{get_traceback_text(*err_info)}")

    @fetcher.emitter.on("IndexJob.step_switch")
    def on_step_switch(sender):
        _logger.debug(f"步骤切换的作业：{sender}")

    @fetcher.emitter.on("IndexJob.handled")
    def on_handled(sender):
        _logger.debug(f"单次作业已完成：{sender}")

    @fetcher.emitter.on("IndexJob.unexpected_exception")
    def on_unexpected_exception(sender, err_info):
        _logger.error(f"工作出现意外异常的作业：{sender}, 出错堆栈：\n{get_traceback_text(*err_info)}")

    @fetcher.emitter.on("IndexJob.handle_skipped")
    def on_handle_error(sender, err_info):
        _logger.debug(f"工作遇到处理过程被跳过的作业：{sender}，导致跳过的出错堆栈：\n{get_traceback_text(*err_info)}")

    @fetcher.emitter.on("error")
    def on_error(err):
        _logger.error(f"未知错误：{err!r}，来自 {fetcher}")

    _fetchers.append(fetcher)

    return {
        "fid": fetcher.name
    }


def try_find_fetcher(fid):
    fetcher = next((f for f in _fetchers if f.name == fid), None)
    if fetcher is None:
        raise HTTPException(404, detail=f"未找到 id 为 {fid!r} 的 fetcher")
    return fetcher


@app.get("/fetcher/start")
def fetcher_start(fid: str):
    try:
        try_find_fetcher(fid).start()
    except Exception as e:
        return {
            "error": str(e)
        }

    return True


@app.get("/fetcher/start_all")
def fetcher_start_all():
    try:
        for fetcher in (f for f in _fetchers if ThreadFlag.pending in f.flag):
            fetcher.start()
    except Exception as e:
        return {
            "error": str(e)
        }

    return True


@app.get("/fetcher/stop")
def fetcher_stop(fid: str):
    try:
        try_find_fetcher(fid).stop()
    except Exception as e:
        return {
            "error": str(e)
        }

    return True


@app.get("/fetcher/stop_all")
def fetcher_stop_all():
    try:
        for fetcher in _fetchers:
            fetcher.stop()
    except Exception as e:
        return {
            "error": str(e)
        }

    return True


@app.get("/fetcher/delete")
def fetcher_delete(fid: str):
    try:
        fetcher = try_find_fetcher(fid)
        fetcher.stop()
        _fetchers.remove(fetcher)
    except Exception as e:
        return {
            "error": str(e)
        }

    return True


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000, log_level="debug")
