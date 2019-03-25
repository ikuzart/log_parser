import argparse
import fileinput
import logging
import os
import sys
from typing import NamedTuple, List, Iterable

import pandas as pd

DEFAULT_LOG_DIR_PATH = os.path.abspath("logs")

logging.basicConfig(filename="main.log",
                    format="[%(asctime)s] %(levelname).1s %(message)s",
                    datefmt="%Y.%m.%d %H:%M:%S",
                    level=logging.INFO)
logger = logging.getLogger(__file__)


class Log(NamedTuple):
    event_time: int
    fronted_request_id: int
    event_type: str
    additional_params: List[str]


def get_log_file_names(log_dir_path: str) -> List[str]:
    return [os.path.join(log_dir_path, file_name) for file_name in os.listdir(log_dir_path)
            if file_name.endswith('.in')]


def log_files_parser(log_dir_path) -> Iterable[Log]:
    with fileinput.input(files=get_log_file_names(log_dir_path)) as log_files:
        for log_row in log_files:
            log_row = log_row.split("\t")
            yield Log(event_time=int(log_row[0]),
                      fronted_request_id=int(log_row[1]),
                      event_type=log_row[2],
                      additional_params=log_row[3:])


def make_dataframe(log_dir_path: str) -> pd.DataFrame:
    return pd.DataFrame(list(log_files_parser(log_dir_path)))


def get_95_time_quantile_by_id(df: pd.DataFrame):
    quantile_95 = {}
    for group_name, df_group in df:
        start_time = df_group[(df_group['event_type'] == 'StartSendResult\n')]['event_time'].reset_index(drop=True)
        end_time = df_group[(df_group['event_type'] == 'FinishRequest\n')]['event_time'].reset_index(drop=True)
        diff = end_time.sub(start_time)
        quantile_95[group_name] = diff.quantile(q=0.95).microseconds / 1000

    return quantile_95


def get_not_full_replica_sets_requests_number(df: pd.DataFrame):
    not_full_requests_number = 0
    for group_name, df_group in df:
        backend_connect = df_group[(df_group['event_type'] == 'BackendConnect')]['additional_params']
        backend_ok = df_group[(df_group['event_type'] == 'BackendOk')]['additional_params']
        requested_replica_groups = set(backend_connect.map(lambda x: x[0]))
        responded_replica_groups = set(backend_ok.map(lambda x: x[0][-2]))
        not_full_requests_number += len(requested_replica_groups - responded_replica_groups)

    return not_full_requests_number


def parse_args(app_name) -> argparse.Namespace:
    parser = argparse.ArgumentParser(app_name)
    parser.add_argument("--logdir", help="Path to directory with logs", default=DEFAULT_LOG_DIR_PATH)
    return parser.parse_args()


def main():
    args = parse_args(__file__)

    if not os.path.isdir(args.logdir):
        logger.error("Log directory doesn't exist.")
        sys.exit(1)

    df = make_dataframe(args.logdir).sort_values(by=['fronted_request_id', 'event_type'])
    df['event_time'] = pd.to_datetime(df['event_time'])
    df = df.groupby(['fronted_request_id'])
    quantile_95_by_id = get_95_time_quantile_by_id(df)
    not_full_requests_number = get_not_full_replica_sets_requests_number(df)
    print("95% quantile by frontend request id: ", quantile_95_by_id)
    print("Number of not fully requested replica sets: ", not_full_requests_number)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(e)
