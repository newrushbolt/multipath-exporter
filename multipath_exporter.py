#! /usr/bin/env python

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time

import prometheus_client as prom
import semver

try:
    import Queue as queue
except ImportError:
    import queue


class MultipathdExporterException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


# This allows tunning cmd with timeout on Python2 with no subprocess32 module installed
def run_command_w_timeout(cmd_args, timeout=5, append_stderr_to_stdout=False):
    timeout_errors = queue.Queue(1)

    def kill_stucked_cmd(process, timeout_errors):
        timeout_errors.put({})
        process.kill()

    cmd_call = subprocess.Popen(cmd_args, universal_newlines=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    cmd_timer = threading.Timer(timeout, kill_stucked_cmd, [cmd_call, timeout_errors])
    cmd_stdout = ""
    try:
        cmd_timer.start()
        cmd_stdout, cmd_stderr = cmd_call.communicate()
        if append_stderr_to_stdout:
            cmd_stdout += cmd_stderr
    finally:
        cmd_timer.cancel()
    if not timeout_errors.empty():
        logging.warning("Process <%s> is killed by timeout <%s>, stdout: %s",
                        cmd_args, timeout, cmd_stdout)
        return None

    return cmd_stdout


def validate_host():
    try:
        uid = os.getuid()
        if uid != 0:
            logging.error("Must be run as root, uid<%s> != 0", uid)
            return False
        multipath_help_stdout = run_command_w_timeout(
            ['multipath', '--help'], timeout=cmd_timeout, append_stderr_to_stdout=True)
        logging.debug("Multipath help response is <%s>", multipath_help_stdout)
        multipath_version_line = re.findall(
            '^multipath-tools v.*$', multipath_help_stdout, re.M)
        logging.debug("Lines with versions found: %s", multipath_version_line)
        multipath_version = multipath_version_line[0].split(' ')[1].replace('v', '')
        logging.debug("Multipath version is <%s>", multipath_version)
        if semver.compare(multipath_version, multipath_min_version) >= 0 and \
           semver.compare(multipath_version, multipath_max_version) <= 0:
            logging.debug("Multipath version <%s> is supported", multipath_version)
            return True
        else:
            logging.error("Multipath version <%s> is unsupported, must be between <%s> and <%s>",
                          multipath_version, multipath_min_version, multipath_max_version)
            return False
    except BaseException as err:
        logging.error("Cannot check multipath version: %s", err)
        return False


def load_multipath_data():
    src_data = {}
    try:
        multipathd_output = run_command_w_timeout(['multipathd', 'show', 'maps', 'json'],
                                                  timeout=cmd_timeout)
        src_data = json.loads(multipathd_output)
    except BaseException as err:
        logging.error("Cannot get valid data from multipathd: %s", err)
    return src_data


def get_luns_state(multipath_data):
    registry = None
    try:
        metrics_object = {}
        metrics_labels = ['uuid', 'dm_st']
        metrics = []
        if not multipath_data['maps']:
            logging.warning("No LUNs found")
        for lun in multipath_data['maps']:
            metrics_for_labels = [lun[label] for label in metrics_labels]
            logging.debug("Found LUN with labels|metrics|values: %s|%s|%s",
                          metrics_labels, metrics_for_labels, lun['paths'])
            metric = {
                "labels": metrics_for_labels,
                "value": lun['paths']
            }
            metrics.append(metric)

        metrics_object['multipathd_lun_paths'] = {
            "desc": 'Number of paths for a LUN',
            "labels": metrics_labels,
            "metrics": metrics
        }
        registry = raw_metrics_to_registried(metrics_object)
    except BaseException as err:
        logging.error("Cannot get LUN states: %s", err)
    return registry


def raw_metrics_to_registried(raw_metrics):
    registry = prom.CollectorRegistry()
    for metric_name, metric_data in raw_metrics.items():
        try:
            metric_gauge = prom.Gauge(
                metric_name, metric_data['desc'], metric_data['labels'],
                registry=registry)
            for metric in metric_data['metrics']:
                try:
                    metric_gauge.labels(*metric['labels']).set(metric['value'])
                except BaseException as err:
                    logging.warning("Cannot set metric <%s>: %s", metric, err)
        except BaseException as err:
            logging.warning("Cannot process metric <%s> with data <%s>: %s",
                            metric_name, metric_data, err)
    return registry


def update_metrics(registry):
    try:
        multipath_data = load_multipath_data()
        if multipath_data:
            registry.register(get_luns_state(multipath_data))
    except BaseException as err:
        logging.error("Cannot update metrics: %s", err)


def log_fatal(msg, *args, **kwargs):
    logging.fatal(msg, *args, **kwargs)
    sys.exit(1)


def main():
    logging.info("Started")

    if not validate_host():
        raise MultipathdExporterException("Cannot work on this host")

    try:
        main_registry = prom.CollectorRegistry()
    except BaseException:
        log_fatal("Cannot init prometheus collector:  ", exc_info=True)

    update_metrics(main_registry)

    try:
        prom.start_http_server(listen_port, registry=main_registry)
    except BaseException:
        log_fatal("Cannot start HTTP server, exiting: ", exc_info=True)

    try:
        while True:
            main_registry.__init__(main_registry)
            update_metrics(main_registry)
            time.sleep(collect_interval)
    except KeyboardInterrupt:
        log_fatal("Exiting on keyboard interrupt")
    except BaseException:
        log_fatal("Main loop crashed, exiting: ", exc_info=True)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Multipath LUN metrics exporter')
        parser.add_argument("--log-level", default="info",
                            help="Logging level (error|info|debug), string, default info")
        parser.add_argument("--listen-port", default=9684,
                            help="Port to listen, int, default 9684")
        parser.add_argument("--cmd-timeout", default=2.0,
                            help="Timeout for shell calls, float, default 2.0")
        parser.add_argument("--collect-interval", default=60.0,
                            help="Metrics update interval, float, default 60.0")
        parser_args = parser.parse_args()

        cmd_timeout = parser_args.cmd_timeout
        collect_interval = parser_args.collect_interval
        listen_port = parser_args.listen_port
        multipath_min_version = '0.4.6'
        multipath_max_version = '0.7.9'

        if parser_args.log_level == 'error':
            logging.basicConfig(level=logging.ERROR)
        elif parser_args.log_level == 'info':
            logging.basicConfig(level=logging.INFO)
        elif parser_args.log_level == 'debug':
            logging.basicConfig(level=logging.DEBUG)
    except SystemExit:
        sys.exit(0)
    except BaseException:
        log_fatal("Cannot init variables, exiting", exc_info=True)
    main()
