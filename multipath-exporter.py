#! /usr/bin/env python

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time

import prometheus_client as prom

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess


class MultipathdExporterException(Exception):
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)


def validate_host():
    try:
        username = getpass.getuser()
        if not username == 'root':
            logging.error("Must be run as root")
            return False
        multipath_help_stdout = subprocess.check_output(
            ['multipath', '-h'], stderr=subprocess.STDOUT, timeout=cmd_timeout)
        multipath_version_line = re.match(
            '^multipath-tools.*$', multipath_help_stdout, re.M).group(0)
        multipath_version = multipath_version_line.split(' ')[1]
        logging.debug("Multipath version is <%s>" % multipath_version)
        if re.match(multipath_regex_version, multipath_version):
            return True
        else:
            logging.error("Multipath version is unsupported")
            return False
    except BaseException as err:
        logging.error("Cannot check multipath version: %s" % err)
        return False


def load_multipath_data():
    src_data = {}
    try:
        multipathd_output = subprocess.check_output(
            ['multipathd', 'show', 'maps', 'json'], stderr=subprocess.STDOUT, timeout=cmd_timeout)
        src_data = json.loads(multipathd_output)
    except BaseException as err:
        logging.error("Cannot get valid data from multipathd: %s" % err)
    return src_data


def get_luns_state(multipath_data):
    registry = None
    try:
        metrics_object = {}
        metrics_labels = ['uuid', 'dm_st']
        metrics = []
        if len(multipath_data['maps']) == 0:
            logging.warning("No LUNs found")
        for lun in multipath_data['maps']:
            metric = {
                "labels": [lun[label] for label in metrics_labels],
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
        logging.error("Cannot get LUN states: %s" % err)
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
                    logging.warning("Cannot set metric <%s>: %s" % (metric, err))
        except BaseException as err:
            logging.warning("Cannot process metric <%s> with data <%s>: %s" %
                            (metric_name, metric_data, err))
    return registry


def update_metrics(registry):
    try:
        multipath_data = load_multipath_data()
        if multipath_data:
            registry.register(get_luns_state(multipath_data))
    except BaseException as err:
        logging.error("Cannot update metrics: %s" % err)


def log_fatal(msg):
    logging.fatal(msg)
    sys.exit(1)


def main():
    logging.info("Started")

    if not validate_host():
        raise MultipathdExporterException("Cannot work on this host")

    try:
        main_registry = prom.CollectorRegistry()
    except BaseException as err:
        log_fatal("Cannot init prometheus collector: %s" % err)

    update_metrics(main_registry)

    try:
        prom.start_http_server(listen_port, registry=main_registry)
    except BaseException as err:
        log_fatal("Cannot start HTTP server, exiting: %s" % err)

    try:
        while True:
            main_registry.__init__(main_registry)
            update_metrics(main_registry)
            time.sleep(collect_interval)
    except KeyboardInterrupt:
        log_fatal("Exiting on keyboard interrupt")
    except BaseException as err:
        log_fatal("Main loop crashed, exiting: %s" % err)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        parser = argparse.ArgumentParser(description='Multipath LUN metrics exporter')
        parser.add_argument("--listen-port", default=9684, help="Port to listen, int, default 9684")
        parser.add_argument("--cmd-timeout", default=2.0, help="Timeout for shell calls, float, default 2.0")
        parser.add_argument("--collect-interval", default=60.0, help="Metrics update interval, float, default 60.0")
        parser_args = parser.parse_args()

        cmd_timeout = parser_args.cmd_timeout
        collect_interval = parser_args.collect_interval
        listen_port = parser_args.listen_port
        multipath_regex_version = "^v(4|5|6|7|8|9)\.[0-1]+\.[0-1]+"
    except BaseException as err:
        log_fatal("Cannot init variables, exiting: %s" % err)
    main()
