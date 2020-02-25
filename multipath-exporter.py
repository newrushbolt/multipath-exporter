#! /usr/bin/env python

import argparse
import getpass
import json
import logging
import os
import prometheus_client as prom
import re
import sys
import time

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
        multipath_version_line = re.match('^multipath-tools.*$', multipath_help_stdout, re.M).group(0)
        multipath_version = multipath_version_line.split(' ')[1]
        logging.debug("Multipath version is <%s>" % multipath_version)
        if re.match(multipath_regex_version, multipath_version):
            return True
        else:
            logging.error("Multipath version is unsupported")
            return False
    except Exception as e:
        logging.error("Cannot check multipath version: %s" % e)
        return False
    return True


def load_multipath_data():
    src_data = {}
    try:
        multipathd_output = subprocess.check_output(
            ['multipathd', 'show', 'maps', 'json'], stderr=subprocess.STDOUT, timeout=cmd_timeout)
        src_data = json.loads(multipathd_output)
    except Exception as e:
        logging.error("Cannot get valid data from multipathd: %s" % e)
    return src_data


def get_luns_state(multipath_data):
    registry = None
    try:
        metrics_object = {}
        metrics_labels = ['uuid', 'dm_st']
        metrics = []
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
    except Exception as e:
        logging.error("Cannot get LUN states: %s" % e)
    return registry


def raw_metrics_to_registried(raw_metrics):
    registry = prom.CollectorRegistry()
    for metric_name, metric_data in raw_metrics.items():
        try:
            metric_gauge = prom.Gauge(metric_name, metric_data['desc'], metric_data['labels'], registry=registry)
            for metric in metric_data['metrics']:
                try:
                    metric_gauge.labels(*metric['labels']).set(metric['value'])
                except Exception as e:
                    logging.warning("Cannot set metric <%s>: %s" % (metric, e))
        except Exception as e:
            logging.warning("Cannot process metric <%s> with data <%s>: %s" % (metric_name, metric_data, e))
    return registry


def update_metrics(registry):
    try:
        multipath_data = load_multipath_data()
        if multipath_data:
            registry.register(get_luns_state(multipath_data))
    except Exception as e:
        logging.error("Cannot update metrics: %s" % e)


def main():
    logging.info("Started")

    if not validate_host():
        raise MultipathdExporterException("Cannot work on this host")

    try:
        main_registry = prom.CollectorRegistry()
    except Exception as e:
        logging.critical("Cannot init prometheus collector: %s" % e)
        sys.exit(1)

    update_metrics(main_registry)

    try:
        prom.start_http_server(8080, registry=main_registry)
    except Exception as e:
        logging.critical("Cannot start HTTP server, exiting: %s" % e)
        sys.exit(1)

    try:
        while True:
            main_registry.__init__(main_registry)
            update_metrics(main_registry)
            time.sleep(collect_interval)
    except Exception as e:
        logging.critical("Main loop crashed, exiting: %s" % e)
        sys.exit(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        parser = argparse.ArgumentParser(description='Multipath LUN metrics exporter')
        parser.add_argument("--listen-port", default=9684, help="Port to listen, int, default 9684")
        parser.add_argument("--cmd-timeout", default=2.0, help="Timeout for shell calls, float, default 2.0")
        parser.add_argument("--collect-interval", default=60.0, help="Metrics update interval, float, default 60.0")
        args = parser.parse_args()

        cmd_timeout = args.cmd_timeout
        collect_interval = args.collect_interval
        listen_port = args.listen_port
        multipath_regex_version = '^v(4|5|6|7|8|9)\.[0-1]+\.[0-1]+'
    except Exception as e:
        logging.critical("Cannot init variables, exiting: %s" % e)
        sys.exit(1)
    main()
