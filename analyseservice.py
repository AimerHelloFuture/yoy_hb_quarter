#! /usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import logging
import os
import signal
import sys
import time
import traceback

from apscheduler.schedulers.background import BackgroundScheduler

from config.config import configs
from service.source.companybalancesheet import ComBalanceYOY
from utils.daemon import Daemon

logger = logging.getLogger("AnalyseService")

WORKER_CONF = {
    'process': dict([
                     # # Quarter
                     # (CompanyFinanceIndicatorsQuarter, 1),
                     # (CompanyProfitSheetQuarter, 1),
                     # (CompanyCashSheetQuarter, 1),
                     # # HB YOY
                     # (CompanyFinanceIndicatorsYOY, 1),
                     # (CompanyFinceIndicatorQuarterHB, 1),
                     # (CompanyFinceIndicatorQuarterYOY, 1),
                     # (CompanyCashSheetYOY, 1),
                     # (CompanyCashSheetQuarterHB, 1),
                     # (CompanyCashSheetQuarterYOY, 1),
                     # (CompanyBalanceSheetYOY, 1),
                     # (CompanyProfitSheetQuarterHB, 1),
                     # (CompanyProfitSheetQuarterYOY, 1),
                     # (CompanyProfitSheetYOY, 1),
                     # (TmpChoiceRating, 1),
                     (ComBalanceYOY, 1)
                     # (CompanyProfitSheetQuarter, 1),
                     # (CompanyCashSheetQuarter, 1)
                     ])
}


def _service_worker(category):
    """
    Work实际入口

    :param category:
    :return:
    """
    c = category
    if c:
        try:
            logger.info("EXPORT START %s" % c)
            c.work()
            logger.info("EXPORT SUCCEED %s" % c)
        except Exception:
            logger.error("EXPORT FAILED %s, %s"%(c,get_exception_info()))
    else:
        logger.warn("EXPORT FAILED category:%s, %s"%category)


def get_exception_info():
    exc_type, value, tb = sys.exc_info()
    formatted_tb = traceback.format_tb(tb)
    data = 'Exception %s: %s traceback=%s' % (exc_type, value, formatted_tb)
    return data


class TransferService(Daemon):
    def __init__(self):
        self._pids = {}

        self._terminating = False
        self._terminated = False
        super(TransferService, self).__init__(
            name="analyse service", pidfile=configs['pid']['analyse'])

    def run(self):
        logger.info("WORKER IMPL RUN COMMON TASK")
        signal.signal(signal.SIGTERM, self._term_handler)

        # WORKER_CONF
        self._init_workers(WORKER_CONF)

        while not self._terminated:
            logger.info('start loop')
            time.sleep(10)

    def _term_handler(self, signum, frame):
        self._terminating = True
        for pid in self._pids.keys():
            try:
                logger.info('kill pid '+pid)
                os.kill(pid, signal.SIGTERM)
            except:
                pass
        self._pids = {}
        self._terminated = True
        logger.info('SERVICE TERMINATING pid: %s.' % os.getpid())

    def _init_workers(self, proccess=WORKER_CONF):
        """
        批量初始化Work

        :param proccess:
        :return:
        """
        for category, count in proccess['process'].iteritems():
            for i in range(count):
                worker_info = (_service_worker, [category], {})
                pid = self._create_worker(*worker_info)

                if pid:
                    self._pids[pid] = worker_info

    def _create_worker(self, worker, args, kwargs):
        pid = os.fork()
        if not pid:
            worker(*args, **kwargs)
            sys.exit()
        return pid


def job():
    TransferService().restart()


def scheduler_task():
    scheduler = BackgroundScheduler()

    scheduler.add_job(job, 'date', run_date=datetime.datetime.now())

    # scheduler.add_job(job, 'cron', day_of_week='1-7', hour=1, minute=1)
    scheduler.start()

    logging.info('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
        logging.info('scheler shutdown')


if __name__ == '__main__':
    logging.basicConfig(filename="service.log", format='%(asctime)s %(name)s %(levelname)s: %(message)s', level=logging.DEBUG)
    logger = logging.getLogger("Analyse Service")

    if len(sys.argv) == 2:
        if sys.argv[1] == 'start':
            TransferService().start()
        elif sys.argv[1] == 'stop':
            TransferService().stop()
        elif sys.argv[1] == 'restart':
            TransferService().restart()
        elif sys.argv[1] == 'job':
            scheduler_task()
        else:
            print('Unkonw command')
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)