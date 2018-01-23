#! /usr/bin/env python
# -*- coding: utf-8 -*-
import logging

import datetime
'''
accumulative yoy
'''
from service.source.companybalancesheet import ComBalanceYOY
from service.source.companyprofitsheet import CompanyProfitSheetYOY
from service.source.companycashsheet import CompanyCashSheetYOY
'''
quarter
'''
from service.source.companyprofitsheet import CompanyProfitSheetQuarter
from service.source.companycashsheet import CompanyCashSheetQuarter
'''
quarter yoy
'''
from service.source.companyprofitsheet import CompanyProfitSheetQuarterYOY
from service.source.companycashsheet import CompanyCashSheetQuarterYOY
'''
quarter hb
'''
from service.source.companyprofitsheet import CompanyProfitSheetQuarterHB
from service.source.companycashsheet import CompanyCashSheetQuarterHB

from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s', datefmt='%a, %d %b %Y %H:%M:%S', filename="service.log", filemode='w')

scheduler = BlockingScheduler()

# '''
# accumulative yoy
# '''
scheduler.add_job(ComBalanceYOY.work, 'date', run_date=datetime.datetime.now())
scheduler.add_job(CompanyProfitSheetYOY.work, 'date', run_date=datetime.datetime.now())
scheduler.add_job(CompanyCashSheetYOY.work, 'date', run_date=datetime.datetime.now())
#
# '''
# quarter
# '''
scheduler.add_job(CompanyProfitSheetQuarter.work, 'date', run_date=datetime.datetime.now())
scheduler.add_job(CompanyCashSheetQuarter.work, 'date', run_date=datetime.datetime.now())

# '''
# quarter yoy
# '''
scheduler.add_job(CompanyProfitSheetQuarterYOY.work_update, 'date', run_date=datetime.datetime.now())
#scheduler.add_job(CompanyCashSheetQuarterYOY.work_update, 'date', run_date=datetime.datetime.now())
#
# '''
# quarter hb
# '''
#scheduler.add_job(CompanyProfitSheetQuarterHB.work_update, 'date', run_date=datetime.datetime.now())
#scheduler.add_job(CompanyCashSheetQuarterHB.work_update, 'date', run_date=datetime.datetime.now())


'''
accumulative yoy
'''
#scheduler.add_job(ComBalanceYOY.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=03, end_date='2018-08-01')
#scheduler.add_job(CompanyProfitSheetYOY.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=03, end_date='2018-08-01')
#scheduler.add_job(CompanyCashSheetYOY.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=03, end_date='2018-08-01')

'''
quarter
'''
#scheduler.add_job(CompanyProfitSheetQuarter.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=03, end_date='2018-08-01')
#scheduler.add_job(CompanyCashSheetQuarter.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=03, end_date='2018-08-01')

'''
quarter yoy
'''
#scheduler.add_job(CompanyProfitSheetQuarterYOY.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=33, end_date='2018-08-01')
#scheduler.add_job(CompanyCashSheetQuarterYOY.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=33, end_date='2018-08-01')

'''
quarter hb
'''
#scheduler.add_job(CompanyProfitSheetQuarterHB.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=33, end_date='2018-08-01')
#scheduler.add_job(CompanyCashSheetQuarterHB.work_update, 'cron', day_of_week='0-6', hour='8,9,10,11,12,13,14,15,16,17,18,19,20,21,22', minute=33, end_date='2018-08-01')

try:
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()


