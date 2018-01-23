#! /usr/bin/env python
# -*- coding: utf-8 -*-
from os.path import expanduser

configs = {

    'mysql': {
        'host': '10.25.170.41',
        'user': 'hs_dev',
        'password': 'UYLRPLLmiLMB3Gre',
        'port': 3306,
        'database': 'data_center',
    },

    'mongo': {
        'uri': 'mongodb://spider:Serwe-8dfgre@10.117.211.16:27017',
        'db': 'cr_data',
    },

    'solr': {
        'host': 'http://10.168.155.48:8080',
        'service': {
            'research_report': '/solrweb/indexService',
            'notice': '/solrweb/noticeIndexByUpdate',
            'news': 'solrweb/newsIndexByUpdate',
            'industry_report': '/solrweb/industryReportIndexByUpdate',
            'chart': '/solrweb/finChartIndexByUpdate',
        },
    },

    'file': {
        'csc': expanduser('~/niub/csc'),  # 行情
    },

    'pid': {
        'analyse': expanduser('~/pid/analyse_service.pid'),
    },

    'mode': 'full'  # 运行模式 full:全量计算, increase:增量计算
}
