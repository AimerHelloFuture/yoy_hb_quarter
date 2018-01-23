#! /usr/bin/env python
# -*- coding: utf-8 -*-
from os.path import expanduser

configs = {

    'mysql11': {
        'host': '10.11.255.60',
        'user': 'data_center',
        'password': 'Wox4I*2pXe#l',
        'port': 3306,
        'database': 'data_center'
    },

    'mysql22': {
        'host': '10.25.170.41',
        'user': 'hs_dev',
        'password': 'UYLRPLLmiLMB3Gre',
        'port': 3306,
        'database': 'data_center',
    },

    'mysql': {
        'host': '114.55.108.136',
        'port': 3306,
        'user': 'dc_select',
        'password': 'Aenl1pnBtXWpQ5DW',
        'database': 'data_center'
    },

    'mongo': {
        'uri': 'mongodb://10.11.255.110:31017',
        'db': 'test',
    },
    'solr': {
        'host': 'http://10.11.0.37:8080',
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

