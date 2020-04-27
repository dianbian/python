#! /usr/bin/env python
# -*- coding: utf8 -*-

import httplib
import urllib2
import json
import MySQLdb
import datetime
import time
import subprocess
import os
import requests
import logging
import csv
import json
import re
import multiprocessing


from inventorymainrouteao.stub import *
from inventorymainrouteao.MainRouteInventoryPo_xxo import * 
from inventorymainrouteao.MainRouteProductInfoPo_xxo import * 
from inventorymainrouteao.MainRouteFilterPo_xxo import * 
from bbcplatform import web_stub_cntl

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

#DEV
# HOST='172.172.177.9'
# PORT=3307
# USER='liuyc'
# PASSWD='haiziwang@liuyongchao'
# DBNAME='helloworld'
# PEERIP = "172.172.177.96"

#TEST
# HOST='172.172.178.18'
# PORT=3309
# USER='liuyc'
# PASSWD='haiziwang@liuyongchao'
# DBNAME='helloworld'
PEERIP = "172.172.178.83" 

#IDC
strUrl = 'http://test.tmqproducer.haiziwang.com/kms-producer/proWrapService/send.do'


#PEERIP = "172.24.2.177" #"172.24.2.235"

#strFilePath = '/home/appadmin/bianbian/log'
strFilePath = './log'
INVENTORY_TBNUM = 1024

global g_fileName
global logger
#  日志模块
def logInit(log_file):
	logger = logging.getLogger('c')
	logger.setLevel(logging.DEBUG)
	ch = logging.FileHandler(log_file,'a')
	ch.setLevel(logging.DEBUG)
	formatter = logging.Formatter('[%(asctime)s]-[%(name)s-%(levelname)s]- %(filename)s-%(lineno)d: %(message)s')
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	return logger

##调接口
def DoInvoke(req,resp):
	if req.machineKey in [None,""]:
		req.machineKey = "local"
	if req.source in [None,""]:
		req.source = "python_tool"
	if req.sceneId in [None,0]:
		req.sceneId = 100
	if req.option in [None,0]:
		req.option = 50	
	try:
		stub = web_stub_cntl.WebStubCntl()
		stub.setPeerIPPort(PEERIP, 53101)
		stub.invoke(req,resp)
		if resp.result == 0:
			logger.debug('call success')
			return True
		else:
			logger.error('call failed code[%d]'%(resp.result))
			return False
	except Exception as e:
		logger.error(e)
		return False	

# 读取线上子商品库存信息
def DoGetMainRouteInventoryInfoForOnline(sku, delivery):
	#定义MainRouteProductInfoPo
	logger.debug('DoGetMainRouteInventoryInfoForOnline')
	mainRouteProduct = MainRouteProductInfoPo()
	mainRouteProduct.ddwProductSysno = sku
	mainRouteProduct.cProductSysno_u = 1
	mainRouteProduct.dwIsSupplierDelivery = delivery
	mainRouteProduct.cIsSupplierDelivery_u = 1
	mainRouteProduct.dwRouteSupport = 0
	mainRouteProduct.cRouteSupport_u = 0
	mainRouteProduct.dwOwnerSysno = 3
	mainRouteProduct.cOwnerSysno_u = 1
	#定义map对象
	mapInventory = stl_map('uint64_t,inventorymainrouteao.MainRouteProductInfoPo_xxo.MainRouteProductInfoPo') 
	mapInventory[sku] = mainRouteProduct

	#定义 oMainRouteFilterPo
	mainFilterPo = MainRouteFilterPo()
	mainFilterPo.mapProductInfoList = mapInventory

	req = GetMainRouteInventoryInfoForOnlineReq()  #请求接口和参数
	resp = GetMainRouteInventoryInfoForOnlineResp() #返回接口
	req.mainRouteFilterPo = mainFilterPo
	req.machineKey = "simida"
	req.source = "bianbian"
	req.sceneId = 666
	req.option = 999
	req.inReserve = ""

	res = DoInvoke(req, resp)
	if res == False:
		logger.debug('req failed ' + str(sku))
		return 0
	mainRouteResultPo = MainRouteResultPo()  
	mainRouteResultPo = resp.mainRouteResultPo #返回结果集
	mapMainRoutePo = stl_map('uint64_t,stl_vector<inventorymainrouteao.MainRouteInventoryPo_xxo.MainRouteInventoryPo>')
	mapMainRoutePo = mainRouteResultPo.mapMainRouteInventoryPo
	if len(mapMainRoutePo) == 0:
		logger.debug('map size = 0')
		return 0
	vecMainRoutePo = mapMainRoutePo[sku]
	if len(vecMainRoutePo) == 0:
		logger.debug('vec size = 0')
		return 0
	mainRouteInventoryPo = vecMainRoutePo[0]
	logger.debug('skuid ' + str(mainRouteInventoryPo.ddwProductSysno) + ' stock ' + str(mainRouteInventoryPo.ddwStockSysno) \
				+ ' dwRealNum ' + str(mainRouteInventoryPo.dwRealNum))
	print('skuid ' + str(mainRouteInventoryPo.ddwProductSysno) + ' stock ' + str(mainRouteInventoryPo.ddwStockSysno) \
				+ ' dwRealNum ' + str(mainRouteInventoryPo.dwRealNum))
	realAvailable = mainRouteInventoryPo.dwRealNum		#实库变化
	realAvailable += mainRouteInventoryPo.dwOversellNum	#超卖数量
	realAvailable -= mainRouteInventoryPo.dwActiveNum	#活动数量
	realAvailable -= mainRouteInventoryPo.dwRealReserveNum	#室库预留
	realAvailable -= mainRouteInventoryPo.dwAllocatedNum	#已分配数量
	realAvailable -= mainRouteInventoryPo.dwManualLockNum	#人工锁定数量
	return realAvailable

#发送捆绑主商品数量了
def sendMainSkuid(mainSkuid, realAvailable):
	beforeNum = 0 if realAvailable > 0 else 100
	msgType = 3 if realAvailable > 0 else 4
	#推送json准备
	mq_json = {
		'req':{
			'appcode' : 'ccss',
			'topic' : 'bzhtosearchtest',
			'token' : '71032F2D8DA75D3A3E88AAE47B1DB023',
			'content' : ''
		}
	}
	mq_json_content = []
	data_json = {
		'ddwProductSysno' : mainSkuid,
		'dwMsgType': msgType,
		'dwNotice': 0,
		'dwBeforeAvailNum': beforeNum,
		'dwAfterAvailNum': realAvailable,
		'dwStoreReserveChgType': 0,
		'dwEntityId' : 0x1010101
	}
	mq_json_content.append(data_json)
	mq_json['req']['content'] = json.dumps(mq_json_content)
	if pushSearch(strUrl, mq_json):
		print('success post, skuid = ' + str(mainSkuid))
	else:
		print('failed post, skuid = ' + str(mainSkuid))

def nowDatetimeStr():
	ticks = time.time()
	timeArray = time.localtime(int(ticks))
	return time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

#推http
def pushSearch(url, mq_json):
	try:
		headers = {
			'Content-Type' : 'application/json'
		}
		http_resp = requests.post(url,json=mq_json, headers=headers)
		http_resp = json.loads(http_resp.text)
		if http_resp['errno'] != 0:
			logger.debug(nowDatetimeStr() + ' push change failed ' + http_resp['errmsg'])
			return False
		logger.debug(nowDatetimeStr() + ' push change success')
		return True

	except Exception as e:
		logger.debug(nowDatetimeStr() + ' error, exception ' + str(e))
	return False

def calculateSkuNum(mainSkuid, vecSkuid):
	vecTuple = ()
	retSkuidNum = []   #子商品的数量保存到这里， 子商品数量/组合需要数量 = 排序最小的数量就是主商品的数量 prefect
	for skuidObj in vecSkuid:
		availNum = DoGetMainRouteInventoryInfoForOnline(skuidObj[0], skuidObj[2])  #返回值就是子商品的数量
		if availNum > 0 and skuidObj[1] > 0:  #组成数量一定大于0，为了安全校验下
			retSkuidNum.append(availNum/skuidObj[1])  #向下取证
		else:
			retSkuidNum.append(availNum)
	retSkuidNum.sort()
	print(retSkuidNum)
	sendMainSkuid(mainSkuid, retSkuidNum[0])

starttime = time.time()
print starttime
g_fileName = "bindForSearch"
strFileName = strFilePath +'/'+ g_fileName+'_'+ nowDatetimeStr() + '.log'
logger = logInit(strFileName) 

#主函数开始
try:
	logger.debug('****************** bind for search*************start time:' + nowDatetimeStr())	
	if(len(sys.argv) == 0):
		print "argv error : use python inventoryTest.py xxx.csv start end"
		exit(0)
	filePath = sys.argv[1]
	csv_read = csv.reader(open(filePath,'r'))
	index = 0
	mapDict = {}
	for data in csv_read: #数据提取
		mainSkuid = int(data[0])
		skuChild = []
		skuChild.append(int(data[1]))
		skuChild.append(int(data[2]))
		skuChild.append(int(data[3]))
		vecTuple = (skuChild,)
		if mapDict.has_key(mainSkuid):
			vecTuple1 = vecTuple + mapDict.get(mainSkuid)
			mapDict.pop(mainSkuid)
			mapDict[mainSkuid] = vecTuple1
		else:
			mapDict[mainSkuid] = vecTuple
		index = index + 1
	print(mapDict)
	for (key, value) in mapDict.items():
		print(key, value)
		calculateSkuNum(key, value)

	logger.debug('****************** bind for search***********end time:' + nowDatetimeStr() + ' index=' + str(index))		
	print "count : " + str(index)
except MySQLdb.Error,e:
	print "Mysql Error %d: %s" % (e.args[0], e.args[1])
		
endtime = time.time()
print endtime
print endtime - starttime
